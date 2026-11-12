"""Dynamic edges must preserve single-execution, acyclicity and cancellation."""

import asyncio
from collections import Counter
import unittest

import datacellpy as cell

from datacellpy import (CellExecutionError, CycleError, DefinitionError, InputError,
                       ResourceError, Runtime, UnknownCellError, UnsealedCellError)


class DependencyEditingTests(unittest.TestCase):
    def test_module_level_dynamic_api(self):
        cell.reset()
        self.addCleanup(cell.reset)
        cell.cell("source")(lambda: 21)
        cell.endof("source")

        @cell.cell("answer")
        async def answer():
            return (await cell.require("source")) * 2

        cell.endof("answer")
        cell.set_dependencies("answer", "source")
        cell.invalidate("source")
        self.assertEqual(cell.run("answer"), 42)

    def test_require_cannot_mutate_scheduler_from_a_worker_event_loop(self):
        runtime = Runtime(max_workers=1)
        runtime.cell("source")(lambda: 1)
        runtime.endof("source")

        @runtime.cell("worker")
        def worker():
            with self.assertRaises(DefinitionError):
                asyncio.run(runtime.require("source"))
            return 42

        runtime.endof("worker")
        self.assertEqual(runtime.run("worker"), 42)

    def test_rewire_sealed_cell_invalidates_cached_descendants(self):
        runtime = Runtime(incremental=True)
        for name, value in (("left", 1), ("right", 20)):
            runtime.cell(name)(lambda value=value: value)
            runtime.endof(name)
        runtime.cell("answer", {"x": "left"})(lambda x: x + 1)
        runtime.endof("answer")
        old_plan = runtime.compile("answer")
        self.assertEqual(runtime.run("answer"), 2)
        runtime.set_dependencies("answer", {"x": "right"})
        self.assertEqual(runtime.run("answer"), 21)
        self.assertEqual(old_plan.dependencies["answer"], ("left",))
        self.assertEqual(runtime.compile("answer").dependencies["answer"], ("right",))

    def test_invalid_rewire_is_atomic(self):
        runtime = Runtime()
        runtime.cell("left")(lambda: 1)
        runtime.cell("right", "left")(lambda left: left)
        runtime.endof("left")
        runtime.endof("right")
        for need, error in (("right", CycleError), ("missing", UnknownCellError),
                            ({"invalid": "right"}, DefinitionError)):
            with self.subTest(need=need):
                with self.assertRaises(error):
                    runtime.set_dependencies("left", need)
                self.assertEqual(runtime.compile("left").dependencies["left"], ())
        self.assertEqual(runtime.run("right"), 1)


class DynamicTests(unittest.IsolatedAsyncioTestCase):
    async def test_branch_selection_nested_requires_and_shared_dependency(self):
        runtime = Runtime(max_workers=1)
        calls = Counter()

        @runtime.cell("source")
        def source():
            calls["source"] += 1
            return 20

        @runtime.cell("left")
        async def left():
            return (await runtime.require("source")) + 1

        @runtime.cell("right", need="source")
        def right(source):
            return source + 2

        @runtime.cell("unused")
        def unused():
            self.fail("unselected branch ran")

        @runtime.cell("router")
        async def router():
            values = await runtime.require(["left", "right"])
            self.assertEqual(await runtime.require("source"), 20)
            return sum(values.values())

        for name in ("source", "left", "right", "unused", "router"):
            runtime.endof(name)
        self.assertEqual(await asyncio.wait_for(runtime.run_async("router"), 3), 43)
        self.assertEqual(calls["source"], 1)
        self.assertEqual(runtime.result("source"), 20)

    async def test_new_cells_can_be_registered_during_a_run(self):
        runtime = Runtime(max_workers=1)

        @runtime.cell("expand")
        async def expand():
            runtime.cell("new")(lambda: 42)
            runtime.endof("new")
            return await runtime.require("new")

        runtime.endof("expand")
        self.assertEqual(await runtime.run_async("expand"), 42)
        self.assertEqual(runtime.result("new"), 42)
        self.assertEqual(runtime.result("expand"), 42)

    async def test_dynamic_cycle_fails_instead_of_hanging(self):
        runtime = Runtime(max_workers=1)

        @runtime.cell("left")
        async def left():
            return await runtime.require("right")

        @runtime.cell("right")
        async def right():
            return await runtime.require("left")

        runtime.endof("left")
        runtime.endof("right")
        with self.assertRaises(CellExecutionError) as caught:
            await asyncio.wait_for(runtime.run_async("left"), 2)
        self.assertIsInstance(caught.exception.cause, CycleError)

    async def test_dynamic_preflight_errors_can_be_caught_without_partial_changes(self):
        runtime = Runtime(resources={"gpu": 1})
        called = []
        runtime.cell("unsealed")(lambda: None)
        runtime.cell("missing_input")(lambda required: required)
        runtime.endof("missing_input")
        runtime.cell("too_large", resources={"gpu": 2})(lambda: called.append(1))
        runtime.endof("too_large")

        @runtime.cell("router")
        async def router():
            for target, error in (("absent", UnknownCellError), ("unsealed", UnsealedCellError),
                                  ("missing_input", InputError), ("too_large", ResourceError)):
                with self.assertRaises(error):
                    await runtime.require(target)
            return 42

        runtime.endof("router")
        self.assertEqual(await runtime.run_async("router"), 42)
        self.assertEqual(called, [])

    async def test_require_outside_execution_is_rejected(self):
        with self.assertRaises(DefinitionError):
            await Runtime().require("anything")

    async def test_dynamic_failure_cancels_suspended_caller(self):
        runtime = Runtime(max_workers=1)
        cleaned = asyncio.Event()

        @runtime.cell("failure")
        async def failure():
            raise ValueError("failed dynamic dependency")

        @runtime.cell("router")
        async def router():
            try:
                return await runtime.require("failure")
            finally:
                cleaned.set()

        runtime.endof("failure")
        runtime.endof("router")
        with self.assertRaises(CellExecutionError) as caught:
            await asyncio.wait_for(runtime.run_async("router"), 2)
        self.assertEqual(caught.exception.cell_name, "failure")
        self.assertTrue(cleaned.is_set())

    async def test_cancellation_during_require_drains_child_cleanup(self):
        runtime = Runtime(max_workers=1)
        started = asyncio.Event()
        cleaned = asyncio.Event()

        @runtime.cell("child")
        async def child():
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)
                cleaned.set()

        @runtime.cell("router")
        async def router():
            return await runtime.require("child")

        runtime.endof("child")
        runtime.endof("router")
        task = asyncio.create_task(runtime.run_async("router"))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await asyncio.wait_for(task, 2)
        self.assertTrue(cleaned.is_set())

    async def test_dynamic_cache_tracks_only_selected_branch_and_invalidation(self):
        runtime = Runtime(max_workers=1, incremental=True)
        calls = Counter()
        for name, value in (("left", 1), ("right", 2)):
            def branch(name=name, value=value):
                calls[name] += 1
                return value
            runtime.cell(name)(branch)
            runtime.endof(name)

        @runtime.cell("router")
        async def router(choice):
            calls["router"] += 1
            return await runtime.require(choice)

        runtime.endof("router")
        self.assertEqual(await runtime.run_async("router", choice="left"), 1)
        self.assertEqual(await runtime.run_async("router", choice="left"), 1)
        self.assertEqual(calls, {"left": 1, "router": 1})
        self.assertEqual(await runtime.run_async("router", choice="right"), 2)
        self.assertEqual(calls, {"left": 1, "right": 1, "router": 2})
        runtime.invalidate("right")
        self.assertEqual(await runtime.run_async("router", choice="right"), 2)
        self.assertEqual(calls, {"left": 1, "right": 2, "router": 3})

    async def test_changing_dynamic_definition_reruns_parent(self):
        runtime = Runtime(incremental=True)
        calls = []
        runtime.cell("source")(lambda: 1)
        runtime.endof("source")

        @runtime.cell("router")
        async def router():
            calls.append(1)
            return await runtime.require("source")

        runtime.endof("router")
        self.assertEqual(await runtime.run_async("router"), 1)
        runtime.cell("source")(lambda: 2)
        runtime.endof("source")
        self.assertEqual(await runtime.run_async("router"), 2)
        self.assertEqual(len(calls), 2)

    async def test_changed_dynamic_selector_does_not_run_old_branch(self):
        runtime = Runtime(incremental=True)
        calls = Counter()
        runtime.cell("selector")(lambda: "left")
        runtime.endof("selector")
        for name in ("left", "right"):
            def branch(name=name):
                calls[name] += 1
                return name
            runtime.cell(name, cache=False)(branch)
            runtime.endof(name)

        @runtime.cell("router")
        async def router():
            choice = await runtime.require("selector")
            return await runtime.require(choice)

        runtime.endof("router")
        self.assertEqual(await runtime.run_async("router"), "left")
        runtime.cell("selector")(lambda: "right")
        runtime.endof("selector")
        self.assertEqual(await runtime.run_async("router"), "right")
        self.assertEqual(calls, {"left": 1, "right": 1})

    async def test_cached_dynamic_request_preserves_parallel_target_groups(self):
        runtime = Runtime(max_workers=2, incremental=True)
        left_started = asyncio.Event()
        right_started = asyncio.Event()
        calls = Counter()

        @runtime.cell("left", cache=False)
        async def left():
            calls["left"] += 1
            left_started.set()
            await right_started.wait()
            return 20

        @runtime.cell("right", cache=False)
        async def right():
            calls["right"] += 1
            right_started.set()
            await left_started.wait()
            return 22

        @runtime.cell("router")
        async def router():
            calls["router"] += 1
            return sum((await runtime.require(["left", "right"])).values())

        for name in ("left", "right", "router"):
            runtime.endof(name)
        for _ in range(2):
            left_started.clear()
            right_started.clear()
            self.assertEqual(await asyncio.wait_for(runtime.run_async("router"), 2), 42)
        self.assertEqual(calls, {"left": 2, "right": 2, "router": 2})

    async def test_concurrent_require_calls_fail_and_cleanup_all_requests(self):
        runtime = Runtime(max_workers=1)
        runtime.cell("child")(lambda: 1)
        runtime.endof("child")

        @runtime.cell("parent")
        async def parent():
            return await asyncio.gather(runtime.require("child"), runtime.require("child"))

        runtime.endof("parent")
        before = asyncio.all_tasks()
        with self.assertRaises(CellExecutionError) as caught:
            await asyncio.wait_for(runtime.run_async("parent"), 2)
        self.assertIsInstance(caught.exception.cause, DefinitionError)
        self.assertEqual(asyncio.all_tasks() - before, set())

    async def test_existing_definitions_are_snapshotted_during_dynamic_expansion(self):
        runtime = Runtime()
        runtime.cell("source")(lambda: 1)
        runtime.endof("source")

        @runtime.cell("parent", need="source")
        async def parent(source):
            runtime.cell("source")(lambda: 2)
            runtime.endof("source")
            return source, await runtime.require("source")

        runtime.endof("parent")
        self.assertEqual(await runtime.run_async("parent"), (1, 1))
        self.assertEqual(await runtime.run_async("source"), 2)


if __name__ == "__main__":
    unittest.main()
