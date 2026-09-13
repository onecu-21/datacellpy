"""Behavior tests that also serve as small, executable DataCellPy examples."""

import asyncio
import inspect
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

import datacellpy as cell


class RuntimeTests(unittest.TestCase):
    def setUp(self):
        self.runtime = cell.Runtime()

    def define(self, name, function, need=None, *, seal=True):
        decorated = self.runtime.c(name, need=need)(function)
        self.assertIs(decorated, function)
        if seal:
            self.runtime.eo(name)
        return function

    def test_full_names_are_aliases(self):
        self.assertIs(cell.Runtime.cell, cell.Runtime.c)
        self.assertIs(cell.Runtime.endof, cell.Runtime.eo)
        self.assertIs(cell.cell, cell.c)
        self.assertIs(cell.endof, cell.eo)
        function = lambda: "full names"
        self.assertIs(self.runtime.cell("example")(function), function)
        self.runtime.endof("example")
        self.assertEqual(self.runtime.run("example"), "full names")

    def test_module_api_runs_cells(self):
        cell.reset()
        self.addCleanup(cell.reset)

        @cell.cell("source")
        def source():
            return 6

        cell.endof("source")

        @cell.c("answer", need="source")
        def answer(source):
            return source * 7

        cell.eo("answer")
        self.assertEqual(cell.run("answer"), 42)
        self.assertEqual(cell.result("source"), 6)

    def test_compile_selects_transitive_dag_and_does_not_execute(self):
        calls = []
        self.define("answer", lambda left, right: left + right, ["left", "right"])
        self.define("right", lambda base: base + 2, "base")
        self.define("left", lambda base: base + 1, ("base",))
        self.define("base", lambda: calls.append("base") or 10)
        self.define("unused", lambda: calls.append("unused"))

        plan = self.runtime.compile("answer")
        self.assertEqual(plan.targets, ("answer",))
        self.assertIsInstance(plan.stages, tuple)
        self.assertTrue(all(isinstance(stage, tuple) for stage in plan.stages))
        self.assertEqual(tuple(map(set, plan.stages)),
                         ({"base"}, {"left", "right"}, {"answer"}))
        self.assertEqual(set(plan.order), {"base", "left", "right", "answer"})
        for dependency in ("left", "right"):
            self.assertLess(plan.order.index("base"), plan.order.index(dependency))
            self.assertLess(plan.order.index(dependency), plan.order.index("answer"))
        self.assertEqual(set(plan.dependencies["answer"]), {"left", "right"})
        self.assertEqual(set(plan.dependencies["base"]), set())
        self.assertEqual(calls, [])
        self.assertEqual(self.runtime.run("answer"), 23)
        self.assertEqual(calls, ["base"])

    def test_target_form_controls_return_shape(self):
        self.define("first", lambda: 1)
        self.define("second", lambda first: first + 1, "first")
        self.assertEqual(self.runtime.run("second"), 2)
        self.assertEqual(self.runtime.run(["second"]), {"second": 2})
        self.assertEqual(self.runtime.run(("first", "second")),
                         {"first": 1, "second": 2})
        self.assertEqual(self.runtime.run(), {"first": 1, "second": 2})
        self.assertEqual(set(self.runtime.compile().targets), {"first", "second"})

    def test_unknown_target_or_dependency_fails_before_execution(self):
        called = []
        self.define("valid", lambda: called.append("valid"))
        self.define("invalid", lambda: called.append("invalid"), "missing")
        with self.assertRaises(cell.UnknownCellError):
            self.runtime.compile("missing")
        with self.assertRaises(cell.UnknownCellError):
            self.runtime.run(["valid", "invalid"])
        self.assertEqual(called, [])

    def test_forward_reference_can_be_resolved_later(self):
        self.define("answer", lambda source: source + 1, "source")
        with self.assertRaises(cell.UnknownCellError):
            self.runtime.compile("answer")
        self.define("source", lambda: 41)
        self.assertEqual(self.runtime.run("answer"), 42)

    def test_unsealed_dependency_is_rejected_before_execution(self):
        called = []
        self.define("source", lambda: called.append("source"), seal=False)
        self.define("answer", lambda: called.append("answer"), "source")
        with self.assertRaises(cell.UnsealedCellError):
            self.runtime.compile("answer")
        with self.assertRaises(cell.UnsealedCellError):
            self.runtime.run("answer")
        self.assertEqual(called, [])
        self.runtime.eo("source")
        self.runtime.run("answer")
        self.assertEqual(called, ["source", "answer"])

    def test_cycles_are_rejected_before_any_cell_runs(self):
        called = []
        self.define("a", lambda: called.append("a"), "b")
        self.define("b", lambda: called.append("b"), "a")
        self.define("independent", lambda: called.append("independent"))
        with self.assertRaises(cell.CycleError):
            self.runtime.compile("a")
        with self.assertRaises(cell.CycleError):
            self.runtime.run(["independent", "a"])
        self.assertEqual(called, [])

    def test_self_dependency_is_a_cycle(self):
        self.define("self", lambda: None, "self")
        with self.assertRaises(cell.CycleError):
            self.runtime.compile("self")

    def test_unselected_invalid_cells_do_not_block_valid_target(self):
        self.define("valid", lambda: 42)
        self.define("unknown", lambda: None, "missing")
        self.define("unsealed", lambda: None, seal=False)
        self.assertEqual(self.runtime.run("valid"), 42)

    def test_dependencies_are_injected_only_when_declared(self):
        self.define("source", lambda: 7)
        self.define("answer", lambda source=3: source * 2)
        self.assertEqual(self.runtime.run("answer"), 6)
        self.assertEqual(self.runtime.run("answer", source=5), 10)

    def test_mapping_alias_and_dependency_precedence(self):
        self.define("source-cell", lambda: 40)
        self.define("answer", lambda value, *, extra=2: value + extra,
                    {"value": "source-cell"})
        self.assertEqual(self.runtime.run("answer", value=-100, extra=3), 43)

    def test_unmatched_dependency_is_ordering_only(self):
        called = []
        self.define("initialization", lambda: called.append("initialized"))

        def answer(*, value=42):
            self.assertEqual(called, ["initialized"])
            return value

        self.define("answer", answer, "initialization")
        self.assertEqual(self.runtime.run("answer", value=8), 8)

    def test_inputs_are_shared_by_declared_parameter_names(self):
        self.define("source", lambda value: value * 2)
        self.define("answer", lambda source, increment=1: source + increment, "source")
        self.assertEqual(self.runtime.run("answer", value=20, increment=2), 42)

    def test_missing_input_is_validated_for_entire_graph_before_execution(self):
        called = []
        self.define("source", lambda: called.append("source") or 1)
        self.define("answer", lambda source, required: source + required, "source")
        with self.assertRaises(cell.InputError):
            self.runtime.run("answer")
        self.assertEqual(called, [])

    def test_unknown_inputs_are_rejected_before_execution(self):
        called = []
        self.define("answer", lambda value=1: called.append("answer") or value)
        self.define("unselected", lambda only_here: only_here)
        for inputs in ({"typo": 1}, {"only_here": 1}):
            with self.subTest(inputs=inputs):
                with self.assertRaises(cell.InputError):
                    self.runtime.run("answer", **inputs)
        self.assertEqual(called, [])

    def test_result_is_available_for_dependencies_and_runs_do_not_cache(self):
        calls = []
        self.define("source", lambda: calls.append(1) or len(calls))
        self.define("answer", lambda source: source * 10, "source")
        with self.assertRaises(cell.ResultUnavailableError):
            self.runtime.result("answer")
        self.assertEqual(self.runtime.run("answer"), 10)
        self.assertEqual(self.runtime.result("source"), 1)
        self.assertEqual(self.runtime.result("answer"), 10)
        self.assertEqual(self.runtime.run("answer"), 20)
        self.assertEqual(self.runtime.result("answer"), 20)
        self.assertEqual(calls, [1, 1])

    def test_results_describe_latest_successful_run(self):
        self.define("first", lambda: 1)
        self.define("second", lambda: 2)
        self.runtime.run("first")
        self.runtime.run("second")
        self.assertEqual(self.runtime.result("second"), 2)
        with self.assertRaises(cell.ResultUnavailableError):
            self.runtime.result("first")

    def test_failed_run_does_not_replace_last_successful_results(self):
        self.define("answer", lambda value: 10 // value)
        self.assertEqual(self.runtime.run("answer", value=2), 5)
        with self.assertRaises(cell.CellExecutionError):
            self.runtime.run("answer", value=0)
        self.assertEqual(self.runtime.result("answer"), 5)

    def test_graph_is_readable_and_does_not_execute_cells(self):
        called = []
        self.define("source", lambda: called.append("source") or 1)
        self.define("answer", lambda source: source + 1, "source")
        self.define("unused", lambda: called.append("unused"))
        diagram = self.runtime.graph("answer")
        self.assertIsInstance(diagram, str)
        self.assertIn("source", diagram)
        self.assertIn("answer", diagram)
        self.assertIn("-->", diagram)
        self.assertNotIn("unused", diagram)
        self.assertEqual(called, [])

    def test_hot_replacement_requires_sealing_new_definition(self):
        self.define("source", lambda: 1)
        self.define("answer", lambda source: source + 1, "source")
        self.assertEqual(self.runtime.run("answer"), 2)
        self.define("source", lambda: 41, seal=False)
        with self.assertRaises(cell.UnsealedCellError):
            self.runtime.run("answer")
        self.runtime.eo("source")
        self.assertEqual(self.runtime.run("answer"), 42)

    def test_reset_clears_definitions_and_results(self):
        self.define("answer", lambda: 42)
        self.runtime.run("answer")
        self.runtime.reset()
        with self.assertRaises(cell.ResultUnavailableError):
            self.runtime.result("answer")
        with self.assertRaises(cell.UnknownCellError):
            self.runtime.compile("answer")

    def test_independent_runtimes_have_separate_registries(self):
        other = cell.Runtime()
        self.define("answer", lambda: 42)
        with self.assertRaises(cell.UnknownCellError):
            other.compile("answer")

    def test_execution_errors_preserve_original_cause_and_skip_descendants(self):
        called = []
        original = ValueError("deliberate failure")

        def fail():
            raise original

        self.define("failure", fail)
        self.define("descendant", lambda: called.append("descendant"), "failure")
        with self.assertRaises(cell.CellExecutionError) as caught:
            self.runtime.run("descendant")
        self.assertEqual(caught.exception.cell_name, "failure")
        self.assertIs(caught.exception.cause, original)
        self.assertEqual(called, [])
        with self.assertRaises(cell.ResultUnavailableError):
            self.runtime.result("descendant")

    def test_max_workers_must_be_positive_integer(self):
        for value in (0, -1, 1.5, "2", True):
            with self.subTest(value=value):
                with self.assertRaises((ValueError, TypeError)):
                    cell.Runtime(max_workers=value)


class SyncConcurrencyTests(unittest.TestCase):
    def test_independent_sync_cells_run_concurrently(self):
        runtime = cell.Runtime(max_workers=2)
        rendezvous = threading.Barrier(2, timeout=5)

        @runtime.c("left")
        def left():
            rendezvous.wait()
            return 19

        @runtime.c("right")
        def right():
            rendezvous.wait()
            return 23

        runtime.eo("left")
        runtime.eo("right")
        self.assertEqual(runtime.run(["left", "right"]), {"left": 19, "right": 23})

    def test_ready_descendant_starts_without_waiting_for_entire_stage(self):
        runtime = cell.Runtime(max_workers=2)
        descendant_started = threading.Event()

        @runtime.c("slow")
        def slow():
            if not descendant_started.wait(5):
                raise AssertionError("ready descendant was blocked behind unrelated cell")
            return "slow done"

        @runtime.c("fast")
        def fast():
            return 21

        @runtime.c("descendant", need="fast")
        def descendant(fast):
            descendant_started.set()
            return fast * 2

        for name in ("slow", "fast", "descendant"):
            runtime.eo(name)
        self.assertEqual(runtime.run(["slow", "descendant"]),
                         {"slow": "slow done", "descendant": 42})

    def test_failure_waits_for_running_sync_work_to_finish(self):
        runtime = cell.Runtime(max_workers=2)
        started = threading.Event()
        failing = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        returned = threading.Event()
        original = ValueError("fail after sibling starts")

        @runtime.c("slow")
        def slow():
            started.set()
            if not release.wait(5):
                raise AssertionError("test failed to release worker")
            finished.set()

        @runtime.c("failure")
        def failure():
            if not started.wait(5):
                raise AssertionError("sibling was never started")
            failing.set()
            raise original

        runtime.eo("slow")
        runtime.eo("failure")

        def run():
            try:
                runtime.run(["slow", "failure"])
            finally:
                returned.set()

        with ThreadPoolExecutor(max_workers=1) as outer:
            future = outer.submit(run)
            try:
                self.assertTrue(failing.wait(5), "failure cell did not execute")
                self.assertFalse(returned.wait(0.15), "run returned with a worker still active")
            finally:
                release.set()
            with self.assertRaises(cell.CellExecutionError) as caught:
                future.result(timeout=5)
        self.assertIs(caught.exception.cause, original)
        self.assertTrue(finished.is_set())


class AsyncRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_returns_awaitable_inside_event_loop(self):
        runtime = cell.Runtime()

        @runtime.c("answer")
        async def answer():
            return 42

        runtime.eo("answer")
        pending = runtime.run("answer")
        self.assertTrue(inspect.isawaitable(pending))
        self.assertEqual(await pending, 42)
        self.assertEqual(runtime.result("answer"), 42)
        self.assertEqual(await runtime.run_async(["answer"]), {"answer": 42})

    async def test_independent_async_cells_run_concurrently(self):
        runtime = cell.Runtime(max_workers=2)
        left_started = asyncio.Event()
        right_started = asyncio.Event()

        @runtime.c("left")
        async def left():
            left_started.set()
            await asyncio.wait_for(right_started.wait(), 5)
            return 19

        @runtime.c("right")
        async def right():
            right_started.set()
            await asyncio.wait_for(left_started.wait(), 5)
            return 23

        runtime.eo("left")
        runtime.eo("right")
        self.assertEqual(await runtime.run_async(["left", "right"]),
                         {"left": 19, "right": 23})

    async def test_mixed_async_and_sync_dependency_chain(self):
        runtime = cell.Runtime()
        loop_thread = threading.get_ident()

        @runtime.c("async_source")
        async def async_source(value):
            self.assertEqual(threading.get_ident(), loop_thread)
            return value

        @runtime.c("sync_middle", need="async_source")
        def sync_middle(async_source):
            self.assertNotEqual(threading.get_ident(), loop_thread)
            return async_source * 2

        @runtime.c("answer", need="sync_middle")
        async def answer(sync_middle):
            return sync_middle + 2

        for name in ("async_source", "sync_middle", "answer"):
            runtime.eo(name)
        self.assertEqual(await runtime.run_async("answer", value=20), 42)

    async def test_concurrent_runs_keep_inputs_and_results_in_own_context(self):
        runtime = cell.Runtime(max_workers=2)
        both_started = asyncio.Event()
        starts = []

        @runtime.c("answer")
        async def answer(value):
            starts.append(value)
            if len(starts) == 2:
                both_started.set()
            await asyncio.wait_for(both_started.wait(), 5)
            return value * 2

        runtime.eo("answer")

        async def caller(value):
            result = await runtime.run_async("answer", value=value)
            await asyncio.sleep(0)
            return result, runtime.result("answer")

        self.assertEqual(await asyncio.gather(caller(10), caller(20)),
                         [(20, 20), (40, 40)])
        with self.assertRaises(cell.ResultUnavailableError):
            runtime.result("answer")

    async def test_async_failure_skips_descendants_and_settles_started_sibling(self):
        runtime = cell.Runtime(max_workers=2)
        started = asyncio.Event()
        finished = asyncio.Event()
        never = asyncio.Event()
        called = []
        original = ValueError("async failure")

        @runtime.c("sibling")
        async def sibling():
            started.set()
            try:
                await never.wait()
            finally:
                finished.set()

        @runtime.c("failure")
        async def failure():
            await asyncio.wait_for(started.wait(), 5)
            raise original

        @runtime.c("descendant", need="failure")
        async def descendant():
            called.append("descendant")

        for name in ("sibling", "failure", "descendant"):
            runtime.eo(name)
        with self.assertRaises(cell.CellExecutionError) as caught:
            await asyncio.wait_for(runtime.run_async(["sibling", "descendant"]), 5)
        self.assertEqual(caught.exception.cell_name, "failure")
        self.assertIs(caught.exception.cause, original)
        self.assertTrue(finished.is_set())
        self.assertEqual(called, [])


if __name__ == "__main__":
    unittest.main()
