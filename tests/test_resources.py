"""Resource admission uses reservations for running work, including async cells."""

import asyncio
import threading
import unittest

from datacellpy import DefinitionError, ResourceError, Runtime


class ResourceTests(unittest.TestCase):
    def test_invalid_units_are_rejected(self):
        for resources in ([1], {"cpu": -1}, {"cpu": 1.5}, {"cpu": True}, {"": 1}, {1: 2}):
            with self.subTest(resources=resources):
                with self.assertRaises(ValueError):
                    Runtime(resources=resources)
                with self.assertRaises(DefinitionError):
                    Runtime().cell("invalid", resources=resources)

    def test_unsatisfiable_demands_fail_before_any_function_starts(self):
        for demands in ({"gpu": 2}, {"missing": 1}):
            with self.subTest(demands=demands):
                runtime = Runtime(resources={"gpu": 1})
                called = []
                runtime.cell("valid")(lambda: called.append("valid"))
                runtime.cell("invalid", resources=demands)(lambda: called.append("invalid"))
                runtime.endof("valid")
                runtime.endof("invalid")
                with self.assertRaises(ResourceError):
                    runtime.run()
                self.assertEqual(called, [])

    def test_sync_workers_share_the_same_resource_budget(self):
        runtime = Runtime(max_workers=3, resources={"cpu": 2})
        lock = threading.Lock()
        active = 0
        peak = 0
        rendezvous = threading.Barrier(2, timeout=3)

        def work():
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                rendezvous.wait()
                return 1
            finally:
                with lock:
                    active -= 1

        for name in ("first", "second"):
            runtime.cell(name, resources={"cpu": 1})(work)
            runtime.endof(name)
        self.assertEqual(runtime.run(), {"first": 1, "second": 1})
        self.assertEqual(peak, 2)


class AsyncResourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_declared_budgets_are_respected(self):
        runtime = Runtime(max_workers=4, resources={"cpu": 3, "gpu": 1, "memory": 8})
        used = {"cpu": 0, "gpu": 0, "memory": 0}
        peak = dict(used)

        def register(name, demands):
            @runtime.cell(name, resources=demands)
            async def work():
                for resource, demand in demands.items():
                    used[resource] += demand
                    peak[resource] = max(peak[resource], used[resource])
                    self.assertLessEqual(used[resource], runtime.resources[resource])
                await asyncio.sleep(0.01)
                for resource, demand in demands.items():
                    used[resource] -= demand
                return name

            runtime.endof(name)

        register("gpu1", {"cpu": 1, "gpu": 1, "memory": 4})
        register("gpu2", {"cpu": 1, "gpu": 1, "memory": 4})
        register("cpu1", {"cpu": 2, "memory": 4})
        register("cpu2", {"cpu": 1, "memory": 8})
        result = await asyncio.wait_for(runtime.run_async(), 3)
        self.assertEqual(set(result), {"gpu1", "gpu2", "cpu1", "cpu2"})
        self.assertEqual(used, {"cpu": 0, "gpu": 0, "memory": 0})
        self.assertEqual(peak["gpu"], 1)

    async def test_blocked_large_cell_does_not_block_smaller_ready_cells(self):
        runtime = Runtime(max_workers=3, resources={"memory": 2})
        small_finished = asyncio.Event()
        order = []

        @runtime.cell("holding", resources={"memory": 1})
        async def holding():
            order.append("holding")
            await asyncio.wait_for(small_finished.wait(), 2)
            return 1

        @runtime.cell("large", resources={"memory": 2})
        async def large():
            order.append("large")
            return 2

        @runtime.cell("small", resources={"memory": 1})
        async def small():
            order.append("small")
            small_finished.set()
            return 3

        for name in ("holding", "large", "small"):
            runtime.endof(name)
        self.assertEqual(await asyncio.wait_for(runtime.run_async(), 3),
                         {"holding": 1, "large": 2, "small": 3})
        self.assertEqual(order, ["holding", "small", "large"])

    async def test_dynamic_wait_releases_and_reacquires_worker_and_resources(self):
        runtime = Runtime(max_workers=1, incremental=True, resources={"gpu": 1})
        calls = []

        @runtime.cell("child", resources={"gpu": 1})
        async def child():
            calls.append("child")
            return 21

        @runtime.cell("parent", resources={"gpu": 1})
        async def parent():
            calls.append("parent")
            return (await runtime.require("child")) * 2

        runtime.endof("child")
        runtime.endof("parent")
        self.assertEqual(await asyncio.wait_for(runtime.run_async("parent"), 2), 42)
        self.assertEqual(await asyncio.wait_for(runtime.run_async("parent"), 2), 42)
        self.assertEqual(calls, ["parent", "child"])

    async def test_resumed_parent_waits_for_other_resource_holder(self):
        runtime = Runtime(max_workers=2, resources={"gpu": 1})
        child_finished = asyncio.Event()
        holder_started = asyncio.Event()
        order = []

        @runtime.cell("parent", resources={"gpu": 1})
        async def parent():
            value = await runtime.require("child")
            order.append("parent resumed")
            return value

        @runtime.cell("holder", resources={"gpu": 1})
        async def holder():
            holder_started.set()
            await asyncio.wait_for(child_finished.wait(), 2)
            await asyncio.sleep(0)
            order.append("holder done")

        @runtime.cell("child")
        async def child():
            await asyncio.wait_for(holder_started.wait(), 2)
            child_finished.set()
            return 42

        for name in ("parent", "holder", "child"):
            runtime.endof(name)
        self.assertEqual(await asyncio.wait_for(runtime.run_async(["parent", "holder"]), 3),
                         {"parent": 42, "holder": None})
        self.assertEqual(order, ["holder done", "parent resumed"])

    async def test_local_require_timeout_reacquires_resources_and_drains_child(self):
        runtime = Runtime(max_workers=2, incremental=True, resources={"gpu": 1})
        release = asyncio.Event()
        child_finished = asyncio.Event()

        @runtime.cell("child")
        async def child():
            await release.wait()
            child_finished.set()
            return 1

        @runtime.cell("parent", resources={"gpu": 1})
        async def parent():
            try:
                await asyncio.wait_for(runtime.require("child"), 0.01)
            except asyncio.TimeoutError:
                release.set()
                return 42
            self.fail("dynamic child should have timed out")

        runtime.endof("child")
        runtime.endof("parent")
        self.assertEqual(await asyncio.wait_for(runtime.run_async("parent"), 2), 42)
        self.assertTrue(child_finished.is_set())


if __name__ == "__main__":
    unittest.main()
