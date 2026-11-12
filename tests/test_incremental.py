"""Incremental execution must track inputs, definitions and dependency lineage."""

import asyncio
from collections import Counter
import threading
import unittest

from datacellpy import CellExecutionError, ResultUnavailableError, Runtime


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        self.runtime = Runtime(incremental=True)
        self.calls = Counter()

    def define(self, name, function, need=None, **options):
        def counted(**kwargs):
            self.calls[name] += 1
            return function(**kwargs)

        # Keep the original signature rather than the wrapper's **kwargs.
        counted.__wrapped__ = function
        self.runtime.cell(name, need, **options)(counted)
        self.runtime.endof(name)

    def test_only_changed_input_branch_and_its_descendants_rerun(self):
        self.define("left", lambda x: x * 2)
        self.define("right", lambda y: y * 3)
        self.define("total", lambda left, right: left + right, ["left", "right"])
        self.assertEqual(self.runtime.run("total", x=2, y=3), 13)
        self.assertEqual(self.runtime.run("total", x=2, y=3), 13)
        self.assertEqual(self.calls, {"left": 1, "right": 1, "total": 1})
        self.assertEqual(self.runtime.run("total", x=4, y=3), 17)
        self.assertEqual(self.calls, {"left": 2, "right": 1, "total": 2})
        self.assertEqual(self.runtime.result("right"), 9)

    def test_same_value_from_reexecuted_dependency_still_invalidates_child(self):
        self.define("source", lambda x: 1)
        self.define("child", lambda source: source + 1, "source")
        self.runtime.run("child", x=1)
        self.runtime.run("child", x=2)
        self.assertEqual(self.calls, {"source": 2, "child": 2})

    def test_replacing_definition_invalidates_only_affected_branch(self):
        self.define("source", lambda: 1)
        self.define("child", lambda source: source + 1, "source")
        self.define("other", lambda: 9)
        self.runtime.run()
        self.define("source", lambda: 10)
        self.assertEqual(self.runtime.run(), {"source": 10, "child": 11, "other": 9})
        self.assertEqual(self.calls, {"source": 2, "child": 2, "other": 1})

    def test_invalidate_cell_descendants_and_all(self):
        self.define("source", lambda: 1)
        self.define("child", lambda source: source + 1, "source")
        self.define("other", lambda: 9)
        self.runtime.run()
        self.runtime.invalidate("source")
        with self.assertRaises(ResultUnavailableError):
            self.runtime.result("child")
        self.runtime.run()
        self.assertEqual(self.calls, {"source": 2, "child": 2, "other": 1})
        self.runtime.invalidate()
        self.runtime.run()
        self.assertEqual(self.calls, {"source": 3, "child": 3, "other": 2})

    def test_uncacheable_cell_and_its_descendants_always_rerun(self):
        self.define("source", lambda: 1, cache=False)
        self.define("child", lambda source: source + 1, "source")
        self.runtime.run("child")
        self.runtime.run("child")
        self.assertEqual(self.calls, {"source": 2, "child": 2})

    def test_mutating_input_is_detected_and_returned_values_are_detached(self):
        self.define("copy", lambda values: list(values))
        values = [1]
        first = self.runtime.run("copy", values=values)
        first.append(99)
        self.assertEqual(self.runtime.run("copy", values=values), [1])
        values.append(2)
        self.assertEqual(self.runtime.run("copy", values=values), [1, 2])
        self.assertEqual(self.calls["copy"], 2)

    def test_unserializable_input_and_result_are_not_cached(self):
        lock = threading.Lock()
        self.define("input", lambda value: 1)
        self.define("output", lambda: lock)
        for _ in range(2):
            self.assertEqual(self.runtime.run("input", value=lock), 1)
            self.assertIs(self.runtime.run("output"), lock)
        self.assertEqual(self.calls, {"input": 2, "output": 2})

    def test_failed_run_does_not_commit_partial_cache_or_replace_results(self):
        self.define("source", lambda value: value)
        self.define("child", lambda source: 10 // source, "source")
        self.assertEqual(self.runtime.run("child", value=2), 5)
        with self.assertRaises(CellExecutionError):
            self.runtime.run("child", value=0)
        self.assertEqual(self.runtime.result("child"), 5)
        self.assertEqual(self.runtime.run("child", value=2), 5)
        self.assertEqual(self.calls, {"source": 2, "child": 2})

    def test_reset_discards_cache_even_for_same_function(self):
        def value():
            self.calls["value"] += 1
            return 42

        for _ in range(2):
            self.runtime.cell("value")(value)
            self.runtime.endof("value")
            self.runtime.run("value")
            self.runtime.reset()
        self.assertEqual(self.calls["value"], 2)


class AsyncIncrementalTests(unittest.IsolatedAsyncioTestCase):
    async def test_concurrent_inputs_and_context_results_stay_separate(self):
        runtime = Runtime(incremental=True)
        started = asyncio.Event()
        calls = []

        @runtime.cell("value")
        async def value(x):
            calls.append(x)
            if len(calls) == 2:
                started.set()
            await asyncio.wait_for(started.wait(), 2)
            return x * 2

        runtime.endof("value")

        async def caller(x):
            result = await runtime.run_async("value", x=x)
            return result, runtime.result("value")

        self.assertEqual(await asyncio.gather(caller(2), caller(3)), [(4, 4), (6, 6)])
        with self.assertRaises(ResultUnavailableError):
            runtime.result("value")
        self.assertEqual(await runtime.run_async("value", x=2), 4)
        runtime.invalidate("value")
        with self.assertRaises(ResultUnavailableError):
            runtime.result("value")

    async def test_invalidation_during_run_prevents_cache_publication(self):
        runtime = Runtime(incremental=True)
        calls = []

        @runtime.cell("value")
        async def value():
            calls.append(1)
            if len(calls) == 1:
                runtime.invalidate("value")
            return 42

        runtime.endof("value")
        await runtime.run_async("value")
        with self.assertRaises(ResultUnavailableError):
            runtime.result("value")
        await runtime.run_async("value")
        await runtime.run_async("value")
        self.assertEqual(len(calls), 2)

    async def test_cancelled_run_does_not_commit_cache(self):
        runtime = Runtime(incremental=True)
        started = asyncio.Event()
        release = asyncio.Event()
        calls = []

        @runtime.cell("source")
        async def source():
            calls.append(1)
            return 1

        @runtime.cell("child", need="source")
        async def child(source):
            started.set()
            await release.wait()
            return source

        runtime.endof("source")
        runtime.endof("child")
        task = asyncio.create_task(runtime.run_async("child"))
        await asyncio.wait_for(started.wait(), 2)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        release.set()
        self.assertEqual(await runtime.run_async("child"), 1)
        self.assertEqual(len(calls), 2)


if __name__ == "__main__":
    unittest.main()
