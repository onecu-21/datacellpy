"""Cancellation must settle started work before returning to its caller."""

import asyncio
import threading
import unittest

from datacellpy import Runtime


class CancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_cancellation_waits_for_running_sync_worker(self):
        runtime = Runtime(max_workers=2)
        loop = asyncio.get_running_loop()
        worker_started = asyncio.Event()
        observer_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        caller_finished = asyncio.Event()
        release_worker = threading.Event()
        worker_finished = threading.Event()

        @runtime.cell("worker")
        def worker():
            loop.call_soon_threadsafe(worker_started.set)
            try:
                if not release_worker.wait(5):
                    raise AssertionError("test did not release the worker")
            finally:
                worker_finished.set()

        @runtime.cell("observer")
        async def observer():
            observer_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()

        for name in ("worker", "observer"):
            runtime.endof(name)

        async def caller():
            try:
                await runtime.run_async(["worker", "observer"])
            finally:
                caller_finished.set()

        task = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(worker_started.wait(), 3)
            await asyncio.wait_for(observer_started.wait(), 3)
            task.cancel()
            await asyncio.wait_for(cleanup_started.wait(), 3)
            task.cancel()
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(caller_finished.wait(), 0.1)
            self.assertFalse(worker_finished.is_set())

            release_worker.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
            self.assertTrue(worker_finished.is_set())
            self.assertTrue(caller_finished.is_set())
        finally:
            release_worker.set()
            if not task.done():
                task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 6)

    async def test_repeated_cancellation_allows_async_child_cleanup_to_finish(self):
        runtime = Runtime()
        child_started = asyncio.Event()
        cleanup_started = asyncio.Event()
        release_cleanup = asyncio.Event()
        cleanup_finished = asyncio.Event()
        caller_finished = asyncio.Event()
        descendants = []

        @runtime.cell("child")
        async def child():
            child_started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cleanup_started.set()
                await asyncio.wait_for(release_cleanup.wait(), 5)
                cleanup_finished.set()

        @runtime.cell("descendant", need="child")
        async def descendant():
            descendants.append("executed")

        runtime.endof("child")
        runtime.endof("descendant")

        async def caller():
            try:
                await runtime.run_async("descendant")
            finally:
                caller_finished.set()

        task = asyncio.create_task(caller())
        try:
            await asyncio.wait_for(child_started.wait(), 3)
            task.cancel()
            await asyncio.wait_for(cleanup_started.wait(), 3)
            task.cancel()
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(caller_finished.wait(), 0.1)
            self.assertFalse(cleanup_finished.is_set())

            release_cleanup.set()
            with self.assertRaises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
            self.assertTrue(cleanup_finished.is_set())
            self.assertTrue(caller_finished.is_set())
            self.assertEqual(descendants, [])
        finally:
            release_cleanup.set()
            if not task.done():
                task.cancel()
            await asyncio.wait_for(asyncio.gather(task, return_exceptions=True), 6)


if __name__ == "__main__":
    unittest.main()
