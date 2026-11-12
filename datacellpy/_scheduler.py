"""Per-run DAG expansion, incremental snapshots, and resource admission."""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import copy_context
from dataclasses import dataclass
from functools import partial
from hashlib import sha256
import inspect
import pickle
from types import MappingProxyType
from typing import Any

from .errors import CellExecutionError, CellError, CycleError, DefinitionError, ResourceError


def resource_units(resources):
    """Validate logical, integer resource units without guessing machine capacity."""
    if resources is None:
        return ()
    if not isinstance(resources, Mapping):
        raise ValueError("resources must be a mapping of names to non-negative integer units")
    units = tuple(resources.items())
    for name, amount in units:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("Resource names must be non-empty strings")
        if isinstance(amount, bool) or not isinstance(amount, int) or amount < 0:
            raise ValueError(f"Resource {name!r} must use non-negative integer units")
    return units


def snapshot(value):
    try:
        return pickle.dumps(value, protocol=5)
    except Exception:
        # Unsupported objects remain valid runtime values, but are never cached.
        return None


@dataclass(frozen=True)
class _CacheEntry:
    definition: Any
    inputs: bytes
    dependencies: tuple[tuple[str, object], ...]
    dynamic: tuple[tuple[str, ...], ...]
    value: bytes
    token: object


class Execution:
    """All mutable scheduling state belongs to one run and its event loop."""

    def __init__(self, runtime, plan, inputs):
        self.runtime = runtime
        self.plan = plan
        self.inputs = inputs
        self.cells = dict(plan._cells)
        self.dependencies = dict(plan.dependencies)
        self.dynamic = {name: () for name in self.cells}
        self.prepared = runtime._arguments(plan, inputs)
        self.capacities = dict(runtime.resources)
        self._check_resources(self.cells)
        self.states = dict.fromkeys(self.cells, "pending")
        self.results = {}
        self.tokens = {}
        self.active = {}
        self.requests = set()
        self.reservations = set()
        self.used = dict.fromkeys(self.capacities, 0)
        self.completed = {name: asyncio.Event() for name in self.cells}
        self.resuming = {}
        self.changed = asyncio.Event()
        self.closing = False
        self.loop = asyncio.get_running_loop()
        self.executor = ThreadPoolExecutor(max_workers=runtime.max_workers,
                                           thread_name_prefix="datacellpy")
        with runtime._lock:
            self.cache = runtime._cache.copy() if runtime.incremental else {}
            self.epoch = runtime._cache_epoch
        self.writes = {}
        # Fingerprint before user functions can mutate inputs.
        self.fingerprints = {name: self._fingerprint(name) for name in self.cells}

    def _fingerprint(self, name):
        if not self.runtime.incremental or not self.cells[name].cache:
            return None
        encoded = snapshot(self.prepared[name][0])
        return sha256(encoded).digest() if encoded is not None else None

    def _check_resources(self, cells):
        for name, definition in cells.items():
            for resource, demand in definition.resources:
                if resource not in self.capacities:
                    raise ResourceError(f"Cell {name!r} requests undeclared resource {resource!r}.")
                if demand > self.capacities[resource]:
                    raise ResourceError(
                        f"Cell {name!r} requests {demand} units of {resource!r}; "
                        f"capacity is {self.capacities[resource]}."
                    )

    @staticmethod
    def _check_cycles(dependencies):
        visited = set()
        visiting = set()
        for root in dependencies:
            if root in visited:
                continue
            stack = [(root, False)]
            path = []
            while stack:
                name, exiting = stack.pop()
                if exiting:
                    visiting.remove(name)
                    visited.add(name)
                    path.pop()
                elif name in visiting:
                    raise CycleError(path[path.index(name):] + [name])
                elif name not in visited:
                    visiting.add(name)
                    path.append(name)
                    stack.append((name, True))
                    stack.extend((dep, False) for dep in reversed(dependencies[name]))

    def _add(self, caller, targets):
        # Compile against active definitions plus newly registered cells. Edits to
        # definitions already selected by this run take effect on the next run.
        with self.runtime._lock:
            registry = {**self.runtime._cells, **self.cells}
            plan = self.runtime._compile(registry, targets, self.runtime._generation)
        new_cells = {name: definition for name, definition in plan._cells.items()
                     if name not in self.cells}
        prepared = self.runtime._arguments(plan, self.inputs, allow_unused=True)
        self._check_resources(new_cells)
        dependencies = {**self.dependencies,
                        **{name: plan.dependencies[name] for name in new_cells}}
        dependencies[caller] = tuple(dict.fromkeys((*dependencies[caller], *plan.targets)))
        self._check_cycles(dependencies)
        # No partial graph update is visible if validation above fails.
        self.dependencies = dependencies
        self.dynamic[caller] = tuple(dict.fromkeys((*self.dynamic[caller], plan.targets)))
        for name, definition in new_cells.items():
            self.cells[name] = definition
            self.prepared[name] = prepared[name]
            self.dynamic[name] = ()
            self.states[name] = "pending"
            self.completed[name] = asyncio.Event()
            self.fingerprints[name] = self._fingerprint(name)
        self.changed.set()
        return plan.targets, plan._single

    def _fits(self, name):
        return (len(self.reservations) < self.runtime.max_workers and all(
            self.used[resource] + demand <= self.capacities[resource]
            for resource, demand in self.cells[name].resources
        ))

    def _reserve(self, name):
        self.reservations.add(name)
        for resource, demand in self.cells[name].resources:
            self.used[resource] += demand
        self.states[name] = "running"

    def _release(self, name):
        if name in self.reservations:
            self.reservations.remove(name)
            for resource, demand in self.cells[name].resources:
                self.used[resource] -= demand

    async def require(self, name, target):
        if asyncio.get_running_loop() is not self.loop:
            raise DefinitionError("require() must run on the executing cell's event loop.")
        if self.closing or self.states.get(name) != "running":
            raise DefinitionError("Await require() sequentially inside an active cell; use a list for multiple targets.")
        request = asyncio.current_task()
        self.requests.add(request)
        try:
            targets, single = self._add(name, target)
            self.states[name] = "waiting"
            self._release(name)
            self.changed.set()
            try:
                await asyncio.gather(*(self.completed[dep].wait() for dep in targets))
            finally:
                # Suspended async cells release both worker slots and logical units.
                # Reacquire before resuming user code, even after a local timeout.
                if not self.closing:
                    resume = self.loop.create_future()
                    self.resuming[name] = resume
                    self.states[name] = "resuming"
                    self.changed.set()
                    await asyncio.shield(resume)
            return self.results[targets[0]] if single else {dep: self.results[dep] for dep in targets}
        finally:
            self.requests.discard(request)

    async def _invoke(self, name):
        definition = self.cells[name]
        external, injection = self.prepared[name]
        kwargs = {**external, **{alias: self.results[source] for alias, source in injection.items()}}
        context_token = self.runtime._execution.set((self, name))
        try:
            if inspect.iscoroutinefunction(definition.function):
                return await definition.function(**kwargs)
            context = copy_context()
            value = await self.loop.run_in_executor(
                self.executor, context.run, partial(definition.function, **kwargs)
            )
            return await value if inspect.isawaitable(value) else value
        except Exception as exc:
            raise CellExecutionError(name, exc) from exc
        finally:
            self.runtime._execution.reset(context_token)

    def _candidate(self, name):
        entry = self.cache.get(name)
        if (entry is None or self.fingerprints[name] is None
                or entry.definition is not self.cells[name]
                or entry.inputs != self.fingerprints[name]):
            return None
        # Only follow a previous dynamic branch when all static inputs and
        # dependency executions are unchanged.
        tokens = dict(entry.dependencies)
        if any(tokens.get(dep) is not self.tokens[dep] for dep in self.cells[name].needs):
            return None
        return entry

    def _reuse(self, name):
        entry = self._candidate(name)
        if entry is None:
            return False
        previous_tokens = dict(entry.dependencies)
        for targets in entry.dynamic:
            if targets not in self.dynamic[name]:
                try:
                    self._add(name, targets)
                except CellError:
                    self.cache.pop(name, None)
                    return False
            if any(dep not in self.results for dep in targets):
                return False
            if any(self.tokens[dep] is not previous_tokens[dep] for dep in targets):
                # A changed dynamic selector can choose a different next branch.
                # Do not speculatively execute the remainder of its old branch.
                return False
        if any(self.tokens.get(dep) is not token for dep, token in entry.dependencies):
            return False
        try:
            value = pickle.loads(entry.value)
        except Exception:
            return False
        self._finish(name, value, cached=entry)
        return True

    def _finish(self, name, value, *, cached=None):
        self._release(name)
        self.results[name] = value
        self.states[name] = "done"
        self.tokens[name] = cached.token if cached else object()
        self.completed[name].set()
        if cached is not None:
            self.writes[name] = cached
        elif (self.fingerprints[name] is not None
              and all(dep in self.tokens for dep in self.dependencies[name])):
            encoded = snapshot(value)
            self.writes[name] = _CacheEntry(
                self.cells[name], self.fingerprints[name],
                tuple((dep, self.tokens[dep]) for dep in self.dependencies[name]),
                self.dynamic[name], encoded, self.tokens[name],
            ) if encoded is not None else None
        else:
            self.writes[name] = None

    async def _cleanup(self):
        self.closing = True
        tasks = set(self.active.values()) | self.requests
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await self.loop.run_in_executor(
            None, partial(self.executor.shutdown, wait=True, cancel_futures=True)
        )

    async def run(self):
        try:
            while len(self.results) < len(self.cells):
                self.changed.clear()
                # Inspect all finished tasks before admitting any descendants.
                done = [(name, task.result()) for name, task in self.active.items() if task.done()]
                for name, value in done:
                    del self.active[name]
                    self._finish(name, value)
                progressed = bool(done)
                for name in tuple(self.cells):
                    if self.states[name] == "resuming" and self._fits(name):
                        self._reserve(name)
                        self.resuming.pop(name).set_result(None)
                    elif (self.states[name] == "pending"
                          and all(dep in self.results for dep in self.dependencies[name])):
                        if self._reuse(name):
                            progressed = True
                        elif (all(dep in self.results for dep in self.dependencies[name])
                              and self._fits(name)):
                            self._reserve(name)
                            task = asyncio.create_task(self._invoke(name), name=f"datacellpy:{name}")
                            task.add_done_callback(lambda _: self.changed.set())
                            self.active[name] = task
                if len(self.results) == len(self.cells):
                    break
                if not progressed:
                    await self.changed.wait()
        finally:
            cleanup = asyncio.create_task(self._cleanup())
            cancelled = False
            while True:
                try:
                    await asyncio.shield(cleanup)
                    break
                except asyncio.CancelledError:
                    cancelled = True
            if cancelled:
                raise asyncio.CancelledError
        with self.runtime._lock:
            unchanged = (self.runtime._cache_epoch == self.epoch and all(
                self.runtime._cells.get(name) is definition for name, definition in self.cells.items()
            ))
            generation = self.runtime._generation if unchanged else self.plan._generation
            if self.runtime.incremental and self.runtime._cache_epoch == self.epoch:
                for name, entry in self.writes.items():
                    if self.runtime._cells.get(name) is self.cells[name]:
                        if entry is None:
                            self.runtime._cache.pop(name, None)
                        else:
                            self.runtime._cache[name] = entry
        value = (self.results[self.plan.targets[0]] if self.plan._single
                 else {name: self.results[name] for name in self.plan.targets})
        return value, MappingProxyType(self.results), generation
