"""Compile explicit dependencies into a plan and execute ready cells."""

from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar, copy_context
from dataclasses import dataclass, field, replace
from functools import partial
import html
import inspect
import json
import keyword
from threading import RLock
from types import MappingProxyType
from typing import Any

from .errors import (
    CellExecutionError, CycleError, DefinitionError, InputError,
    ResultUnavailableError, UnknownCellError, UnsealedCellError,
)

Need = str | Iterable[str] | Mapping[str, str] | None
Target = str | Iterable[str] | None


@dataclass(frozen=True)
class _Cell:
    name: str
    function: Callable[..., Any]
    signature: inspect.Signature
    bindings: tuple[tuple[str, str], ...]
    sealed: bool = False

    @property
    def needs(self) -> tuple[str, ...]:
        return tuple(dict.fromkeys(source for _, source in self.bindings))


@dataclass(frozen=True)
class ExecutionPlan:
    """An immutable snapshot. Stages visualize dependency depth, not barriers."""

    targets: tuple[str, ...]
    stages: tuple[tuple[str, ...], ...]
    order: tuple[str, ...]
    dependencies: Mapping[str, tuple[str, ...]]
    _cells: Mapping[str, _Cell] = field(repr=False, compare=False)
    _generation: int = field(repr=False, compare=False)
    _single: bool = field(repr=False, compare=False)

    def __str__(self) -> str:
        return "\n".join(
            f"Stage {index}: " + " || ".join(names)
            for index, names in enumerate(self.stages)
        ) or "(empty plan)"

    def graph(self) -> str:
        """Return Mermaid source using generated IDs, allowing arbitrary names."""
        ids = {name: f"c{index}" for index, name in enumerate(self.order)}
        lines = ["flowchart TD"]
        for name in self.order:
            label = html.escape(name, quote=True).replace("\n", " ").replace("\r", " ")
            lines.append(f"    {ids[name]}[{json.dumps(label, ensure_ascii=False)}]")
        for name in self.order:
            lines.extend(f"    {ids[dep]} --> {ids[name]}" for dep in self.dependencies[name])
        return "\n".join(lines)


class Runtime:
    """An isolated cell registry and scheduler, with no cache between runs."""

    def __init__(self, max_workers: int = 8):
        if isinstance(max_workers, bool) or not isinstance(max_workers, int) or max_workers < 1:
            raise ValueError("max_workers must be a positive integer")
        self.max_workers = max_workers
        self._cells: dict[str, _Cell] = {}
        self._lock = RLock()
        self._generation = 0
        self._last: ContextVar[tuple[int, Mapping[str, Any]] | None] = ContextVar(
            f"datacellpy_results_{id(self)}", default=None
        )

    @staticmethod
    def _name(name: str) -> str:
        if not isinstance(name, str) or not name.strip():
            raise DefinitionError("Cell names must be non-empty strings.")
        return name

    def cell(self, name: str, need: Need = None):
        """Register a function without running it; close it with endof()."""
        name = self._name(name)
        explicit_aliases = isinstance(need, Mapping)
        if need is None:
            bindings = ()
        elif explicit_aliases:
            bindings = tuple(need.items())
            for alias, source in bindings:
                if not isinstance(alias, str) or not alias.isidentifier() or keyword.iskeyword(alias):
                    raise DefinitionError(f"Dependency alias {alias!r} must be a parameter name.")
                self._name(source)
        else:
            if isinstance(need, str):
                needs = (need,)
            else:
                try:
                    needs = tuple(need)
                except TypeError as exc:
                    raise DefinitionError("need must be None, a name, names, or an alias mapping.") from exc
            for source in needs:
                self._name(source)
            bindings = tuple((source, source) for source in dict.fromkeys(needs))

        def decorate(function):
            if not callable(function):
                raise DefinitionError(f"Cell {name!r} must wrap a callable.")
            if inspect.isgeneratorfunction(function) or inspect.isasyncgenfunction(function):
                raise DefinitionError(f"Cell {name!r} must return a value, not yield one.")
            try:
                signature = inspect.signature(function)
            except (TypeError, ValueError) as exc:
                raise DefinitionError(f"Cannot inspect cell {name!r}: {exc}") from exc
            if any(p.kind == p.POSITIONAL_ONLY for p in signature.parameters.values()):
                raise DefinitionError(f"Cell {name!r} must use named parameters (no positional-only parameters).")
            accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in signature.parameters.values())
            named = {
                p.name for p in signature.parameters.values()
                if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
            }
            if explicit_aliases and not accepts_kwargs:
                unmatched = [alias for alias, _ in bindings if alias not in named]
                if unmatched:
                    raise DefinitionError(f"Cell {name!r} has no parameters for aliases: {', '.join(unmatched)}")
            definition = _Cell(name, function, signature, bindings)
            with self._lock:
                self._cells[name] = definition
                self._generation += 1
            return function

        return decorate

    c = cell

    def endof(self, name: str) -> None:
        """Seal a declaration. Forward dependencies are validated at compile()."""
        name = self._name(name)
        with self._lock:
            if name not in self._cells:
                raise UnknownCellError(name)
            if not self._cells[name].sealed:
                self._cells[name] = replace(self._cells[name], sealed=True)
                self._generation += 1

    eo = endof

    def compile(self, target: Target = None) -> ExecutionPlan:
        """Validate and topologically order only the requested dependency closure."""
        with self._lock:
            cells = self._cells.copy()
            generation = self._generation
        single = isinstance(target, str)
        if target is None:
            targets = tuple(cells)
        elif single:
            targets = (self._name(target),)
        else:
            try:
                targets = tuple(dict.fromkeys(self._name(name) for name in target))
            except TypeError as exc:
                raise DefinitionError("target must be a cell name or iterable of names.") from exc

        # Iterative DFS keeps long dependency chains independent of Python's recursion limit.
        state: dict[str, int] = {}
        order: list[str] = []
        depth: dict[str, int] = {}
        for root in targets:
            if state.get(root) == 2:
                continue
            stack = [(root, False)]
            path: list[str] = []
            while stack:
                name, exiting = stack.pop()
                if exiting:
                    state[name] = 2
                    path.pop()
                    order.append(name)
                    depth[name] = max((depth[dep] + 1 for dep in cells[name].needs), default=0)
                    continue
                if state.get(name) == 2:
                    continue
                if state.get(name) == 1:
                    raise CycleError(path[path.index(name):] + [name])
                if name not in cells:
                    raise UnknownCellError(name)
                if not cells[name].sealed:
                    raise UnsealedCellError(name)
                state[name] = 1
                path.append(name)
                stack.append((name, True))
                stack.extend((dep, False) for dep in reversed(cells[name].needs))
        stages: list[list[str]] = []
        for name in order:
            while len(stages) <= depth[name]:
                stages.append([])
            stages[depth[name]].append(name)
        return ExecutionPlan(
            targets, tuple(tuple(stage) for stage in stages), tuple(order),
            MappingProxyType({name: cells[name].needs for name in order}),
            MappingProxyType({name: cells[name] for name in order}), generation, single,
        )

    def graph(self, target: Target = None) -> str:
        return self.compile(target).graph()

    @staticmethod
    def _arguments(plan: ExecutionPlan, inputs: Mapping[str, Any]):
        """Preflight every signature before starting any user function."""
        prepared = {}
        used = set()
        for name in plan.order:
            definition = plan._cells[name]
            params = definition.signature.parameters
            accepts_kwargs = any(p.kind == p.VAR_KEYWORD for p in params.values())
            named = {
                p.name for p in params.values()
                if p.kind in (p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY)
            }
            injection = {
                alias: source for alias, source in definition.bindings
                if alias in named or accepts_kwargs
            }
            external = {
                key: value for key, value in inputs.items()
                if (key in named or accepts_kwargs) and key not in injection
            }
            # An input shadowed by an explicit dependency is recognized but never overrides it.
            used.update(key for key in inputs if key in named or accepts_kwargs)
            try:
                definition.signature.bind(**{**external, **dict.fromkeys(injection)})
            except TypeError as exc:
                raise InputError(f"Invalid inputs for cell {name!r}: {exc}") from exc
            prepared[name] = (external, injection)
        unknown = set(inputs) - used
        if unknown:
            raise InputError("Unused run inputs: " + ", ".join(sorted(unknown)))
        return prepared

    async def _execute(self, plan: ExecutionPlan, inputs: Mapping[str, Any]):
        prepared = self._arguments(plan, inputs)
        results: dict[str, Any] = {}
        remaining = {name: len(plan.dependencies[name]) for name in plan.order}
        children: dict[str, list[str]] = {name: [] for name in plan.order}
        for name in plan.order:
            for dep in plan.dependencies[name]:
                children[dep].append(name)
        ready = deque(name for name in plan.order if remaining[name] == 0)
        active: dict[asyncio.Task, str] = {}
        loop = asyncio.get_running_loop()
        executor = ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="datacellpy")

        async def invoke(name):
            definition = plan._cells[name]
            external, injection = prepared[name]
            kwargs = {**external, **{alias: results[source] for alias, source in injection.items()}}
            try:
                if inspect.iscoroutinefunction(definition.function):
                    return await definition.function(**kwargs)
                context = copy_context()
                value = await loop.run_in_executor(
                    executor, context.run, partial(definition.function, **kwargs)
                )
                if inspect.isawaitable(value):
                    return await value
                return value
            except Exception as exc:
                raise CellExecutionError(name, exc) from exc

        try:
            while ready or active:
                while ready and len(active) < self.max_workers:
                    name = ready.popleft()
                    active[asyncio.create_task(invoke(name), name=f"datacellpy:{name}")] = name
                done, _ = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
                # Check all completions for failure before releasing any descendants.
                completed = [(task, active[task], task.result()) for task in active if task in done]
                for task, name, value in completed:
                    del active[task]
                    results[name] = value
                    for child in children[name]:
                        remaining[child] -= 1
                        if remaining[child] == 0:
                            ready.append(child)
        finally:
            async def cleanup():
                for task in active:
                    task.cancel()
                if active:
                    await asyncio.gather(*active, return_exceptions=True)
                # Python cannot kill a running thread. Drain it without blocking the loop.
                await loop.run_in_executor(None, partial(executor.shutdown, wait=True, cancel_futures=True))

            cleanup_task = asyncio.create_task(cleanup())
            cancelled_during_cleanup = False
            while True:
                try:
                    await asyncio.shield(cleanup_task)
                    break
                except asyncio.CancelledError:
                    # Repeated caller cancellation must not abandon running workers.
                    cancelled_during_cleanup = True
            if cancelled_during_cleanup:
                raise asyncio.CancelledError
        value = results[plan.targets[0]] if plan._single else {name: results[name] for name in plan.targets}
        return value, MappingProxyType(results)

    def _publish(self, plan, results):
        self._last.set((plan._generation, results))

    def run(self, target: Target = None, /, **inputs):
        """Return a value in normal Python; return an awaitable inside an event loop."""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            pass
        else:
            return self.run_async(target, **inputs)
        plan = self.compile(target)
        value, results = asyncio.run(self._execute(plan, inputs))
        self._publish(plan, results)
        return value

    async def run_async(self, target: Target = None, /, **inputs):
        """Explicit async entry point, suitable for notebooks and web requests."""
        plan = self.compile(target)
        value, results = await self._execute(plan, inputs)
        self._publish(plan, results)
        return value

    def result(self, name: str):
        """Read this context's latest successful run; invalidated by graph edits."""
        latest = self._last.get()
        with self._lock:
            generation = self._generation
        if latest is None or latest[0] != generation or name not in latest[1]:
            raise ResultUnavailableError(name)
        return latest[1][name]

    def reset(self) -> None:
        """Clear this registry and invalidate all results."""
        with self._lock:
            self._cells.clear()
            self._generation += 1
        self._last.set(None)
