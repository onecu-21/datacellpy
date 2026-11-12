# DataCellPy

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

DataCellPy is a small Python runtime that registers functions as **cells** and executes them according to explicit data dependencies. It uses ordinary Python code and libraries without a separate language syntax or parser.

Register cells, seal their definitions with `endof()`, and call `run()`. The runtime executes only the requested targets and their dependencies. Independent cells can run concurrently.

## Requirements and installation

Python 3.10 or later is required. The runtime has no external dependencies.

Install from PyPI:

```sh
pip install datacellpy
```

For development, install from the project directory:

```sh
python -m pip install -e .
```

The distribution name and Python import name are both `datacellpy`.

On Windows, `run.ps1` locates an available Python interpreter, including the bundled Codex Python when available. Run `.\run.ps1` without arguments to display usage, or pass Python arguments to the script.

## Development releases

The `developing` branch is the development (Dev) branch. Every push builds the
package and runs the test suite on Python 3.10–3.14, including Windows on Python
3.14. After all checks pass, GitHub Actions creates a GitHub prerelease with the
wheel (`.whl`) and source distribution (`.tar.gz`) attached. Each workflow run
and rerun has a unique `dev-<run-id>-<attempt>` tag, and development releases do
not replace the latest stable GitHub release.

Development builds are **never uploaded to PyPI**. Download their packages from
[GitHub Releases](https://github.com/onecu-21/datacellpy/releases), or install the
current development branch directly:

```sh
python -m pip install "git+https://github.com/onecu-21/datacellpy.git@developing"
```

PyPI releases remain a separate, manually requested operation on `main`. To
rerun development validation and create another prerelease manually, select
the `developing` branch when running the workflow in GitHub Actions.

## Cell registration

Import the module with `import datacellpy as cell` to use the default runtime. Full names and short aliases have identical behavior:

| Operation | Full name | Alias |
| --- | --- | --- |
| Register a function as a cell | `cell.cell(name, need=None)` | `cell.c(name, need=None)` |
| Seal a cell definition | `cell.endof(name)` | `cell.eo(name)` |

`cell()` returns a decorator for a synchronous function or an `async def` function. Cell names must be non-empty strings. Registration does not execute the function. Each definition must be sealed with `endof()` before the runtime can execute it.

Registration also accepts keyword-only options `cache=True` and `resources=None`.
They control incremental reuse and logical resource reservations as described below.

## Dependencies and inputs

The `need` argument declares dependencies and how their return values are passed to the cell:

| Form | Meaning |
| --- | --- |
| Omitted or `None` | No dependencies |
| A cell name | Depend on one cell |
| An iterable of cell names | Depend on multiple cells |
| A mapping from parameter names to cell names | Pass each dependency result to the corresponding parameter |

With a name or iterable, dependency results are passed to parameters with matching names. If no parameter matches, the dependency only determines execution order. Use a mapping to bind names containing dots or hyphens to valid parameter names.

Pass additional inputs as keyword arguments to `run()` or `run_async()`. Inputs are matched to each selected cell's parameter names. Dependency results take precedence over inputs with the same name. An input accepted by no selected cell raises `InputError`. A cell with `**kwargs` can receive otherwise unmatched inputs and dependency values.

Required inputs, missing dependencies, cycles, and unsealed definitions are checked before any cell function starts. Input binding is based on parameter names; positional-only parameters and generator functions are not supported.

## Execution and return values

Use `cell.run(target, **inputs)` to execute cells. The target, when provided, is a positional argument.

| Target | Execution scope | Return value |
| --- | --- | --- |
| One cell name | That cell and its dependencies | The target cell's return value |
| An iterable of cell names | Requested cells and their dependencies | A dictionary containing the requested targets and their values |
| Omitted or `None` | All registered cells | A dictionary containing all cells and their values |

Cells unrelated to the requested targets do not run. By default, each call executes the required cells again. Opt into reuse between successful runs with `Runtime(incremental=True)`.

Outside an event loop, `run()` waits for execution and returns the result. Inside an event loop, it returns an awaitable; use `await cell.run(...)`. The explicit asynchronous entry point, `await cell.run_async(...)`, is also available.

## Plans, graphs, and results

| API | Purpose |
| --- | --- |
| `cell.compile(target=None)` | Validate the selected dependency graph and return an immutable `ExecutionPlan` |
| `cell.graph(target=None)` | Return the selected graph as Mermaid source text |
| `cell.result(name)` | Retrieve a cell's value from the current context's latest successful run |
| `cell.require(target)` | Await dependencies discovered inside a running async cell |
| `cell.set_dependencies(name, need=None)` | Replace a sealed cell's static dependencies for subsequent runs |
| `cell.invalidate(name=None)` | Clear incremental cache for a cell and its descendants, or all cells |

`compile()` and `graph()` do not execute cell functions. Plans expose `targets`, `stages`, `order`, and `dependencies`; `str(plan)` displays the stages, and `plan.graph()` returns Mermaid source text.

Plan stages describe dependency depth, not synchronization barriers. During execution, a cell can start as soon as its own dependencies finish and a worker slot is available.

Stored results include dependency cells executed in the same run. Failed or cancelled runs do not replace the previous successful results. Registering, replacing, or newly sealing a cell invalidates stored results, as does `reset()`. Reading a missing or invalidated result raises `ResultUnavailableError`.

Results are stored in the current execution context. Runs in separate asynchronous tasks do not publish their results back into the caller's context; use their returned values.

## Incremental execution

Create `Runtime(incremental=True)` to reuse successful computations when a cell's
definition, external inputs, and dependency executions are unchanged. A changed
input or definition reruns its affected branch and descendants; independent
branches can reuse their values. Dependency tracking also includes ordering-only
edges and dependencies discovered through `require()`.

```python
from datacellpy import Runtime

runtime = Runtime(incremental=True)

@runtime.cell("source")
def source(value):
    return value * 2

@runtime.cell("answer", need="source")
def answer(source):
    return source + 2

runtime.endof("source")
runtime.endof("answer")

assert runtime.run("answer", value=20) == 42
assert runtime.run("answer", value=20) == 42  # Reuses both cells.
assert runtime.run("answer", value=30) == 62  # Reruns source and answer.
runtime.invalidate("source")                # Invalidates its descendants too.
```

Only successful runs commit cache updates. Failed or cancelled runs preserve the
previous cache. `invalidate()` clears the entire cache, and `reset()` also clears
the registry. Cache values are shared by runs of the same runtime, while
`result()` remains local to the caller's execution context.

Inputs are fingerprinted using their pickle representation, so changes inside
lists and dictionaries are detected. Cached results are stored as in-memory
pickle snapshots and restored as separate objects on reuse. Values that cannot
be serialized are executed normally without caching. The cache is not persisted
to disk and stores only the most recent successful entry per cell.

Use incremental caching for deterministic computations. Declare
`@runtime.cell("read_file", cache=False)` for a cell that must read external
state or perform a side effect on every run; its descendants also rerun. Changes
to files, globals, closures, or services are not detected automatically. Call
`invalidate(name)` after such changes, or disable caching for the affected cell.

## Dynamic DAG

An asynchronous cell can choose dependencies at execution time with
`await runtime.require(target)`. A single name returns that cell's value, and an
iterable returns a dictionary of requested values, matching `run()`.

```python
from datacellpy import Runtime

runtime = Runtime(max_workers=1)

@runtime.cell("left")
def left():
    return 21

@runtime.cell("right")
def right():
    return 42

@runtime.cell("route")
async def route(branch):
    return await runtime.require(branch)

for name in ("left", "right", "route"):
    runtime.endof(name)

assert runtime.run("route", branch="right") == 42  # left does not execute.
```

Dynamic dependencies share the current run's inputs, results, worker limit, and
resource budgets. Shared dependencies execute at most once in a run, and nested
`require()` calls work even with one worker. A suspended caller releases its
worker slot and logical resource reservations, then reacquires them before
resuming. Await requests sequentially; request parallel dependencies together
with `await runtime.require(["left", "right"])`.

You can register and seal new cells during execution, then request them with
`require()`. Already selected definitions remain snapshots for that run; edits
to those definitions apply on the next run. New closures are validated before
they start. Invalid requests raise `UnknownCellError`, `UnsealedCellError`,
`InputError`, `ResourceError`, or `CycleError` inside the requesting cell. As with
other cell errors, uncaught exceptions are wrapped in `CellExecutionError`.
An executing dependency's failure aborts the run and settles active work.

Run inputs must satisfy the initially selected cells as usual. If a dynamic-only
cell needs an input, declare that parameter on the requesting cell too, so the
initial input validation can recognize it. `require()` takes no separate input
overrides. Dynamically discovered edges belong to that execution and its cache;
`compile()` and `graph()` describe the registered static graph.

To change the static DAG between runs without replacing a function, use
`runtime.set_dependencies("answer", {"source": "other_source"})`. The cell stays
sealed. Unknown dependencies, invalid aliases, unsealed dependencies, and cycles
are rejected atomically; a failed edit leaves the original graph intact.

## Resource-aware scheduler

Declare resource capacities on the runtime and demands on individual cells:

```python
from datacellpy import Runtime

runtime = Runtime(max_workers=4, resources={"cpu": 4, "gpu": 1, "memory": 8192})

@runtime.cell("transform", resources={"cpu": 2, "memory": 2048})
def transform():
    return [1, 2, 3]

@runtime.cell("score", need="transform", resources={"gpu": 1, "memory": 4096})
async def score(transform):
    return sum(transform)

runtime.endof("transform")
runtime.endof("score")
assert runtime.run("score") == 6
```

Resource names are arbitrary and amounts are non-negative integers. Choose a
consistent unit for each name, such as MiB for `memory`. A cell starts only when
all its declared demands fit the available capacities and a worker slot is
available. Reservations cover the entire invocation of both synchronous and
asynchronous cells. A large waiting cell does not block smaller ready cells that
fit. Cached results need no reservations.

Undeclared resources or demands larger than capacity raise `ResourceError`
before any initially selected function starts. Dynamic additions receive the
same validation before being admitted. Budgets apply independently to each run;
they are logical scheduling limits, not OS memory limits, hardware allocation,
or coordination across concurrent runs. Cells without declarations reserve only
a worker slot. Incremental execution, dynamic DAGs, and resource limits can be
used together on the same runtime.

## Concurrency and failures

Synchronous cells run in threads; asynchronous cells run cooperatively on the event loop. They can be combined in the same dependency graph.

Create an isolated runtime with `from datacellpy import Runtime` and `Runtime(max_workers=8)`. The default limit is eight concurrently active cells per run. `max_workers` must be a positive integer and also limits the synchronous worker threads for that run.

A cell function's exception is wrapped in `CellExecutionError`. Its `cell_name` attribute identifies the failed cell, and `cause` contains the original exception.

When a run fails or is cancelled, the runtime stops scheduling new cells and cancels active asynchronous work. Python cannot forcibly stop an already running synchronous function, so cleanup waits for those functions to finish before returning the error or cancellation. Side effects, such as file writes, are not rolled back.

Dependency values are passed by reference without copying. If multiple cells modify the same mutable object, use dependencies to enforce their execution order.

## Isolated runtimes and replacing cells

The module-level API uses a shared default runtime. Each `Runtime()` instance has its own cell registry and provides the same registration, planning, execution, and result APIs. Use separate instances for independent projects or jobs.

Registering a cell under an existing name replaces its definition. Seal the new definition again with `endof(name)` before executing it. `reset()` clears the runtime's registry and invalidates its results.

## Imports across modules

Import a library in each Python module that uses it directly. DataCellPy follows Python's normal import rules; imports in one module are not automatically available in another.

If you split your application into `main.py` and `define.py`:

- If only the cell functions in `define.py` use NumPy, add `import numpy as np` to `define.py` only.
- If both files use NumPy directly, add the import to both files.
- If `main.py` only imports your cell definitions and calls `cell.run()`, it does not need to import NumPy.

A function uses the global names of the module where it was defined, even when DataCellPy executes it from another module. Importing NumPy in `main.py` therefore does not make `np` available to functions defined in `define.py`.

Importing the same library in multiple modules normally reuses Python's cached module within the same process; it does not load a separate copy for each file. Install any additional libraries separately. DataCellPy's `need` argument declares dependencies between cells; it does not install or import Python packages.

## Limitations

DataCellPy does not remove CPython's GIL. Threads can help with I/O waits, but they do not automatically speed up CPU-bound pure Python code. The runtime does not provide JIT compilation, a process pool, or automatic retries. Incremental caching is opt-in and in-memory; resource budgets are declared by the application.

## Support

Report bugs and request features through [GitHub Issues](https://github.com/onecu-21/datacellpy/issues). Include your Python version, operating system, a minimal reproduction, and any relevant error output when reporting a bug.

## Project files and tests

- `datacellpy/`: cell registration, dependency planning, and execution runtime.
- `tests/`: runtime tests.
- `pyproject.toml`: package metadata and build configuration.
- `run.ps1`: Windows Python launcher.

Run the test suite from the project directory:

```sh
python -m unittest discover -s tests -v
```

On Windows, the launcher can run the same tests:

```powershell
.\run.ps1 -m unittest discover -s tests -v
```
