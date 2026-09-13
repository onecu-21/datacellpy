# DataCellPy

[![PyPI](https://img.shields.io/pypi/v/datacellpy.svg)](https://pypi.org/project/datacellpy/)
[![Python](https://img.shields.io/pypi/pyversions/datacellpy.svg)](https://pypi.org/project/datacellpy/)
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

## Cell registration

Import the module with `import datacellpy as cell` to use the default runtime. Full names and short aliases have identical behavior:

| Operation | Full name | Alias |
| --- | --- | --- |
| Register a function as a cell | `cell.cell(name, need=None)` | `cell.c(name, need=None)` |
| Seal a cell definition | `cell.endof(name)` | `cell.eo(name)` |

`cell()` returns a decorator for a synchronous function or an `async def` function. Cell names must be non-empty strings. Registration does not execute the function. Each definition must be sealed with `endof()` before the runtime can execute it.

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

Cells unrelated to the requested targets do not run. There is no cache between runs: each call executes the required cells again.

Outside an event loop, `run()` waits for execution and returns the result. Inside an event loop, it returns an awaitable; use `await cell.run(...)`. The explicit asynchronous entry point, `await cell.run_async(...)`, is also available.

## Plans, graphs, and results

| API | Purpose |
| --- | --- |
| `cell.compile(target=None)` | Validate the selected dependency graph and return an immutable `ExecutionPlan` |
| `cell.graph(target=None)` | Return the selected graph as Mermaid source text |
| `cell.result(name)` | Retrieve a cell's value from the current context's latest successful run |

`compile()` and `graph()` do not execute cell functions. Plans expose `targets`, `stages`, `order`, and `dependencies`; `str(plan)` displays the stages, and `plan.graph()` returns Mermaid source text.

Plan stages describe dependency depth, not synchronization barriers. During execution, a cell can start as soon as its own dependencies finish and a worker slot is available.

Stored results include dependency cells executed in the same run. Failed or cancelled runs do not replace the previous successful results. Registering, replacing, or newly sealing a cell invalidates stored results, as does `reset()`. Reading a missing or invalidated result raises `ResultUnavailableError`.

Results are stored in the current execution context. Runs in separate asynchronous tasks do not publish their results back into the caller's context; use their returned values.

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

DataCellPy does not remove CPython's GIL. Threads can help with I/O waits, but they do not automatically speed up CPU-bound pure Python code. Version 1.0.0 does not provide JIT compilation, a process pool, caching between runs, or automatic retries.

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
