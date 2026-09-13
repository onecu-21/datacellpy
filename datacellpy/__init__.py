"""DataCellPy: sequential inside, dependency-driven concurrency outside."""

from .errors import (
    CellError, CellExecutionError, CycleError, DefinitionError, InputError,
    ResultUnavailableError, UnknownCellError, UnsealedCellError,
)
from .runtime import ExecutionPlan, Runtime

__version__ = "1.0.0"

_default = Runtime()
cell = _default.cell
c = cell
endof = _default.endof
eo = endof
compile = _default.compile
graph = _default.graph
run = _default.run
run_async = _default.run_async
result = _default.result
reset = _default.reset

__all__ = [
    "Runtime", "ExecutionPlan", "cell", "c", "endof", "eo", "compile", "graph",
    "run", "run_async", "result", "reset", "CellError", "CellExecutionError",
    "CycleError", "DefinitionError", "InputError", "ResultUnavailableError",
    "UnknownCellError", "UnsealedCellError", "__version__",
]
