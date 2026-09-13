"""Public exceptions raised by the DataCellPy runtime."""


class CellError(Exception):
    """Base class for DataCellPy errors."""


class DefinitionError(CellError):
    """A cell declaration is invalid."""


class UnknownCellError(CellError):
    def __init__(self, cell_name: str):
        self.cell_name = cell_name
        super().__init__(f"Unknown cell {cell_name!r}. Register it with cell() first.")


class UnsealedCellError(CellError):
    def __init__(self, cell_name: str):
        self.cell_name = cell_name
        super().__init__(f"Cell {cell_name!r} is not closed. Call endof({cell_name!r}).")


class CycleError(CellError):
    def __init__(self, cycle):
        self.cycle = tuple(cycle)
        super().__init__("Dependency cycle: " + " -> ".join(self.cycle))


class InputError(CellError):
    """Run inputs cannot satisfy the selected cells' function signatures."""


class CellExecutionError(CellError):
    def __init__(self, cell_name: str, cause: Exception):
        self.cell_name = cell_name
        self.cause = cause
        super().__init__(f"Cell {cell_name!r} failed: {type(cause).__name__}: {cause}")


class ResultUnavailableError(CellError):
    def __init__(self, cell_name: str):
        self.cell_name = cell_name
        super().__init__(f"No result for {cell_name!r} in this context's latest successful run.")
