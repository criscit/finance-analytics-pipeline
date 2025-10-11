# datacontract_io/errors.py
"""Custom exceptions for data contract I/O operations."""


class ContractError(Exception):
    """Raised when data contract validation or enforcement fails."""

    def __init__(
        self,
        message: str,
        contract_path: str | None = None,
        details: dict[str, object] | None = None,
    ):
        self.contract_path = contract_path
        self.details = details or {}
        super().__init__(message)


class SchemaError(Exception):
    """Raised when schema validation or enforcement fails."""

    def __init__(
        self,
        message: str,
        columns: list[str] | None = None,
        details: dict[str, object] | None = None,
    ):
        self.columns = columns or []
        self.details = details or {}
        super().__init__(message)


class ReadError(Exception):
    """Raised when data reading operations fail."""

    def __init__(self, message: str, file_path: str | None = None, line_number: int | None = None):
        self.file_path = file_path
        self.line_number = line_number
        super().__init__(message)


class WriteError(Exception):
    """Raised when data writing operations fail."""

    def __init__(
        self, message: str, output_path: str | None = None, cause: Exception | None = None
    ):
        self.output_path = output_path
        self.cause = cause
        super().__init__(message)
