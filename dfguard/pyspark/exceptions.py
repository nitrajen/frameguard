"""
dfguard.pyspark.exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~~
All exceptions raised by the dfguard PySpark backend.
Every exception carries the full schema history so the caller can trace
exactly where the schema diverged from expectations.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dfguard.pyspark.history import SchemaHistory


class DfTypesError(Exception):
    """Base class for all dfguard errors."""


class SchemaValidationError(DfTypesError):
    """
    Raised when a dataset's runtime schema does not match the declared schema.

    Attributes
    ----------
    errors:
        Human-readable list of individual mismatches.
    history:
        Full schema evolution history of the dataset at the point of failure.
    """

    def __init__(self, errors: list[str], history: SchemaHistory) -> None:
        self.errors  = errors
        self.history = history

        lines = ["Schema validation failed:"]
        for err in errors:
            lines.append(f"  ✗ {err}")

        lines.append("")
        lines.append("Schema evolution (most recent last):")
        for change in history.changes:
            summary = change.summary()
            lines.append(f"  [{change.operation}]  {summary}")

        super().__init__("\n".join(lines))


class TypeAnnotationError(DfTypesError):
    """Raised when a SparkSchema annotation cannot be converted to a Spark DataType."""


class ColumnNotFoundError(DfTypesError):
    """Raised when an expected column is absent from the DataFrame."""
