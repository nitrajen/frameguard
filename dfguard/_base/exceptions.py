"""Shared exceptions for all dfguard backends."""

from __future__ import annotations

from typing import Any


class DfTypesError(Exception):
    """Base class for all dfguard errors."""


class SchemaValidationError(DfTypesError):
    """
    Raised when a DataFrame's runtime schema does not match the declared schema.

    Attributes
    ----------
    errors:
        Human-readable list of individual mismatches.
    history:
        Full schema evolution history at the point of failure.
    """

    def __init__(self, errors: list[str], history: Any) -> None:
        self.errors  = errors
        self.history = history

        lines = ["Schema validation failed:"]
        for err in errors:
            lines.append(f"  \u2717 {err}")
        lines.append("")
        lines.append("Schema evolution (most recent last):")
        for change in history.changes:
            lines.append(f"  [{change.operation}]  {change.summary()}")
        super().__init__("\n".join(lines))


class TypeAnnotationError(DfTypesError):
    """Raised when a schema annotation cannot be converted to the backend's dtype."""


class ColumnNotFoundError(DfTypesError):
    """Raised when an expected column is absent from the DataFrame."""
