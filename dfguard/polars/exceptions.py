"""dfguard.polars.exceptions — re-exported from dfguard._base.exceptions."""

from dfguard._base.exceptions import (
    ColumnNotFoundError,
    DfTypesError,
    SchemaValidationError,
    TypeAnnotationError,
)

__all__ = [
    "DfTypesError",
    "SchemaValidationError",
    "TypeAnnotationError",
    "ColumnNotFoundError",
]
