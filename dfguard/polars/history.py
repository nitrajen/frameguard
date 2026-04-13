"""Polars schema history — thin aliases over the shared dict-based history."""

from dfguard._base.history import DictSchemaChange as PolarsSchemaChange
from dfguard._base.history import SchemaHistory as PolarsSchemaHistory

__all__ = ["PolarsSchemaChange", "PolarsSchemaHistory"]
