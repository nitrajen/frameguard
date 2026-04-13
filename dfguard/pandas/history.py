"""Pandas schema history — thin aliases over the shared dict-based history."""

from dfguard._base.history import DictSchemaChange as PandasSchemaChange
from dfguard._base.history import SchemaHistory as PandasSchemaHistory

__all__ = ["PandasSchemaChange", "PandasSchemaHistory"]
