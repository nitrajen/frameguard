"""Immutable schema evolution log. Stores only StructType metadata, never DataFrame data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class SchemaChange:
    """One step in the schema evolution chain."""

    operation:        str              # e.g. "withColumn('revenue')"
    schema_after:     Any              # StructType after this operation
    added:            tuple[Any, ...] = field(default_factory=tuple)   # StructField list
    dropped:          tuple[str, ...] = field(default_factory=tuple)
    type_changed:     tuple[tuple[str, Any, Any], ...]  = field(default_factory=tuple)
    nullable_changed: tuple[tuple[str, bool, bool], ...] = field(default_factory=tuple)

    @classmethod
    def initial(cls, schema: Any) -> SchemaChange:
        return cls(operation="input", schema_after=schema)

    @classmethod
    def compute(cls, operation: str, before: Any, after: Any) -> SchemaChange:
        """Diff two StructTypes from Spark and record exactly what changed,
        including nested types."""
        before_fields: dict[str, Any] = {f.name: f for f in before.fields}
        after_fields:  dict[str, Any] = {f.name: f for f in after.fields}

        added   = tuple(f for name, f in after_fields.items() if name not in before_fields)
        dropped = tuple(name for name in before_fields if name not in after_fields)

        type_changed:     list[tuple[str, Any, Any]] = []
        nullable_changed: list[tuple[str, bool, bool]] = []

        for name, before_f in before_fields.items():
            if name not in after_fields:
                continue
            after_f = after_fields[name]
            if _types_differ(before_f.dataType, after_f.dataType):
                type_changed.append((name, before_f.dataType, after_f.dataType))
            if before_f.nullable != after_f.nullable:
                nullable_changed.append((name, before_f.nullable, after_f.nullable))

        return cls(
            operation        = operation,
            schema_after     = after,
            added            = added,
            dropped          = dropped,
            type_changed     = tuple(type_changed),
            nullable_changed = tuple(nullable_changed),
        )

    def has_changes(self) -> bool:
        return bool(self.added or self.dropped or self.type_changed or self.nullable_changed)

    def summary(self) -> str:
        if not self.has_changes():
            return f"{self.schema_after.simpleString()}  (no schema change)"
        parts: list[str] = []
        if self.added:
            parts.append("added: " + ", ".join(
                f"{f.name}:{f.dataType.simpleString()}" for f in self.added
            ))
        if self.dropped:
            parts.append("dropped: " + ", ".join(self.dropped))
        if self.type_changed:
            parts.append("type changed: " + ", ".join(
                f"{n} {b.simpleString()}→{a.simpleString()}"
                for n, b, a in self.type_changed
            ))
        if self.nullable_changed:
            parts.append("nullable changed: " + ", ".join(
                f"{n} {b}→{a}" for n, b, a in self.nullable_changed
            ))
        return " | ".join(parts)


@dataclass(frozen=True)
class SchemaHistory:
    """Immutable list of SchemaChanges. Each append returns a new instance."""

    changes: tuple[SchemaChange, ...]

    @classmethod
    def initial(cls, schema: Any) -> SchemaHistory:
        return cls(changes=(SchemaChange.initial(schema),))

    def append(self, change: SchemaChange) -> SchemaHistory:
        return SchemaHistory(changes=self.changes + (change,))

    @property
    def current_schema(self) -> Any:
        return self.changes[-1].schema_after

    def print(self) -> None:
        """Print the schema evolution to stdout."""
        print("─" * 60)
        print("Schema Evolution")
        print("─" * 60)
        for i, change in enumerate(self.changes):
            print(f"  [{i:2d}] {change.operation}")
            print(f"       {change.summary()}")
        print("─" * 60)

    def __len__(self) -> int:
        return len(self.changes)


def _types_differ(a: Any, b: Any) -> bool:
    """Recursively compare two Spark DataTypes for structural equality."""
    from pyspark.sql import types as T

    if type(a) is not type(b):
        return True

    if isinstance(a, T.StructType):
        a_fields = {f.name: f for f in a.fields}
        b_fields = {f.name: f for f in b.fields}
        if set(a_fields) != set(b_fields):
            return True
        return any(
            _types_differ(a_fields[n].dataType, b_fields[n].dataType)
            or a_fields[n].nullable != b_fields[n].nullable
            for n in a_fields
        )

    if isinstance(a, T.ArrayType):
        return a.containsNull != b.containsNull or _types_differ(a.elementType, b.elementType)

    if isinstance(a, T.MapType):
        return (
            a.valueContainsNull != b.valueContainsNull
            or _types_differ(a.keyType, b.keyType)
            or _types_differ(a.valueType, b.valueType)
        )

    return bool(a != b)
