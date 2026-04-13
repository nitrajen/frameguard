"""Generic immutable schema history for dict-based backends (pandas, polars).

PySpark keeps its own StructType-aware history; pandas and polars share this one.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class DictSchemaChange:
    """One step in a schema evolution chain; schema is a ``{col: dtype}`` dict."""

    operation:    str
    schema_after: dict[str, Any]
    added:        tuple[str, ...]                  = field(default_factory=tuple)
    dropped:      tuple[str, ...]                  = field(default_factory=tuple)
    type_changed: tuple[tuple[str, Any, Any], ...] = field(default_factory=tuple)

    @classmethod
    def initial(cls, schema: dict[str, Any]) -> DictSchemaChange:
        return cls(operation="input", schema_after=schema)

    @classmethod
    def compute(
        cls,
        operation: str,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> DictSchemaChange:
        added   = tuple(c for c in after  if c not in before)
        dropped = tuple(c for c in before if c not in after)
        type_changed = tuple(
            (c, before[c], after[c])
            for c in before
            if c in after and not _dtypes_equal(before[c], after[c])
        )
        return cls(
            operation    = operation,
            schema_after = after,
            added        = added,
            dropped      = dropped,
            type_changed = type_changed,
        )

    def has_changes(self) -> bool:
        return bool(self.added or self.dropped or self.type_changed)

    def summary(self) -> str:
        schema_str = ", ".join(f"{c}:{d}" for c, d in self.schema_after.items())
        if not self.has_changes():
            return f"{{{schema_str}}}  (no schema change)"
        parts: list[str] = []
        if self.added:
            parts.append("added: " + ", ".join(self.added))
        if self.dropped:
            parts.append("dropped: " + ", ".join(self.dropped))
        if self.type_changed:
            parts.append("type changed: " + ", ".join(
                f"{n} {b}\u2192{a}" for n, b, a in self.type_changed
            ))
        return " | ".join(parts)


@dataclass(frozen=True)
class SchemaHistory:
    """Immutable list of schema changes. Each append returns a new instance."""

    changes: tuple[DictSchemaChange, ...]

    @classmethod
    def initial(cls, schema: dict[str, Any]) -> SchemaHistory:
        return cls(changes=(DictSchemaChange.initial(schema),))

    def append(self, change: DictSchemaChange) -> SchemaHistory:
        return SchemaHistory(changes=self.changes + (change,))

    @property
    def current_schema(self) -> dict[str, Any]:
        return self.changes[-1].schema_after

    def print(self) -> None:
        print("\u2500" * 60)
        print("Schema Evolution")
        print("\u2500" * 60)
        for i, change in enumerate(self.changes):
            print(f"  [{i:2d}] {change.operation}")
            print(f"       {change.summary()}")
        print("\u2500" * 60)

    def __len__(self) -> int:
        return len(self.changes)


def _dtypes_equal(a: Any, b: Any) -> bool:
    try:
        return bool(a == b)
    except Exception:
        return type(a) is type(b)
