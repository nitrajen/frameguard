"""schema_of, dataset, and _PolarsDatasetBase for the Polars backend."""

from __future__ import annotations

from typing import Any

from dfguard._base.history import DictSchemaChange, SchemaHistory
from dfguard.polars.exceptions import ColumnNotFoundError, SchemaValidationError
from dfguard.polars.types import dtypes_compatible

# ---------------------------------------------------------------------------
# schema_of — exact-match type from a live DataFrame
# ---------------------------------------------------------------------------

class _TypedDatasetMeta(type):
    """Metaclass for schema_of types: enforces exact schema (no extra columns)."""

    def __instancecheck__(cls, instance: Any) -> bool:
        import polars as pl
        expected: dict[str, Any] = cls.__dict__.get("_expected_schema", {})
        if not expected:
            return False
        if isinstance(instance, pl.LazyFrame):
            actual = dict(instance.collect_schema())
        elif isinstance(instance, pl.DataFrame):
            actual = dict(instance.schema)
        elif type.__instancecheck__(_PolarsDatasetBase, instance):
            actual = dict(instance.schema)
        else:
            return False
        if set(actual) != set(expected):
            return False
        return all(dtypes_compatible(expected[c], actual[c]) for c in expected)


def schema_of(df: Any) -> type:
    """
    Capture the exact schema of *df* as a type usable with ``@enforce``.

    Unlike ``PolarsSchema`` (subset matching), the resulting type requires
    the DataFrame to have **exactly** the captured columns — no extras.

    Works with both ``pl.DataFrame`` and ``pl.LazyFrame``.
    """
    import polars as pl
    if isinstance(df, pl.LazyFrame):
        captured = dict(df.collect_schema())
    elif isinstance(df, pl.DataFrame):
        captured = dict(df.schema)
    elif type.__instancecheck__(_PolarsDatasetBase, df):
        captured = dict(df.schema)
    else:
        raise TypeError(f"Expected a Polars DataFrame or LazyFrame, got {type(df).__name__}")

    return _TypedDatasetMeta(
        "CapturedSchema",
        (object,),
        {"_expected_schema": captured, "_fg_check": classmethod(_exact_fg_check)},
    )


def _exact_fg_check(cls: type, value: Any, subset: bool) -> bool:  # noqa: ARG001
    return isinstance(value, cls)


# ---------------------------------------------------------------------------
# _PolarsDatasetBase — schema-tracking DataFrame wrapper
# ---------------------------------------------------------------------------

class _PolarsDatasetBase:
    """
    Immutable wrapper around a ``polars.DataFrame`` that tracks schema evolution.

    Every mutating operation returns a new ``_PolarsDatasetBase`` with an updated
    history, leaving the original unchanged.
    """

    __slots__ = ("_df", "_history")

    def __init__(self, df: Any, history: SchemaHistory) -> None:
        object.__setattr__(self, "_df", df)
        object.__setattr__(self, "_history", history)

    # ── Schema / metadata ────────────────────────────────────────────────────

    @property
    def schema(self) -> Any:
        return self._df.schema

    @property
    def columns(self) -> list[str]:
        return self._df.columns

    @property
    def dtypes(self) -> Any:
        return self._df.dtypes

    @property
    def schema_history(self) -> SchemaHistory:
        return self._history

    def __len__(self) -> int:
        return len(self._df)

    # ── Mutation helpers ─────────────────────────────────────────────────────

    def _evolve(self, new_df: Any, operation: str) -> _PolarsDatasetBase:
        change = DictSchemaChange.compute(
            operation, dict(self._df.schema), dict(new_df.schema)
        )
        return _PolarsDatasetBase(new_df, self._history.append(change))

    # ── Transformations ───────────────────────────────────────────────────────

    def with_columns(self, *exprs: Any, **named_exprs: Any) -> _PolarsDatasetBase:
        """Add or replace columns (Polars equivalent of pandas ``assign``)."""
        return self._evolve(self._df.with_columns(*exprs, **named_exprs), "with_columns")

    def rename(self, mapping: dict[str, str]) -> _PolarsDatasetBase:
        return self._evolve(self._df.rename(mapping), "rename")

    def drop(self, *cols: str) -> _PolarsDatasetBase:
        return self._evolve(self._df.drop(list(cols)), "drop")

    def select(self, *cols: Any) -> _PolarsDatasetBase:
        return self._evolve(self._df.select(list(cols)), "select")

    def filter(self, expr: Any) -> _PolarsDatasetBase:
        return self._evolve(self._df.filter(expr), "filter")

    def sort(self, by: Any, *, descending: bool = False) -> _PolarsDatasetBase:
        return self._evolve(self._df.sort(by, descending=descending), "sort")

    def unique(self, subset: Any = None) -> _PolarsDatasetBase:
        return self._evolve(self._df.unique(subset=subset), "unique")

    def join(
        self,
        other: Any,
        on: Any = None,
        *,
        how: str = "inner",
        left_on: Any = None,
        right_on: Any = None,
    ) -> _PolarsDatasetBase:
        other_df = other._df if type.__instancecheck__(_PolarsDatasetBase, other) else other
        new_df = self._df.join(other_df, on=on, how=how, left_on=left_on, right_on=right_on)
        return self._evolve(new_df, "join")

    def group_by(self, *by: Any) -> _PolarsGroupBy:
        return _PolarsGroupBy(self, list(by))

    # ── Validation ────────────────────────────────────────────────────────────

    def validate(self, schema_cls: type) -> _PolarsDatasetBase:
        schema_cls.assert_valid(self)
        return self

    def assert_columns(self, *cols: str) -> _PolarsDatasetBase:
        missing = [c for c in cols if c not in self._df.columns]
        if missing:
            raise SchemaValidationError(
                [f"Missing column '{c}'" for c in missing],
                self._history,
            )
        return self

    def assert_column_type(self, col: str, dtype: Any) -> _PolarsDatasetBase:
        if col not in self._df.columns:
            raise ColumnNotFoundError(f"Column '{col}' not found")
        actual = self._df.schema[col]
        if not dtypes_compatible(dtype, actual):
            raise SchemaValidationError(
                [f"Column '{col}': expected {dtype}, got {actual}"],
                self._history,
            )
        return self

    # ── Pandas interop ────────────────────────────────────────────────────────

    def to_pandas(self) -> Any:
        return self._df.to_pandas()


class _PolarsGroupBy:
    """Thin wrapper so ``group_by(...).agg(...)`` returns ``_PolarsDatasetBase``."""

    __slots__ = ("_ds", "_by")

    def __init__(self, ds: _PolarsDatasetBase, by: list[Any]) -> None:
        self._ds = ds
        self._by = by

    def agg(self, *exprs: Any) -> _PolarsDatasetBase:
        new_df = self._ds._df.group_by(self._by).agg(list(exprs))
        return self._ds._evolve(new_df, f"group_by({self._by}).agg")


def _make_dataset(df: Any) -> _PolarsDatasetBase:
    import polars as pl
    if isinstance(df, _PolarsDatasetBase):
        return df
    if isinstance(df, pl.DataFrame):
        history = SchemaHistory.initial(dict(df.schema))
        return _PolarsDatasetBase(df, history)
    raise TypeError(f"Expected pl.DataFrame, got {type(df).__name__}")


dataset = _make_dataset
