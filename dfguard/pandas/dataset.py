"""
dfguard.pandas.dataset: schema-as-type for pandas DataFrames.

``schema_of(df)`` captures a DataFrame's schema as a Python class.
Use that class as a type annotation and dfguard enforces it at every call.

    from dfguard.pandas import schema_of, arm

    RawSchema     = schema_of(raw_df)
    EnrichedSchema = schema_of(enriched_df)

    def enrich(df: RawSchema) -> EnrichedSchema:   # wrong schema → error at call site
        return df.assign(revenue=df["amount"] * df["quantity"])

    arm()

For mutation tracking use ``dataset(df)``::

    from dfguard.pandas import dataset

    ds = dataset(raw_df)
    ds = ds.assign(revenue=...)
    print(ds.schema_history)

The dataset wrapper mirrors the pandas API for the most common operations and
records every schema-changing step in an immutable history log.
"""

from __future__ import annotations

from typing import Any, cast

from dfguard.pandas.exceptions import SchemaValidationError
from dfguard.pandas.history import PandasSchemaChange, PandasSchemaHistory

# ---------------------------------------------------------------------------
# Metaclass: structural isinstance checks based on exact dtype match
# ---------------------------------------------------------------------------

class _TypedDatasetMeta(type):
    """
    Metaclass for all schema type classes returned by ``schema_of()``.

    ``isinstance(ds, DT)`` is a structural check: True when ``ds`` has
    exactly the columns (names + types) that ``DT`` was created with.
    Extra columns make it False — capture a new type at each stage boundary.
    """

    def __instancecheck__(cls, instance: Any) -> bool:
        import pandas as pd

        is_df       = isinstance(instance, pd.DataFrame)
        is_ds       = type.__instancecheck__(_PandasDatasetBase, instance)
        if not (is_df or is_ds):
            return False

        expected: dict[str, Any] | None = cls.__dict__.get("_expected_schema")
        if expected is None:
            return True

        actual = dict(instance.dtypes)
        if set(actual) != set(expected):
            return False

        from dfguard.pandas.types import dtypes_compatible
        return all(dtypes_compatible(expected[col], actual[col]) for col in expected)


# ---------------------------------------------------------------------------
# Base implementation
# ---------------------------------------------------------------------------

class _PandasDatasetBase(metaclass=_TypedDatasetMeta):
    """
    Internal base shared by all schema type subclasses.

    Users never instantiate this directly. Call ``dataset(df)`` to get a tracked
    instance, or ``schema_of(df)`` to get the schema type class.
    """

    _expected_schema: dict[str, Any] | None = None
    _source_df:       Any                   = None

    @classmethod
    def _fg_check(cls, value: Any, subset: bool) -> bool:
        """Enforcement protocol hook: schema_of types are always exact."""
        return isinstance(value, cls)

    _df:      Any
    _history: PandasSchemaHistory
    __slots__ = ("_df", "_history")

    def __init__(
        self,
        df: Any = None,
        *,
        history: PandasSchemaHistory | None = None,
        _skip_validation: bool = False,
    ) -> None:
        import pandas as pd

        if df is None:
            raw = type(self)._source_df
        elif type.__instancecheck__(_PandasDatasetBase, df):
            raw = df._df
        elif isinstance(df, pd.DataFrame):
            raw = df
        else:
            raise TypeError(f"Expected a pandas DataFrame, got {type(df).__name__}")

        if not _skip_validation:
            expected = type(self)._expected_schema
            if expected is not None:
                from dfguard.pandas.schema import _compare_schemas
                errors = _compare_schemas(expected, dict(raw.dtypes), strict=False)
                if errors:
                    raise SchemaValidationError(
                        [str(e) for e in errors],
                        history or PandasSchemaHistory.initial(dict(raw.dtypes)),
                    )

        object.__setattr__(self, "_df",      raw)
        initial = history or PandasSchemaHistory.initial(dict(raw.dtypes))
        object.__setattr__(self, "_history", initial)

    # ------------------------------------------------------------------
    # Internal factory: every operation calls this
    # ------------------------------------------------------------------

    def _wrap(self, new_df: Any, operation: str) -> _PandasDatasetBase:
        change = PandasSchemaChange.compute(
            operation = operation,
            before    = dict(self._df.dtypes),
            after     = dict(new_df.dtypes),
        )
        new_cls = cast(type[_PandasDatasetBase], _TypedDatasetMeta(
            "SchemaType",
            (_PandasDatasetBase,),
            {"_expected_schema": dict(new_df.dtypes), "_source_df": new_df},
        ))
        instance: _PandasDatasetBase = object.__new__(new_cls)
        object.__setattr__(instance, "_df",      new_df)
        object.__setattr__(instance, "_history", self._history.append(change))
        return instance

    # ------------------------------------------------------------------
    # Schema properties
    # ------------------------------------------------------------------

    @property
    def dtypes(self) -> Any:
        return self._df.dtypes

    @property
    def columns(self) -> Any:
        return self._df.columns

    @property
    def shape(self) -> tuple[int, int]:
        return self._df.shape  # type: ignore[no-any-return]

    @property
    def schema_history(self) -> PandasSchemaHistory:
        return self._history

    # ------------------------------------------------------------------
    # Column-mutating transformations
    # ------------------------------------------------------------------

    def assign(self, **kwargs: Any) -> _PandasDatasetBase:
        """Add or replace columns. Pandas equivalent of Spark's ``withColumn``."""
        return self._wrap(self._df.assign(**kwargs), f"assign({list(kwargs)})")

    def rename(self, columns: dict[str, str], **kwargs: Any) -> _PandasDatasetBase:
        """Rename columns. Pandas equivalent of Spark's ``withColumnRenamed``."""
        return self._wrap(
            self._df.rename(columns=columns, **kwargs),
            f"rename({columns})",
        )

    def drop(self, columns: list[str] | str, **kwargs: Any) -> _PandasDatasetBase:
        """Drop columns by name."""
        cols = [columns] if isinstance(columns, str) else list(columns)
        return self._wrap(self._df.drop(columns=cols, **kwargs), f"drop({cols})")

    def select(self, *cols: str) -> _PandasDatasetBase:
        """Select a subset of columns (convenience alias for df[list])."""
        return self._wrap(self._df[list(cols)], f"select({list(cols)})")

    def astype(self, dtype: Any, **kwargs: Any) -> _PandasDatasetBase:
        """Cast column dtypes."""
        return self._wrap(self._df.astype(dtype, **kwargs), f"astype({dtype!r})")

    # ------------------------------------------------------------------
    # Row-level operations (schema-preserving)
    # ------------------------------------------------------------------

    def filter(self, mask: Any) -> _PandasDatasetBase:
        """Boolean-mask row filter. Preserves schema."""
        return self._wrap(self._df[mask], "filter(...)")

    def query(self, expr: str, **kwargs: Any) -> _PandasDatasetBase:
        """String-expression row filter."""
        return self._wrap(self._df.query(expr, **kwargs), f"query({expr!r})")

    def head(self, n: int = 5) -> _PandasDatasetBase:
        return self._wrap(self._df.head(n), f"head({n})")

    def tail(self, n: int = 5) -> _PandasDatasetBase:
        return self._wrap(self._df.tail(n), f"tail({n})")

    def sample(self, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.sample(**kwargs), "sample(...)")

    def drop_duplicates(self, subset: list[str] | None = None, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(
            self._df.drop_duplicates(subset=subset, **kwargs),
            f"drop_duplicates(subset={subset})",
        )

    def sort_values(self, by: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.sort_values(by, **kwargs), f"sort_values({by!r})")

    def reset_index(self, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.reset_index(**kwargs), "reset_index(...)")

    # ------------------------------------------------------------------
    # Joins / concat
    # ------------------------------------------------------------------

    def merge(self, right: Any, **kwargs: Any) -> _PandasDatasetBase:
        """Merge (join) with another DataFrame or dataset wrapper."""
        right_df = right._df if type.__instancecheck__(_PandasDatasetBase, right) else right
        how = kwargs.get("how", "inner")
        return self._wrap(self._df.merge(right_df, **kwargs), f"merge(..., how='{how}')")

    def join(self, other: Any, **kwargs: Any) -> _PandasDatasetBase:
        other_df = other._df if type.__instancecheck__(_PandasDatasetBase, other) else other
        how = kwargs.get("how", "left")
        return self._wrap(self._df.join(other_df, **kwargs), f"join(..., how='{how}')")

    # ------------------------------------------------------------------
    # GroupBy
    # ------------------------------------------------------------------

    def groupby(self, by: Any, **kwargs: Any) -> TypedGroupBy:
        return TypedGroupBy(self._df.groupby(by, **kwargs), parent=self)

    # ------------------------------------------------------------------
    # Reshape
    # ------------------------------------------------------------------

    def melt(self, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.melt(**kwargs), "melt(...)")

    def pivot_table(self, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.pivot_table(**kwargs), "pivot_table(...)")

    def explode(self, column: str | list[str], **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap(self._df.explode(column, **kwargs), f"explode({column!r})")

    # ------------------------------------------------------------------
    # Validation helpers (chainable)
    # ------------------------------------------------------------------

    def validate(self, schema: Any, strict: bool = False) -> _PandasDatasetBase:
        """Validate this dataset against a ``PandasSchema`` class or dtype dict."""
        from dfguard.pandas.schema import _compare_schemas

        if isinstance(schema, dict):
            annotations = schema
        else:
            annotations = schema._schema_fields  # PandasSchema subclass

        errors = _compare_schemas(annotations, dict(self._df.dtypes), strict=strict)
        if errors:
            raise SchemaValidationError([str(e) for e in errors], self._history)
        return self

    def assert_columns(self, *col_names: str) -> _PandasDatasetBase:
        missing = [c for c in col_names if c not in self._df.columns]
        if missing:
            available = ", ".join(str(c) for c in self._df.columns)
            raise SchemaValidationError(
                [f"Missing column '{c}'" for c in missing]
                + [f"Available columns: {available}"],
                self._history,
            )
        return self

    def assert_column_type(self, col_name: str, expected_annotation: Any) -> _PandasDatasetBase:
        from dfguard.pandas.types import annotation_to_pandas_dtype, dtypes_compatible
        if col_name not in self._df.columns:
            raise SchemaValidationError([f"Column '{col_name}' not found"], self._history)
        expected_dtype, _ = annotation_to_pandas_dtype(expected_annotation)
        actual_dtype = self._df[col_name].dtype
        if not dtypes_compatible(expected_dtype, actual_dtype):
            raise SchemaValidationError(
                [f"Column '{col_name}': expected {expected_dtype}, got {actual_dtype}"],
                self._history,
            )
        return self

    # ------------------------------------------------------------------
    # Delegation
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        return getattr(self._df, name)

    def __repr__(self) -> str:
        fields = ", ".join(f"{c}:{d}" for c, d in self._df.dtypes.items())
        return f"{type(self).__name__}[{fields}]"

    def __str__(self) -> str:
        return self.__repr__()

    def __len__(self) -> int:
        return len(self._df)


# ---------------------------------------------------------------------------
# TypedGroupBy
# ---------------------------------------------------------------------------

class TypedGroupBy:
    _gd:     Any
    _parent: _PandasDatasetBase
    __slots__ = ("_gd", "_parent")

    def __init__(self, grouped: Any, parent: _PandasDatasetBase) -> None:
        object.__setattr__(self, "_gd",     grouped)
        object.__setattr__(self, "_parent", parent)

    def _wrap_result(self, result_df: Any, operation: str) -> _PandasDatasetBase:
        return self._parent._wrap(result_df, operation)

    def agg(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.agg(*args, **kwargs), "groupby(...).agg(...)")

    def sum(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.sum(*args, **kwargs), "groupby(...).sum()")

    def mean(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.mean(*args, **kwargs), "groupby(...).mean()")

    def count(self) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.count(), "groupby(...).count()")

    def min(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.min(*args, **kwargs), "groupby(...).min()")

    def max(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.max(*args, **kwargs), "groupby(...).max()")

    def first(self, *args: Any, **kwargs: Any) -> _PandasDatasetBase:
        return self._wrap_result(self._gd.first(*args, **kwargs), "groupby(...).first()")

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._gd, name)
        if callable(attr):
            def _delegated(*args: Any, **kwargs: Any) -> Any:
                import pandas as pd
                result = attr(*args, **kwargs)
                if isinstance(result, pd.DataFrame):
                    return self._wrap_result(result, f"groupby(...).{name}(...)")
                return result
            return _delegated
        return attr


# ---------------------------------------------------------------------------
# Internal instance factory
# ---------------------------------------------------------------------------

def _make_dataset(df: Any, *, history: PandasSchemaHistory | None = None) -> _PandasDatasetBase:
    """Low-level factory used internally and by the public ``dataset()`` alias."""
    import pandas as pd

    raw = df._df if type.__instancecheck__(_PandasDatasetBase, df) else df
    if not isinstance(raw, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame, got {type(df).__name__}")

    schema = dict(raw.dtypes)
    cls = cast(type[_PandasDatasetBase], _TypedDatasetMeta(
        "SchemaType",
        (_PandasDatasetBase,),
        {"_expected_schema": schema, "_source_df": raw},
    ))
    instance: _PandasDatasetBase = object.__new__(cls)
    object.__setattr__(instance, "_df",      raw)
    object.__setattr__(instance, "_history", history or PandasSchemaHistory.initial(schema))
    return instance


# ---------------------------------------------------------------------------
# Public factories
# ---------------------------------------------------------------------------

def schema_of(df: Any) -> type[_PandasDatasetBase]:
    """
    Capture a DataFrame's schema as a Python type class.

    Returns a class whose ``isinstance`` check does **exact** schema matching:
    same column names, same dtypes, nothing extra.  Assign in PascalCase and
    use as a type annotation::

        RawSchema = schema_of(raw_df)

        @enforce
        def enrich(df: RawSchema): ...   # wrong schema → raises at call site

    Capture a new type at each stage that changes the schema::

        EnrichedSchema = schema_of(enriched_df)
    """
    import pandas as pd

    if type.__instancecheck__(_PandasDatasetBase, df):
        raw = df._df
    elif isinstance(df, pd.DataFrame):
        raw = df
    else:
        raise TypeError(f"Expected a pandas DataFrame, got {type(df).__name__}")

    return cast(type[_PandasDatasetBase], _TypedDatasetMeta(
        "SchemaType",
        (_PandasDatasetBase,),
        {"_expected_schema": dict(raw.dtypes), "_source_df": raw},
    ))
