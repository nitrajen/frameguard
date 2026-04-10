"""
frameguard.pyspark.dataset — schema-as-type for PySpark DataFrames.

``schema_of(df)`` captures a DataFrame's schema as a Python class.
Use that class as a type annotation and beartype enforces it at every call.

    from frameguard.pyspark import schema_of, enable
    enable()

    RawSchema  = schema_of(raw_df)
    RichSchema = schema_of(enriched_df)

    def enrich(df: RawSchema) -> RichSchema:   # wrong schema → error at call site
        return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

The returned class is a real Python class — isinstance(df, RawSchema) works,
and it integrates naturally with beartype, mypy stubs, and your own tooling.

For mutation tracking use ``dataset(df)`` (aliased as ``_make_dataset``):

    from frameguard.pyspark import dataset
    ds = dataset(raw_df)
    ds = ds.withColumn("revenue", ...)
    print(ds.schema_history)
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from frameguard.pyspark.exceptions import SchemaValidationError
from frameguard.pyspark.history import SchemaChange, SchemaHistory

# ---------------------------------------------------------------------------
# Metaclass — enables structural isinstance checks based on schema
# ---------------------------------------------------------------------------

class _TypedDatasetMeta(type):
    """
    Metaclass for all schema type classes returned by ``schema_of()``.

    Makes ``isinstance(ds, DT)`` a structural check: True when ``ds`` has at
    least all the columns (names + types) that ``DT`` was created with.
    """

    def __instancecheck__(cls, instance: Any) -> bool:
        from pyspark.sql import DataFrame as SparkDF

        # Accept both plain SparkDFs and typed dataset instances
        is_sparkdf   = isinstance(instance, SparkDF)
        is_ds_instance = type.__instancecheck__(_TypedDatasetBase, instance)
        if not (is_sparkdf or is_ds_instance):
            return False

        expected = cls.__dict__.get("_expected_schema")
        if expected is None:
            return True

        # Exact match: same column names AND same types, no extras allowed.
        # A DataFrame with extra columns does NOT satisfy the contract —
        # use DT2 = schema_of(enriched_df) to capture the new schema.
        actual   = {f.name: f.dataType for f in instance.schema.fields}
        expected_ = {f.name: f.dataType for f in expected.fields}
        return actual == expected_


# ---------------------------------------------------------------------------
# Base implementation — all methods live here
# ---------------------------------------------------------------------------

class _TypedDatasetBase(metaclass=_TypedDatasetMeta):
    """
    Internal base shared by all schema type subclasses.

    Users never instantiate this directly. They call ``schema_of(df)``
    which returns a subclass whose constructor validates the incoming schema.
    """

    _expected_schema: Any = None   # set on each subclass by schema_of()
    _source_df:       Any = None   # the original df stored on the class

    @classmethod
    def _fg_check(cls, value: Any, subset: bool) -> bool:
        """Enforcement protocol hook — schema_of types are always exact."""
        return isinstance(value, cls)  # exact: extra columns fail

    _df:      Any
    _history: SchemaHistory
    _strict:  bool
    __slots__ = ("_df", "_history", "_strict")

    def __init__(
        self,
        df: Any = None,
        *,
        history: SchemaHistory | None = None,
        strict: bool = False,
        _skip_validation: bool = False,
    ) -> None:
        from pyspark.sql import DataFrame as SparkDF

        # Resolve the actual Spark DataFrame
        if df is None:
            raw = type(self)._source_df
        elif type.__instancecheck__(_TypedDatasetBase, df):
            raw = df._df
        elif isinstance(df, SparkDF):
            raw = df
        else:
            raise TypeError(f"Expected a DataFrame, got {type(df).__name__}")

        # Validate schema unless called from internal _wrap
        if not _skip_validation:
            expected = type(self)._expected_schema
            if expected is not None:
                from frameguard.pyspark.schema import _compare_structs
                errors = _compare_structs(expected, raw.schema, strict=False, path="")
                if errors:
                    raise SchemaValidationError(
                        [str(e) for e in errors],
                        history or SchemaHistory.initial(raw.schema),
                    )

        object.__setattr__(self, "_df",      raw)
        object.__setattr__(self, "_strict",  strict)
        object.__setattr__(self, "_history", history or SchemaHistory.initial(raw.schema))

    # ------------------------------------------------------------------
    # Internal factory — every operation calls this
    # ------------------------------------------------------------------

    def _wrap(self, new_df: Any, operation: str) -> _TypedDatasetBase:
        change = SchemaChange.compute(
            operation = operation,
            before    = self._df.schema,
            after     = new_df.schema,
        )
        # Each mutation produces a new class whose _expected_schema reflects the
        # evolved schema.  This means the returned instance, when used as a type
        # annotation or passed to DT2(other_df), validates against the new schema.
        new_cls = cast(type[_TypedDatasetBase], _TypedDatasetMeta(
            "SchemaType",
            (_TypedDatasetBase,),
            {"_expected_schema": new_df.schema, "_source_df": new_df},
        ))
        instance: _TypedDatasetBase = object.__new__(new_cls)
        object.__setattr__(instance, "_df",      new_df)
        object.__setattr__(instance, "_strict",  self._strict)
        object.__setattr__(instance, "_history", self._history.append(change))
        return instance

    def _safe(self, fn: Callable[[], _TypedDatasetBase], operation: str) -> _TypedDatasetBase:
        """Call a Spark operation and wrap AnalysisException with schema history."""
        try:
            return fn()
        except Exception as exc:
            cls_name = type(exc).__name__
            if "AnalysisException" in cls_name:
                raise SchemaValidationError(
                    [str(exc)],
                    self._history,
                ) from exc
            raise

    # ------------------------------------------------------------------
    # Schema properties
    # ------------------------------------------------------------------

    @property
    def schema(self) -> Any:
        return self._df.schema

    @property
    def dtypes(self) -> list[tuple[str, str]]:
        return self._df.dtypes  # type: ignore[no-any-return]

    @property
    def columns(self) -> list[str]:
        return self._df.columns  # type: ignore[no-any-return]

    @property
    def schema_history(self) -> SchemaHistory:
        return self._history

    # ------------------------------------------------------------------
    # Column-mutating transformations
    # ------------------------------------------------------------------

    def withColumn(self, col_name: str, col: Any) -> _TypedDatasetBase:
        return self._safe(
            lambda: self._wrap(self._df.withColumn(col_name, col), f"withColumn('{col_name}')"),
            f"withColumn('{col_name}')",
        )

    def withColumns(self, col_map: dict[str, Any]) -> _TypedDatasetBase:
        return self._safe(
            lambda: self._wrap(self._df.withColumns(col_map), f"withColumns({list(col_map)})"),
            "withColumns",
        )

    def withColumnRenamed(self, existing: str, new: str) -> _TypedDatasetBase:
        return self._wrap(
            self._df.withColumnRenamed(existing, new),
            f"withColumnRenamed('{existing}'→'{new}')",
        )

    def withColumnsRenamed(self, col_map: dict[str, str]) -> _TypedDatasetBase:
        return self._wrap(self._df.withColumnsRenamed(col_map), f"withColumnsRenamed({col_map})")

    def withMetadata(self, column_name: str, metadata: dict[str, Any]) -> _TypedDatasetBase:
        return self._wrap(
            self._df.withMetadata(column_name, metadata), f"withMetadata('{column_name}')"
        )

    def drop(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap(self._df.drop(*cols), f"drop({list(cols)})")

    def select(self, *cols: Any) -> _TypedDatasetBase:
        return self._safe(
            lambda: self._wrap(self._df.select(*cols), "select(...)"),
            "select",
        )

    def selectExpr(self, *exprs: str) -> _TypedDatasetBase:
        return self._safe(
            lambda: self._wrap(self._df.selectExpr(*exprs), f"selectExpr({list(exprs)})"),
            "selectExpr",
        )

    def toDF(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap(self._df.toDF(*cols), f"toDF({list(cols)})")

    # ------------------------------------------------------------------
    # Row-level (no schema change)
    # ------------------------------------------------------------------

    def filter(self, condition: Any) -> _TypedDatasetBase:
        return self._wrap(self._df.filter(condition), "filter(...)")

    def where(self, condition: Any) -> _TypedDatasetBase:
        return self.filter(condition)

    def limit(self, num: int) -> _TypedDatasetBase:
        return self._wrap(self._df.limit(num), f"limit({num})")

    def sample(
        self,
        with_replacement: bool | None = None,
        fraction: float | None = None,
        seed: int | None = None,
    ) -> _TypedDatasetBase:
        return self._wrap(self._df.sample(with_replacement, fraction, seed), "sample(...)")

    def distinct(self) -> _TypedDatasetBase:
        return self._wrap(self._df.distinct(), "distinct()")

    def dropDuplicates(self, subset: list[str] | None = None) -> _TypedDatasetBase:
        return self._wrap(self._df.dropDuplicates(subset), f"dropDuplicates({subset})")

    drop_duplicates = dropDuplicates

    def orderBy(self, *cols: Any, **kwargs: Any) -> _TypedDatasetBase:
        return self._wrap(self._df.orderBy(*cols, **kwargs), "orderBy(...)")

    sort = orderBy

    def repartition(self, num_partitions: Any, *cols: Any) -> _TypedDatasetBase:
        return self._wrap(
            self._df.repartition(num_partitions, *cols), f"repartition({num_partitions})"
        )

    def repartitionByRange(self, num_partitions: Any, *cols: Any) -> _TypedDatasetBase:
        return self._wrap(
            self._df.repartitionByRange(num_partitions, *cols),
            f"repartitionByRange({num_partitions})",
        )

    def coalesce(self, num_partitions: int) -> _TypedDatasetBase:
        return self._wrap(self._df.coalesce(num_partitions), f"coalesce({num_partitions})")

    # ------------------------------------------------------------------
    # Set operations
    # ------------------------------------------------------------------

    def union(self, other: Any) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.union(other_df), "union(...)")

    unionAll = union

    def unionByName(self, other: Any, allow_missing_columns: bool = False) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(
            self._df.unionByName(other_df, allowMissingColumns=allow_missing_columns),
            "unionByName(...)",
        )

    def intersect(self, other: Any) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.intersect(other_df), "intersect(...)")

    def intersectAll(self, other: Any) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.intersectAll(other_df), "intersectAll(...)")

    def subtract(self, other: Any) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.subtract(other_df), "subtract(...)")

    exceptAll = subtract

    # ------------------------------------------------------------------
    # Joins
    # ------------------------------------------------------------------

    def join(self, other: Any, on: Any = None, how: str = "inner") -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.join(other_df, on=on, how=how), f"join(..., how='{how}')")

    def crossJoin(self, other: Any) -> _TypedDatasetBase:
        other_df = other._df if type.__instancecheck__(_TypedDatasetBase, other) else other
        return self._wrap(self._df.crossJoin(other_df), "crossJoin(...)")

    # ------------------------------------------------------------------
    # GroupBy
    # ------------------------------------------------------------------

    def groupBy(self, *cols: Any) -> TypedGroupedData:
        return TypedGroupedData(self._df.groupBy(*cols), parent=self)

    groupby = groupBy

    def rollup(self, *cols: Any) -> TypedGroupedData:
        return TypedGroupedData(self._df.rollup(*cols), parent=self)

    def cube(self, *cols: Any) -> TypedGroupedData:
        return TypedGroupedData(self._df.cube(*cols), parent=self)

    # ------------------------------------------------------------------
    # na / stat
    # ------------------------------------------------------------------

    @property
    def na(self) -> TypedNaFunctions:
        return TypedNaFunctions(self._df.na, parent=self)

    @property
    def stat(self) -> TypedStatFunctions:
        return TypedStatFunctions(self._df.stat, parent=self)

    # ------------------------------------------------------------------
    # transform
    # ------------------------------------------------------------------

    def transform(
        self, func: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> _TypedDatasetBase:
        result = func(self, *args, **kwargs)
        if type.__instancecheck__(_TypedDatasetBase, result):
            return cast(_TypedDatasetBase, result)
        return self._wrap(result, f"transform({getattr(func, '__name__', '?')})")

    # ------------------------------------------------------------------
    # Reshape (Spark 3.4+)
    # ------------------------------------------------------------------

    def unpivot(
        self,
        ids: list[str],
        values: list[str] | None,
        variable_column_name: str,
        value_column_name: str,
    ) -> _TypedDatasetBase:
        return self._wrap(
            self._df.unpivot(ids, values, variable_column_name, value_column_name),
            "unpivot(...)",
        )

    melt = unpivot

    # ------------------------------------------------------------------
    # Validation helpers
    # ------------------------------------------------------------------

    def validate(self, schema: Any, strict: bool | None = None) -> _TypedDatasetBase:
        from pyspark.sql.types import StructType

        from frameguard.pyspark.schema import _compare_structs

        s = strict if strict is not None else self._strict

        if isinstance(schema, dict):
            expected_struct = _struct_from_dict(schema)
        elif isinstance(schema, StructType):
            expected_struct = schema
        else:
            expected_struct = schema.to_struct()

        errors = _compare_structs(expected_struct, self._df.schema, strict=s, path="")
        if errors:
            raise SchemaValidationError([str(e) for e in errors], self._history)
        return self

    def assert_columns(self, *col_names: str) -> _TypedDatasetBase:
        missing = [c for c in col_names if c not in self.columns]
        if missing:
            available = ", ".join(self.columns)
            raise SchemaValidationError(
                [f"Missing column '{c}'" for c in missing]
                + [f"Available columns: {available}"],
                self._history,
            )
        return self

    def assert_column_type(self, col_name: str, expected_type: Any) -> _TypedDatasetBase:
        from frameguard.pyspark.types import annotation_to_spark
        field = next((f for f in self._df.schema.fields if f.name == col_name), None)
        if field is None:
            raise SchemaValidationError([f"Column '{col_name}' not found"], self._history)
        expected_spark_type, _ = annotation_to_spark(expected_type)
        if field.dataType != expected_spark_type:
            raise SchemaValidationError(
                [
                    f"Column '{col_name}': expected "
                    f"{expected_spark_type.simpleString()}, "
                    f"got {field.dataType.simpleString()}"
                ],
                self._history,
            )
        return self

    def assert_not_null(self, *col_names: str) -> _TypedDatasetBase:
        errors = []
        for name in col_names:
            field = next((f for f in self._df.schema.fields if f.name == name), None)
            if field is None:
                errors.append(f"Column '{name}' not found")
            elif field.nullable:
                errors.append(f"Column '{name}' is nullable (expected non-nullable)")
        if errors:
            raise SchemaValidationError(errors, self._history)
        return self

    # ------------------------------------------------------------------
    # Validation via call — DT(other_df) checks schema then wraps
    # ------------------------------------------------------------------

    def __call__(self, df: Any) -> _TypedDatasetBase:
        """
        Use this instance as a schema validator for another DataFrame.

        ``DT(other_df)`` validates ``other_df``'s schema against ``DT``'s
        expected schema and returns a new typed dataset.  Raises
        ``SchemaValidationError`` immediately if the schema doesn't match.
        """
        from pyspark.sql import DataFrame as SparkDF

        from frameguard.pyspark.schema import _compare_structs

        raw = df._df if type.__instancecheck__(_TypedDatasetBase, df) else df
        if not isinstance(raw, SparkDF):
            raise TypeError(f"Expected a DataFrame, got {type(df).__name__}")

        expected = type(self)._expected_schema
        if expected is not None:
            errors = _compare_structs(expected, raw.schema, strict=False, path="")
            if errors:
                raise SchemaValidationError([str(e) for e in errors], self._history)

        return _make_dataset(raw, history=self._history)

    # ------------------------------------------------------------------
    # Delegation / display
    # ------------------------------------------------------------------

    def __getattr__(self, name: str) -> Any:
        return getattr(self._df, name)

    def __repr__(self) -> str:
        fields = ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in self._df.schema.fields)
        return f"{type(self).__name__}[{fields}]"

    def __str__(self) -> str:
        return self.__repr__()

    def __len__(self) -> int:
        return int(self._df.count())


# ---------------------------------------------------------------------------
# Internal instance factory (used by _wrap, decorators, schema.empty())
# ---------------------------------------------------------------------------

def _make_dataset(df: Any, *, history: Any = None, strict: bool = False) -> _TypedDatasetBase:
    """
    Create a ``_TypedDatasetBase`` **instance** from a DataFrame.

    This is the low-level factory used internally (by ``_wrap``, decorators, and
    ``SparkSchema.empty``).  End-users should call ``dataset(df)`` to get a tracked
    instance, or ``schema_of(df)`` to get the schema type class.
    """
    from pyspark.sql import DataFrame as SparkDF

    raw = df._df if type.__instancecheck__(_TypedDatasetBase, df) else df
    if not isinstance(raw, SparkDF):
        raise TypeError(f"Expected a DataFrame, got {type(df).__name__}")

    schema = raw.schema
    DT_cls = cast(type[_TypedDatasetBase], _TypedDatasetMeta(
        "SchemaType",
        (_TypedDatasetBase,),
        {"_expected_schema": schema, "_source_df": raw},
    ))
    instance: _TypedDatasetBase = object.__new__(DT_cls)
    object.__setattr__(instance, "_df",      raw)
    object.__setattr__(instance, "_strict",  strict)
    object.__setattr__(instance, "_history", history or SchemaHistory.initial(raw.schema))
    return instance


# ---------------------------------------------------------------------------
# Public factory
# ---------------------------------------------------------------------------

def schema_of(df: Any) -> type[_TypedDatasetBase]:
    """
    Capture a DataFrame's schema as a Python type class.

    Returns a class whose ``isinstance`` check does **exact** schema matching:
    same column names, same types, nothing extra.  Assign in PascalCase and
    use as a type annotation::

        RawSchema = schema_of(raw_df)

        @enforce
        def enrich(df: RawSchema): ...   # wrong schema → raises at call site

    Capture a new type at each stage that changes the schema::

        EnrichedSchema = schema_of(enriched_df)
    """
    from pyspark.sql import DataFrame as SparkDF

    if type.__instancecheck__(_TypedDatasetBase, df):
        raw = df._df
    elif isinstance(df, SparkDF):
        raw = df
    else:
        raise TypeError(f"Expected a DataFrame, got {type(df).__name__}")

    # Returns a CLASS — beartype calls isinstance(arg, cls) at runtime,
    # routing through _TypedDatasetMeta.__instancecheck__ for schema validation.
    return cast(type[_TypedDatasetBase], _TypedDatasetMeta(
        "SchemaType",
        (_TypedDatasetBase,),
        {"_expected_schema": raw.schema, "_source_df": raw},
    ))


# ---------------------------------------------------------------------------
# TypedGroupedData
# ---------------------------------------------------------------------------

class TypedGroupedData:
    _gd:     Any
    _parent: _TypedDatasetBase
    __slots__ = ("_gd", "_parent")

    def __init__(self, grouped_data: Any, parent: _TypedDatasetBase) -> None:
        object.__setattr__(self, "_gd",     grouped_data)
        object.__setattr__(self, "_parent", parent)

    def _wrap_result(self, result_df: Any, operation: str) -> _TypedDatasetBase:
        return self._parent._wrap(result_df, operation)

    def agg(self, *exprs: Any) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.agg(*exprs), "groupBy(...).agg(...)")

    def count(self) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.count(), "groupBy(...).count()")

    def mean(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.mean(*cols), "groupBy(...).mean(...)")

    def avg(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.avg(*cols), "groupBy(...).avg(...)")

    def sum(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.sum(*cols), "groupBy(...).sum(...)")

    def min(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.min(*cols), "groupBy(...).min(...)")

    def max(self, *cols: str) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.max(*cols), "groupBy(...).max(...)")

    def pivot(self, pivot_col: str, values: list[Any] | None = None) -> TypedGroupedData:
        return TypedGroupedData(self._gd.pivot(pivot_col, values), parent=self._parent)

    def apply(self, udf: Any) -> _TypedDatasetBase:
        return self._wrap_result(self._gd.apply(udf), "groupBy(...).apply(udf)")

    def applyInPandas(self, func: Any, schema: Any) -> _TypedDatasetBase:
        return self._wrap_result(
            self._gd.applyInPandas(func, schema), "groupBy(...).applyInPandas(...)"
        )

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._gd, name)
        if callable(attr):
            def _delegated(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                from pyspark.sql import DataFrame as SparkDF
                if isinstance(result, SparkDF):
                    return self._wrap_result(result, f"groupBy(...).{name}(...)")
                return result
            return _delegated
        return attr


# ---------------------------------------------------------------------------
# TypedNaFunctions
# ---------------------------------------------------------------------------

class TypedNaFunctions:
    _na:     Any
    _parent: _TypedDatasetBase
    __slots__ = ("_na", "_parent")

    def __init__(self, na_funcs: Any, parent: _TypedDatasetBase) -> None:
        object.__setattr__(self, "_na",     na_funcs)
        object.__setattr__(self, "_parent", parent)

    def fill(self, value: Any, subset: list[str] | None = None) -> _TypedDatasetBase:
        return self._parent._wrap(
            self._na.fill(value, subset), f"na.fill({value!r}, subset={subset})"
        )

    def drop(
        self, how: str = "any", thresh: int | None = None, subset: list[str] | None = None
    ) -> _TypedDatasetBase:
        return self._parent._wrap(
            self._na.drop(how=how, thresh=thresh, subset=subset), f"na.drop(how={how!r})"
        )

    def replace(
        self, to_replace: Any, value: Any = None, subset: list[str] | None = None
    ) -> _TypedDatasetBase:
        return self._parent._wrap(
            self._na.replace(to_replace, value, subset), "na.replace(...)"
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._na, name)


# ---------------------------------------------------------------------------
# TypedStatFunctions
# ---------------------------------------------------------------------------

class TypedStatFunctions:
    _stat:   Any
    _parent: _TypedDatasetBase
    __slots__ = ("_stat", "_parent")

    def __init__(self, stat_funcs: Any, parent: _TypedDatasetBase) -> None:
        object.__setattr__(self, "_stat",   stat_funcs)
        object.__setattr__(self, "_parent", parent)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._stat, name)
        if callable(attr):
            def _delegated(*args: Any, **kwargs: Any) -> Any:
                result = attr(*args, **kwargs)
                from pyspark.sql import DataFrame as SparkDF
                if isinstance(result, SparkDF):
                    return self._parent._wrap(result, f"stat.{name}(...)")
                return result
            return _delegated
        return attr


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _struct_from_dict(col_types: dict[str, Any]) -> Any:
    from pyspark.sql.types import StructField, StructType

    from frameguard.pyspark.types import annotation_to_spark
    fields = []
    for col_name, annotation in col_types.items():
        spark_type, nullable = annotation_to_spark(annotation)
        fields.append(StructField(col_name, spark_type, nullable=nullable))
    return StructType(fields)
