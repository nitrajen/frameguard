"""SparkSchema: declare a DataFrame's expected shape as a Python class."""

from __future__ import annotations

from typing import Any

from dfguard.pyspark.exceptions import SchemaValidationError, TypeAnnotationError
from dfguard.pyspark.types import annotation_to_spark, spark_type_to_str


class _SchemaError:
    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return self.message


class _SparkSchemaMeta(type):
    """Collects annotations at class-definition time and caches the StructType."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> _SparkSchemaMeta:
        cls = super().__new__(mcs, name, bases, namespace)

        # Merge annotations from all bases in MRO order; child wins on conflict
        merged: dict[str, Any] = {}
        for base in reversed(cls.__mro__):
            if base is object:
                continue
            merged.update(getattr(base, "__annotations__", {}))

        cls._schema_fields: dict[str, Any] = {  # type: ignore[misc, attr-defined]
            k: v for k, v in merged.items() if not k.startswith("_")
        }
        cls._cached_struct: Any = None  # type: ignore[misc, attr-defined]
        return cls

    def __instancecheck__(cls, instance: Any) -> bool:
        """
        Enable ``isinstance(df, MySchema)`` as a subset schema check.

        Returns True when ``instance`` (a DataFrame or dataset wrapper) has
        at least every field declared in ``cls``, with matching types.
        Extra columns are allowed. SparkSchema expresses a contract, not an
        exact snapshot.
        """
        schema = getattr(instance, "schema", None)
        if schema is None:
            return False
        try:
            required = {f.name: f.dataType for f in cls.to_struct().fields}  # type: ignore[attr-defined]
            actual   = {f.name: f.dataType for f in schema.fields}
            return all(actual.get(name) == dtype for name, dtype in required.items())
        except Exception:
            return False


class SparkSchema(metaclass=_SparkSchemaMeta):
    """
    Declare a DataFrame's expected shape as a Python class.

    Use this when you want to write down a schema without a live DataFrame.
    ``SparkSchema`` uses **subset matching**: the DataFrame must have every
    declared field, but extra columns are fine.  This is the opposite of
    ``schema_of(df)``, which requires an exact match (no extra columns).

    ::

        from pyspark.sql import types as T
        from typing import Optional
        from dfguard.pyspark import SparkSchema, enforce

        class OrderSchema(SparkSchema):
            order_id: T.LongType()
            amount:   T.DoubleType()
            quantity: T.IntegerType()
            zip:      Optional[T.StringType()]   # nullable field

        @enforce
        def process(df: OrderSchema): ...

        # A DataFrame with only these columns passes.
        # A DataFrame with extra columns also passes (subset matching).
        # A DataFrame missing 'order_id' raises immediately.

    Child classes inherit all parent fields::

        class EnrichedSchema(OrderSchema):
            revenue: T.DoubleType()   # adds revenue, keeps order_id/amount/quantity/zip

    Note: use ``Optional[T.XxxType()]`` for nullable fields. PySpark DataType
    instances do not support the ``X | None`` union syntax.
    ``SubSchema | None`` works when the field type is a nested SparkSchema
    subclass (a Python class, not a DataType instance).
    """

    @classmethod
    def _fg_check(cls, value: Any, subset: bool) -> bool:
        """
        Enforcement protocol hook called by ``@fg.enforce`` at runtime.

        Any class that exposes this method participates in dfguard enforcement
        automatically. New DataFrame backends just add this to their schema type.

        - ``subset=True``: extra columns in *value* are fine (default).
        - ``subset=False``: *value* must have exactly the declared columns, nothing extra.
        """
        if subset:
            return isinstance(value, cls)  # extra columns fine
        # Exact column-name set required. Use isinstance for type/value check first
        # (avoids duplicating the type-check logic), then compare names only so
        # nullable mismatches don't cause false failures.
        if not isinstance(value, cls):
            return False
        schema = getattr(value, "schema", None)
        if schema is None:
            return False
        actual_names   = {f.name for f in schema.fields}
        declared_names = {f.name for f in cls.to_struct().fields}
        return actual_names == declared_names

    @classmethod
    def to_struct(cls) -> Any:
        """Return the ``StructType`` for this schema. Result is cached after the first call."""
        if cls._cached_struct is not None:  # type: ignore[attr-defined]
            return cls._cached_struct  # type: ignore[attr-defined]

        from pyspark.sql.types import StructField, StructType

        fields: list[StructField] = []
        for col_name, annotation in cls._schema_fields.items():  # type: ignore[attr-defined]
            try:
                spark_type, nullable = annotation_to_spark(annotation)
            except TypeError as exc:
                raise TypeAnnotationError(
                    f"Field '{col_name}' on {cls.__name__}: {exc}"
                ) from exc
            fields.append(StructField(col_name, spark_type, nullable=nullable))

        cls._cached_struct = StructType(fields)  # type: ignore[attr-defined]
        return cls._cached_struct  # type: ignore[attr-defined]

    @classmethod
    def from_struct(cls, struct: Any, name: str = "GeneratedSchema") -> type[SparkSchema]:
        """
        Create a new SparkSchema subclass from a live ``StructType``.

        Nested StructTypes are recursively converted to their own SparkSchema
        subclasses. Useful for round-tripping or generating typed wrappers for
        DataFrames whose schema is only known at runtime.
        """
        annotations: dict[str, Any] = {}
        nested_classes: dict[str, type[SparkSchema]] = {}

        for field in struct.fields:
            annotations[field.name] = _struct_field_to_annotation(
                field, parent_name=name, nested_classes=nested_classes
            )

        new_cls = _SparkSchemaMeta(
            name,
            (SparkSchema,),
            {"__annotations__": annotations, **nested_classes},
        )
        return new_cls  # type: ignore[return-value]

    @classmethod
    def validate(
        cls,
        df_or_schema: Any,
        subset: bool = True,
    ) -> list[_SchemaError]:
        """
        Compare a DataFrame or StructType against this schema.

        Returns a list of errors; an empty list means the schema is valid.
        When ``subset=True`` (default), extra columns beyond the declared schema are fine.
        When ``subset=False``, extra columns are also reported as errors.
        """
        from pyspark.sql import DataFrame as SparkDF

        from dfguard.pyspark.dataset import _TypedDatasetBase

        if isinstance(df_or_schema, _TypedDatasetBase):
            actual_struct = df_or_schema.schema
        elif isinstance(df_or_schema, SparkDF):
            actual_struct = df_or_schema.schema
        else:
            actual_struct = df_or_schema
        return _compare_structs(cls.to_struct(), actual_struct, strict=not subset, path="")

    @classmethod
    def assert_valid(
        cls,
        df_or_schema: Any,
        subset: bool = True,
        history: Any = None,
    ) -> None:
        """Like ``validate`` but raises ``SchemaValidationError`` on the first failure."""
        from dfguard.pyspark.history import SchemaHistory

        errors = cls.validate(df_or_schema, subset=subset)
        if errors:
            h = history or SchemaHistory.initial(
                df_or_schema.schema if hasattr(df_or_schema, "schema") else df_or_schema
            )
            raise SchemaValidationError([str(e) for e in errors], h)

    @classmethod
    def empty(cls, spark: Any) -> Any:
        """Return an empty typed dataset instance with this schema and zero rows."""
        from dfguard.pyspark.dataset import _make_dataset

        return _make_dataset(spark.createDataFrame([], cls.to_struct()))

    @classmethod
    def to_code(cls) -> str:
        """Generate valid Python source code for this schema class."""
        lines = ["from pyspark.sql import types as T", "from typing import Optional", ""]
        lines.append(f"class {cls.__name__}(SparkSchema):")
        if not cls._schema_fields:  # type: ignore[attr-defined]
            lines.append("    pass")
        else:
            for col_name, annotation in cls._schema_fields.items():  # type: ignore[attr-defined]
                lines.append(f"    {col_name}: {_annotation_to_str(annotation)}")
        return "\n".join(lines)

    @classmethod
    def diff(cls, other: type[SparkSchema]) -> str:
        """Return a human-readable diff between two SparkSchema classes."""
        errors = _compare_structs(other.to_struct(), cls.to_struct(), strict=True, path="")
        if not errors:
            return f"{cls.__name__} and {other.__name__} are identical"
        lines = [f"Diff {cls.__name__} → {other.__name__}:"]
        for e in errors:
            lines.append(f"  {e}")
        return "\n".join(lines)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._cached_struct = None  # type: ignore[attr-defined]  # invalidate cache for new subclass


def _compare_structs(
    expected: Any,
    actual: Any,
    strict: bool,
    path: str,
) -> list[_SchemaError]:
    """
    Recursively diff two StructTypes.

    ``path`` is the dot-separated column path used in nested error messages
    (e.g. ``"address.street"``). Recursion handles nested StructType, ArrayType,
    and MapType at arbitrary depth.
    """
    from pyspark.sql import types as T

    errors: list[_SchemaError] = []
    expected_fields: dict[str, Any] = {f.name: f for f in expected.fields}
    actual_fields:   dict[str, Any] = {f.name: f for f in actual.fields}

    for name, exp_field in expected_fields.items():
        col_path = f"{path}.{name}" if path else name

        if name not in actual_fields:
            errors.append(_SchemaError(
                f"Missing column '{col_path}' "
                f"(expected {exp_field.dataType.simpleString()}, nullable={exp_field.nullable})"
            ))
            continue

        act_field = actual_fields[name]

        if type(exp_field.dataType) is not type(act_field.dataType):
            errors.append(_SchemaError(
                f"Column '{col_path}': type mismatch: "
                f"expected {exp_field.dataType.simpleString()}, "
                f"got {act_field.dataType.simpleString()}"
            ))
        elif isinstance(exp_field.dataType, T.StructType):
            errors.extend(_compare_structs(
                exp_field.dataType, act_field.dataType, strict=strict, path=col_path,
            ))
        elif isinstance(exp_field.dataType, T.ArrayType):
            errors.extend(_compare_array_types(
                exp_field.dataType, act_field.dataType, strict=strict, path=col_path,
            ))
        elif isinstance(exp_field.dataType, T.MapType):
            errors.extend(_compare_map_types(
                exp_field.dataType, act_field.dataType, path=col_path,
            ))
        elif exp_field.dataType != act_field.dataType:
            errors.append(_SchemaError(
                f"Column '{col_path}': type mismatch: "
                f"expected {exp_field.dataType.simpleString()}, "
                f"got {act_field.dataType.simpleString()}"
            ))

        if strict and exp_field.nullable != act_field.nullable:
            errors.append(_SchemaError(
                f"Column '{col_path}': nullable mismatch: "
                f"expected nullable={exp_field.nullable}, got nullable={act_field.nullable}"
            ))

    if strict:
        for name in actual_fields:
            col_path = f"{path}.{name}" if path else name
            if name not in expected_fields:
                errors.append(_SchemaError(f"Unexpected column '{col_path}' (strict mode)"))

    return errors


def _compare_array_types(
    expected: Any, actual: Any, strict: bool, path: str
) -> list[_SchemaError]:
    """Compare element types of two ArrayTypes, recursing into nested structs."""
    from pyspark.sql import types as T

    errors: list[_SchemaError] = []
    if type(expected.elementType) is not type(actual.elementType):
        errors.append(_SchemaError(
            f"Array element type mismatch at '{path}': "
            f"expected {expected.elementType.simpleString()}, "
            f"got {actual.elementType.simpleString()}"
        ))
    elif isinstance(expected.elementType, T.StructType):
        errors.extend(_compare_structs(
            expected.elementType, actual.elementType, strict=strict, path=f"{path}[]",
        ))
    return errors


def _compare_map_types(expected: Any, actual: Any, path: str) -> list[_SchemaError]:
    """Compare key and value types of two MapTypes."""
    errors: list[_SchemaError] = []
    if expected.keyType != actual.keyType:
        errors.append(_SchemaError(
            f"Map key type mismatch at '{path}': "
            f"expected {expected.keyType.simpleString()}, got {actual.keyType.simpleString()}"
        ))
    if expected.valueType != actual.valueType:
        errors.append(_SchemaError(
            f"Map value type mismatch at '{path}': "
            f"expected {expected.valueType.simpleString()}, got {actual.valueType.simpleString()}"
        ))
    return errors


def _struct_field_to_annotation(
    field: Any,
    parent_name: str,
    nested_classes: dict[str, type[SparkSchema]],
) -> Any:
    """Convert a StructField to a dfguard annotation, wrapping with fg.Optional if nullable."""
    from dfguard.pyspark._nullable import _NullableAnnotation

    annotation = _spark_type_to_annotation(field.dataType, parent_name, nested_classes)
    return _NullableAnnotation(annotation) if field.nullable else annotation


def _spark_type_to_annotation(
    spark_type: Any,
    parent_name: str,
    nested_classes: dict[str, type[SparkSchema]],
) -> Any:
    """
    Recursively convert a Spark DataType to a Python annotation.

    Primitive and complex types are returned as DataType *instances*, exactly
    what users would write themselves (e.g. ``T.LongType()``, ``T.ArrayType(T.StringType())``).
    Nested StructTypes are converted to SparkSchema subclasses so the round-trip
    preserves the class-based API.
    """
    from pyspark.sql import types as T

    if isinstance(spark_type, T.StructType):
        nested_name = f"{parent_name}_nested_{len(nested_classes)}"
        nested_cls  = SparkSchema.from_struct(spark_type, name=nested_name)
        nested_classes[nested_name] = nested_cls
        return nested_cls

    # For all other DataType instances, return them directly.
    # They are already the correct annotation for SparkSchema fields.
    return spark_type


def _annotation_to_str(annotation: Any) -> str:
    """Render a field annotation as its Python source string for to_code() output."""
    import typing

    from pyspark.sql import types as T

    origin = getattr(annotation, "__origin__", None)
    args   = getattr(annotation, "__args__", ())

    # Optional[X]
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return f"Optional[{_annotation_to_str(non_none[0])}]"

    # DataType instance → render as T.XxxType(...)
    if isinstance(annotation, T.DataType):
        return spark_type_to_str(annotation)

    # SparkSchema subclass (nested struct)
    if isinstance(annotation, type) and issubclass(annotation, SparkSchema):
        return annotation.__name__

    # Fallback
    return str(annotation)
