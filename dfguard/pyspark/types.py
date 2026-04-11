"""Annotation-to-Spark conversion utilities.

No custom type aliases. Use pyspark.sql.types directly.
"""

from __future__ import annotations

import types as _builtin_types
from typing import Any, Union, get_args, get_origin

# Cache: annotation → (DataType instance, nullable bool)
_ANNOTATION_CACHE: dict[Any, tuple[Any, bool]] = {}


def annotation_to_spark(annotation: Any) -> tuple[Any, bool]:
    """
    Convert a type annotation to ``(DataType instance, nullable)``.

    Accepts:
    - ``pyspark.sql.types.DataType`` instances  e.g. ``LongType()``, ``ArrayType(StringType())``
    - ``pyspark.sql.types.DataType`` subclasses e.g. ``LongType`` (instantiated automatically)
    - ``Optional[<any of the above>]``           → ``nullable=True``
    - A ``SparkSchema`` subclass                 → converted to ``StructType`` recursively
    """
    try:
        if annotation in _ANNOTATION_CACHE:
            return _ANNOTATION_CACHE[annotation]
    except TypeError:
        pass  # DataType instances may not be hashable in all Spark versions

    from pyspark.sql import types as T

    from dfguard.pyspark._nullable import _NullableAnnotation

    # fg.Optional[X] — our nullable wrapper, works on Python 3.10+
    if isinstance(annotation, _NullableAnnotation):
        spark_type, _ = annotation_to_spark(annotation.inner)
        return spark_type, True

    origin = get_origin(annotation)
    args   = get_args(annotation)

    # Optional[X]  ≡  Union[X, None]
    if origin is Union or (
        hasattr(_builtin_types, "UnionType") and isinstance(annotation, _builtin_types.UnionType)
    ):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            spark_type, _ = annotation_to_spark(non_none[0])
            return spark_type, True
        raise TypeError(
            f"Only Optional[T] (Union with None) is supported for nullable columns, "
            f"got: {annotation}"
        )

    # Already a DataType instance, use as-is
    if isinstance(annotation, T.DataType):
        result = annotation, False
        try:
            _ANNOTATION_CACHE[annotation] = result
        except TypeError:
            pass
        return result

    # A DataType subclass (e.g. LongType, not LongType()) -- instantiate
    if isinstance(annotation, type) and issubclass(annotation, T.DataType):
        result = annotation(), False
        _ANNOTATION_CACHE[annotation] = result
        return result

    # SparkSchema subclass → StructType (deferred import to avoid circular)
    from dfguard.pyspark.schema import SparkSchema
    if isinstance(annotation, type) and issubclass(annotation, SparkSchema):
        result = annotation.to_struct(), False
        _ANNOTATION_CACHE[annotation] = result
        return result

    raise TypeError(
        f"Cannot convert {annotation!r} to a Spark DataType. "
        "Annotate fields with pyspark.sql.types instances "
        "(e.g. LongType(), ArrayType(StringType())) "
        "or a SparkSchema subclass for nested structs."
    )


def spark_type_to_str(spark_type: Any) -> str:
    """Return a Python source string for a DataType instance (used by infer_schema / to_code)."""
    from pyspark.sql import types as T

    t = type(spark_type)
    name = t.__name__

    if isinstance(spark_type, T.DecimalType):
        return f"T.DecimalType({spark_type.precision}, {spark_type.scale})"
    if isinstance(spark_type, T.ArrayType):
        inner = spark_type_to_str(spark_type.elementType)
        null  = "" if not spark_type.containsNull else ", containsNull=True"
        return f"T.ArrayType({inner}{null})"
    if isinstance(spark_type, T.MapType):
        k = spark_type_to_str(spark_type.keyType)
        v = spark_type_to_str(spark_type.valueType)
        null = "" if not spark_type.valueContainsNull else ", valueContainsNull=True"
        return f"T.MapType({k}, {v}{null})"
    if isinstance(spark_type, T.StructType):
        return "SparkSchema"   # infer_schema replaces with the real nested class name

    return f"T.{name}()"
