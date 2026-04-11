"""annotation_to_spark: converting type annotations to (DataType, nullable)."""

import pytest
from pyspark.sql import types as T

from dfguard.pyspark import Optional
from dfguard.pyspark.schema import SparkSchema
from dfguard.pyspark.types import annotation_to_spark


def test_datatype_instance():
    spark_type, nullable = annotation_to_spark(T.LongType())
    assert spark_type == T.LongType()
    assert nullable is False


def test_optional_makes_nullable():
    spark_type, nullable = annotation_to_spark(Optional[T.LongType()])
    assert spark_type == T.LongType()
    assert nullable is True


def test_sparkschema_subclass():
    class Inner(SparkSchema):
        x: T.LongType()

    spark_type, nullable = annotation_to_spark(Inner)
    assert isinstance(spark_type, T.StructType)
    assert nullable is False


def test_optional_sparkschema():
    class Inner(SparkSchema):
        x: T.LongType()

    spark_type, nullable = annotation_to_spark(Optional[Inner])
    assert isinstance(spark_type, T.StructType)
    assert nullable is True


def test_invalid_annotation_raises():
    with pytest.raises(TypeError, match="Cannot convert"):
        annotation_to_spark("not_a_type")
