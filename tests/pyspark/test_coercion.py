"""
Tests for dfguard.pyspark.coercion: pure-Python type coercion rules.

Each test verifies our result_type() against what Spark's Catalyst actually
produces, confirming dfguard can resolve derived column types without Spark.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dfguard.pyspark.coercion import result_type

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def spark_result(spark, left_field, right_field, op_expr):
    """Ask Spark what type an expression produces."""
    schema = T.StructType([
        T.StructField("a", left_field,  False),
        T.StructField("b", right_field, False),
    ])
    df = spark.createDataFrame([], schema)
    return df.select(op_expr.alias("r")).schema.fields[0].dataType


# ---------------------------------------------------------------------------
# Addition  a + b
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("left, right, expected", [
    (T.ByteType(),          T.ShortType(),         T.ShortType()),
    (T.ShortType(),         T.IntegerType(),        T.IntegerType()),
    (T.IntegerType(),       T.LongType(),           T.LongType()),
    (T.IntegerType(),       T.FloatType(),          T.DoubleType()),
    (T.IntegerType(),       T.DoubleType(),         T.DoubleType()),
    (T.LongType(),          T.FloatType(),          T.DoubleType()),
    (T.LongType(),          T.DoubleType(),         T.DoubleType()),
    (T.FloatType(),         T.DoubleType(),         T.DoubleType()),
    (T.FloatType(),         T.DecimalType(10, 2),   T.DoubleType()),
    (T.DoubleType(),        T.DecimalType(10, 2),   T.DoubleType()),
    (T.IntegerType(),       T.DecimalType(10, 2),   T.DecimalType(13, 2)),
    (T.LongType(),          T.DecimalType(10, 2),   T.DecimalType(23, 2)),
    (T.DecimalType(10, 2),  T.DecimalType(10, 2),   T.DecimalType(11, 2)),
    (T.DecimalType(10, 4),  T.DecimalType(8,  2),   T.DecimalType(11, 4)),
])
def test_add_matches_spark(spark, left, right, expected):
    ours   = result_type(left, right, "+")
    theirs = spark_result(spark, left, right, F.col("a") + F.col("b"))
    assert ours == expected,  f"dfguard: {ours}"
    assert ours == theirs,    f"Spark disagrees: {theirs}"


# ---------------------------------------------------------------------------
# Subtraction  a - b  (same rules as addition)
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("left, right, expected", [
    (T.IntegerType(),  T.LongType(),         T.LongType()),
    (T.IntegerType(),  T.DoubleType(),        T.DoubleType()),
    (T.LongType(),     T.DecimalType(10, 2),  T.DecimalType(23, 2)),
])
def test_sub_matches_spark(spark, left, right, expected):
    ours   = result_type(left, right, "-")
    theirs = spark_result(spark, left, right, F.col("a") - F.col("b"))
    assert ours == expected
    assert ours == theirs


# ---------------------------------------------------------------------------
# Multiplication  a * b
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("left, right, expected", [
    (T.IntegerType(),      T.IntegerType(),      T.IntegerType()),
    (T.IntegerType(),      T.LongType(),          T.LongType()),
    (T.IntegerType(),      T.DoubleType(),        T.DoubleType()),
    (T.LongType(),         T.DoubleType(),        T.DoubleType()),
    (T.FloatType(),        T.DecimalType(10, 2),  T.DoubleType()),
    (T.DoubleType(),       T.DecimalType(10, 2),  T.DoubleType()),
    (T.DecimalType(10, 2), T.DecimalType(5,  1),  T.DecimalType(16, 3)),
])
def test_mul_matches_spark(spark, left, right, expected):
    ours   = result_type(left, right, "*")
    theirs = spark_result(spark, left, right, F.col("a") * F.col("b"))
    assert ours == expected,  f"dfguard: {ours}"
    assert ours == theirs,    f"Spark disagrees: {theirs}"


# ---------------------------------------------------------------------------
# Division  a / b: always Double
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("left, right", [
    (T.IntegerType(),  T.IntegerType()),
    (T.IntegerType(),  T.LongType()),
    (T.LongType(),     T.DoubleType()),
    (T.DoubleType(),   T.DecimalType(10, 2)),
])
def test_div_always_double(spark, left, right):
    ours   = result_type(left, right, "/")
    theirs = spark_result(spark, left, right, F.col("a") / F.col("b"))
    assert ours   == T.DoubleType()
    assert theirs == T.DoubleType()


# ---------------------------------------------------------------------------
# Comparison  a > b: always Boolean
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("left, right", [
    (T.IntegerType(), T.DoubleType()),
    (T.LongType(),    T.LongType()),
    (T.DoubleType(),  T.DecimalType(10, 2)),
])
def test_comparison_always_boolean(spark, left, right):
    ours   = result_type(left, right, ">")
    theirs = spark_result(spark, left, right, F.col("a") > F.col("b"))
    assert ours   == T.BooleanType()
    assert theirs == T.BooleanType()


# ---------------------------------------------------------------------------
# Symmetry: a op b == b op a
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("op", ["+", "*"])
@pytest.mark.parametrize("left, right", [
    (T.IntegerType(),      T.DoubleType()),
    (T.LongType(),         T.DecimalType(10, 2)),
    (T.DecimalType(10, 2), T.DecimalType(5, 1)),
])
def test_coercion_is_symmetric(left, right, op):
    assert result_type(left, right, op) == result_type(right, left, op)
