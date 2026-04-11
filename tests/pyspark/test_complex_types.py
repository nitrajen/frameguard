"""Enforcement with ArrayType, MapType, and deeply nested column types.

All matching is delegated to PySpark's own DataType.__eq__, which handles
recursive equality for any nesting depth. These tests confirm that dfguard
does not hardcode specific types and correctly handles the full type hierarchy.
"""

import pytest
from pyspark.sql import types as T

from dfguard.pyspark import Optional, SparkSchema, enforce

# ---------------------------------------------------------------------------
# Schema fixtures: defined once, reused across tests
# ---------------------------------------------------------------------------

class AddressSchema(SparkSchema):
    street: T.StringType()
    city:   T.StringType()
    zip:    Optional[T.StringType()]


class EventSchema(SparkSchema):
    user_id: T.LongType()
    tags:    T.ArrayType(T.StringType())


class MetricsSchema(SparkSchema):
    user_id: T.LongType()
    scores:  T.MapType(T.StringType(), T.DoubleType())


class PersonSchema(SparkSchema):
    name:      T.StringType()
    addresses: T.ArrayType(AddressSchema.to_struct())  # array of structs


class DeepSchema(SparkSchema):
    id:   T.LongType()
    data: T.MapType(T.StringType(), T.ArrayType(T.DoubleType()))


# ---------------------------------------------------------------------------
# ArrayType column
# ---------------------------------------------------------------------------

def test_array_col_isinstance_passes(spark):
    df = spark.createDataFrame(
        [(1, ["vip", "active"])],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("tags",    T.ArrayType(T.StringType())),
        ]),
    )
    assert isinstance(df, EventSchema)


def test_array_col_wrong_element_type_fails(spark):
    """ArrayType(LongType) does not satisfy ArrayType(StringType)."""
    df = spark.createDataFrame(
        [(1, [1, 2])],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("tags",    T.ArrayType(T.LongType())),  # wrong element type
        ]),
    )
    assert not isinstance(df, EventSchema)


def test_array_col_enforce_passes(spark):
    @enforce
    def process(df: EventSchema): return df

    df = spark.createDataFrame(
        [(1, ["a"])],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("tags",    T.ArrayType(T.StringType())),
        ]),
    )
    process(df)   # must not raise


def test_array_col_enforce_rejects_wrong_type(spark):
    @enforce
    def process(df: EventSchema): return df

    df = spark.createDataFrame(
        [(1, {"a": 1.0})],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("tags",    T.MapType(T.StringType(), T.DoubleType())),
        ]),
    )
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(df)


# ---------------------------------------------------------------------------
# MapType column
# ---------------------------------------------------------------------------

def test_map_col_isinstance_passes(spark):
    df = spark.createDataFrame(
        [(1, {"accuracy": 0.95})],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("scores",  T.MapType(T.StringType(), T.DoubleType())),
        ]),
    )
    assert isinstance(df, MetricsSchema)


def test_map_col_wrong_value_type_fails(spark):
    """MapType(String, Long) does not satisfy MapType(String, Double)."""
    df = spark.createDataFrame(
        [(1, {"accuracy": 1})],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("scores",  T.MapType(T.StringType(), T.LongType())),
        ]),
    )
    assert not isinstance(df, MetricsSchema)


def test_map_col_enforce_passes(spark):
    @enforce
    def process(df: MetricsSchema): return df

    df = spark.createDataFrame(
        [(1, {"accuracy": 0.95})],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("scores",  T.MapType(T.StringType(), T.DoubleType())),
        ]),
    )
    process(df)


# ---------------------------------------------------------------------------
# ArrayType(StructType): array of structs
# ---------------------------------------------------------------------------

def test_array_of_structs_isinstance_passes(spark):
    addr_type = AddressSchema.to_struct()
    df = spark.createDataFrame(
        [("Alice", [("123 Main", "Austin", None)])],
        T.StructType([
            T.StructField("name",      T.StringType()),
            T.StructField("addresses", T.ArrayType(addr_type)),
        ]),
    )
    assert isinstance(df, PersonSchema)


def test_array_of_structs_wrong_struct_fails(spark):
    """Array containing a struct with different fields fails."""
    wrong_struct = T.StructType([
        T.StructField("street", T.StringType()),
        T.StructField("country", T.StringType()),   # 'city' missing, 'country' extra
    ])
    df = spark.createDataFrame(
        [("Alice", [("123 Main", "US")])],
        T.StructType([
            T.StructField("name",      T.StringType()),
            T.StructField("addresses", T.ArrayType(wrong_struct)),
        ]),
    )
    assert not isinstance(df, PersonSchema)


def test_array_of_structs_enforce_passes(spark):
    @enforce
    def process(df: PersonSchema): return df

    addr_type = AddressSchema.to_struct()
    df = spark.createDataFrame(
        [("Bob", [("456 Elm", "Dallas", "75001")])],
        T.StructType([
            T.StructField("name",      T.StringType()),
            T.StructField("addresses", T.ArrayType(addr_type)),
        ]),
    )
    process(df)


# ---------------------------------------------------------------------------
# Deep nesting: MapType(String, ArrayType(Double))
# ---------------------------------------------------------------------------

def test_deep_nesting_isinstance_passes(spark):
    df = spark.createDataFrame(
        [(1, {"metrics": [1.0, 2.0, 3.0]})],
        T.StructType([
            T.StructField("id",   T.LongType()),
            T.StructField("data", T.MapType(T.StringType(), T.ArrayType(T.DoubleType()))),
        ]),
    )
    assert isinstance(df, DeepSchema)


def test_deep_nesting_wrong_inner_type_fails(spark):
    """MapType(String, ArrayType(Long)) != MapType(String, ArrayType(Double))."""
    df = spark.createDataFrame(
        [(1, {"metrics": [1, 2, 3]})],
        T.StructType([
            T.StructField("id",   T.LongType()),
            T.StructField("data", T.MapType(T.StringType(), T.ArrayType(T.LongType()))),
        ]),
    )
    assert not isinstance(df, DeepSchema)


# ---------------------------------------------------------------------------
# subset=False with complex column types
# ---------------------------------------------------------------------------

def test_subset_false_exact_with_array_col(spark):
    """subset=False requires exact column name set; works correctly with array columns."""
    @enforce(subset=False)
    def process(df: EventSchema): return df

    exact_df = spark.createDataFrame(
        [(1, ["a"])],
        T.StructType([
            T.StructField("user_id", T.LongType()),
            T.StructField("tags",    T.ArrayType(T.StringType())),
        ]),
    )
    process(exact_df)   # exact match, must pass


def test_subset_false_rejects_extra_with_array_col(spark):
    """subset=False rejects extra columns even when array column types match."""
    @enforce(subset=False)
    def process(df: EventSchema): return df

    extra_df = spark.createDataFrame(
        [(1, ["a"], "extra")],
        T.StructType([
            T.StructField("user_id",   T.LongType()),
            T.StructField("tags",      T.ArrayType(T.StringType())),
            T.StructField("extra_col", T.StringType()),
        ]),
    )
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(extra_df)


def test_subset_true_allows_extra_with_map_col(spark):
    """subset=True (default) allows extra columns alongside a map column."""
    @enforce
    def process(df: MetricsSchema): return df

    extra_df = spark.createDataFrame(
        [(1, {"accuracy": 0.9}, "label")],
        T.StructType([
            T.StructField("user_id",  T.LongType()),
            T.StructField("scores",   T.MapType(T.StringType(), T.DoubleType())),
            T.StructField("category", T.StringType()),
        ]),
    )
    process(extra_df)   # must not raise
