"""SparkSchema: definition, to_struct, validate, inheritance, from_struct, isinstance."""

import pytest
from pyspark.sql import types as T

from dfguard.pyspark import Optional, SparkSchema, enforce
from dfguard.pyspark.exceptions import TypeAnnotationError


class AddressSchema(SparkSchema):
    street: T.StringType()
    city:   T.StringType()
    zip:    Optional[T.StringType()]


class OrderSchema(SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    address:  AddressSchema


class EnrichedSchema(OrderSchema):
    revenue: T.DoubleType()


# --- to_struct ---

def test_to_struct_basic():
    s = OrderSchema.to_struct()
    names = [f.name for f in s.fields]
    assert names == ["order_id", "amount", "address"]
    assert s["order_id"].dataType == T.LongType()
    assert s["amount"].dataType == T.DoubleType()


def test_nullable_field():
    s = AddressSchema.to_struct()
    assert s["zip"].nullable is True
    assert s["street"].nullable is False


def test_nested_struct():
    s = OrderSchema.to_struct()
    addr = s["address"].dataType
    assert isinstance(addr, T.StructType)
    assert addr["street"].dataType == T.StringType()


def test_inheritance_merges_fields():
    s = EnrichedSchema.to_struct()
    names = [f.name for f in s.fields]
    assert "order_id" in names
    assert "amount"   in names
    assert "revenue"  in names


def test_bad_annotation_raises():
    with pytest.raises(TypeAnnotationError):
        class Bad(SparkSchema):
            x: "not_a_type"  # noqa: F821
        Bad.to_struct()


# --- validate ---

def test_validate_passes(spark):
    df = spark.createDataFrame([], OrderSchema.to_struct())
    assert OrderSchema.validate(df) == []


def test_validate_missing_column(spark):
    partial = T.StructType([T.StructField("order_id", T.LongType())])
    df = spark.createDataFrame([], partial)
    errors = OrderSchema.validate(df)
    assert any("amount" in str(e) for e in errors)


def test_validate_type_mismatch(spark):
    wrong = T.StructType([
        T.StructField("order_id", T.StringType()),  # should be Long
        T.StructField("amount",   T.DoubleType()),
        T.StructField("address",  AddressSchema.to_struct()),
    ])
    df = spark.createDataFrame([], wrong)
    errors = OrderSchema.validate(df)
    assert any("order_id" in str(e) for e in errors)


def test_validate_strict_rejects_extra(spark):
    extra = T.StructType(
        OrderSchema.to_struct().fields + [T.StructField("extra", T.StringType())]
    )
    df = spark.createDataFrame([], extra)
    assert OrderSchema.validate(df, subset=False) != []
    assert OrderSchema.validate(df, subset=True) == []


# --- isinstance via SparkSchema annotation ---

def test_sparkschema_as_enforce_annotation(spark):
    """SparkSchema subclass works as an enforce annotation with subset matching."""
    @enforce
    def process(df: OrderSchema): ...

    good_df = spark.createDataFrame([], OrderSchema.to_struct())
    process(good_df)   # must not raise

    # Enriched DataFrame (extra column) also satisfies OrderSchema
    extra = T.StructType(OrderSchema.to_struct().fields + [T.StructField("rev", T.DoubleType())])
    enriched_df = spark.createDataFrame([], extra)
    process(enriched_df)   # must not raise: subset matching

    # Wrong schema raises
    wrong_df = spark.createDataFrame([], T.StructType([T.StructField("x", T.StringType())]))
    with pytest.raises(Exception):
        process(wrong_df)


# --- from_struct / empty / to_code ---

def test_from_struct_round_trip(spark):
    struct = OrderSchema.to_struct()
    Generated = SparkSchema.from_struct(struct, name="Generated")
    assert Generated.to_struct() == struct


def test_empty_creates_zero_row_df(spark):
    ds = OrderSchema.empty(spark)
    assert ds.count() == 0
    assert ds.schema == OrderSchema.to_struct()


def test_to_code_is_executable(spark):
    code = OrderSchema.to_code()
    assert "OrderSchema" in code
    assert "T.LongType()" in code
