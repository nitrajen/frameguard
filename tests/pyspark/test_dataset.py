"""
dataset() wrapper: schema tracking, mutations, validation, groupBy, join, union.
schema_of(): schema capture and enforcement.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dfguard.pyspark import enforce, schema_of
from dfguard.pyspark.dataset import _make_dataset as dataset
from dfguard.pyspark.dataset import _TypedDatasetBase
from dfguard.pyspark.exceptions import SchemaValidationError

# --- Construction ---

def test_dataset_wraps_df(order_ds, order_df):
    assert isinstance(order_ds, _TypedDatasetBase)
    assert order_ds.schema == order_df.schema


def test_initial_history(order_ds):
    assert len(order_ds.schema_history) == 1
    assert order_ds.schema_history.changes[0].operation == "input"


# --- schema_of returns a class ---

def test_schema_of_returns_class(order_df):
    Schema = schema_of(order_df)
    assert isinstance(Schema, type)


def test_schema_of_exact_match(spark, order_df):
    Schema = schema_of(order_df)
    assert isinstance(order_df, Schema)


def test_schema_of_rejects_extra_columns(spark, order_df):
    """Exact matching: enriched DataFrame does NOT satisfy RawSchema."""
    RawSchema = schema_of(order_df)
    enriched = order_df.withColumn("revenue", F.col("amount") * F.col("quantity"))
    assert not isinstance(enriched, RawSchema)


def test_schema_of_as_enforce_annotation(spark, order_df):
    RawSchema = schema_of(order_df)

    @enforce
    def process(df: RawSchema): ...

    process(order_df)   # correct

    enriched = order_df.withColumn("revenue", F.col("amount") * F.col("quantity"))
    with pytest.raises(Exception):
        process(enriched)   # extra column


# --- withColumn ---

def test_withColumn_adds_and_tracks(order_ds):
    ds = order_ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
    assert "revenue" in ds.columns
    rev = next(f for f in ds.schema.fields if f.name == "revenue")
    assert rev.dataType == T.DoubleType()   # Spark promotes Double * Int -> Double
    last = ds.schema_history.changes[-1]
    assert "withColumn('revenue')" in last.operation
    assert any(f.name == "revenue" for f in last.added)


def test_withColumn_replace_records_type_change(order_ds):
    ds = order_ds.withColumn("amount", F.col("amount").cast(T.FloatType()))
    last = ds.schema_history.changes[-1]
    assert any("amount" == name for name, _, _ in last.type_changed)


def test_withColumn_chained(order_ds):
    ds = (
        order_ds
        .withColumn("revenue", F.col("amount") * F.col("quantity"))
        .withColumn("tax",     F.col("revenue") * 0.1)
    )
    assert len(ds.schema_history) == 3   # input + 2 withColumn
    for col in ("revenue", "tax"):
        f = next(x for x in ds.schema.fields if x.name == col)
        assert f.dataType == T.DoubleType()


# --- drop / select ---

def test_drop_records_history(order_ds):
    ds = order_ds.drop("quantity", "active")
    assert "quantity" not in ds.columns
    assert "active"   not in ds.columns
    assert "quantity" in ds.schema_history.changes[-1].dropped


def test_select_reduces_schema(order_ds):
    ds = order_ds.select("order_id", "amount")
    assert ds.columns == ["order_id", "amount"]


def test_select_with_derived_column(order_ds):
    ds = order_ds.select(
        F.col("order_id"),
        (F.col("amount") * 2).alias("double_amount"),
    )
    f = next(x for x in ds.schema.fields if x.name == "double_amount")
    assert f.dataType == T.DoubleType()


# --- filter / orderBy / distinct: schema preserved ---

def test_schema_preserving_ops(order_ds):
    for ds in [
        order_ds.filter(F.col("amount") > 100),
        order_ds.orderBy("amount"),
        order_ds.distinct(),
        order_ds.limit(1),
    ]:
        assert ds.schema == order_ds.schema


# --- groupBy ---

def test_groupby_agg(order_ds):
    ds = order_ds.groupBy("active").agg(
        F.sum("amount").alias("total"),
        F.count("*").alias("n"),
    )
    assert isinstance(ds, _TypedDatasetBase)
    assert next(f for f in ds.schema.fields if f.name == "total").dataType == T.DoubleType()
    assert next(f for f in ds.schema.fields if f.name == "n").dataType    == T.LongType()


# --- join ---

def test_join_merges_schemas(spark, order_ds):
    extra = spark.createDataFrame(
        [(1, "North"), (2, "South")],
        T.StructType([
            T.StructField("order_id", T.LongType()),
            T.StructField("region",   T.StringType()),
        ]),
    )
    ds = order_ds.join(extra, on="order_id")
    assert "region" in ds.columns
    assert isinstance(ds, _TypedDatasetBase)


# --- union ---

def test_union_preserves_schema(order_ds):
    ds = order_ds.union(order_ds)
    assert ds.schema == order_ds.schema


# --- na ---

def test_na_fill_preserves_schema(order_ds):
    ds = order_ds.na.fill(0, subset=["quantity"])
    assert ds.schema == order_ds.schema


# --- validate / assert helpers ---

def test_validate_passes(order_ds):
    from tests.pyspark.conftest import OrderSchema
    assert order_ds.validate(OrderSchema) is order_ds


def test_validate_fails_with_history(order_ds):
    from tests.pyspark.conftest import EnrichedOrderSchema
    with pytest.raises(SchemaValidationError) as exc:
        order_ds.validate(EnrichedOrderSchema)
    assert "Schema evolution" in str(exc.value)


def test_assert_columns(order_ds):
    order_ds.assert_columns("order_id", "amount")
    with pytest.raises(SchemaValidationError):
        order_ds.assert_columns("nonexistent")


def test_assert_not_null_nullable_field(spark):
    df = spark.createDataFrame(
        [(1,)], T.StructType([T.StructField("x", T.LongType(), nullable=True)])
    )
    with pytest.raises(SchemaValidationError):
        dataset(df).assert_not_null("x")


# --- transform ---

def test_transform(order_ds):
    def add_tax(ds):
        return ds.withColumn("tax", F.col("amount") * 0.1)
    result = order_ds.transform(add_tax)
    assert "tax" in result.columns
    assert len(result.schema_history) == 2


# --- delegation ---

def test_count_delegates(order_ds):
    assert order_ds.count() == 2
