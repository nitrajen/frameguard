"""
Verify that BOTH ways to define a schema type work as enforce annotations,
and that the two semantics (exact vs subset) are correct and distinct.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dfguard.pyspark import SparkSchema, enforce, schema_of


class RawSchema(SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()


class EnrichedSchema(RawSchema):
    revenue: T.DoubleType()


def test_schema_of_exact_rejects_enriched(spark):
    """schema_of(raw_df) must reject a DataFrame that has extra columns."""
    raw = spark.createDataFrame(
        [(1, 9.9)],
        T.StructType([
            T.StructField("order_id", T.LongType()),
            T.StructField("amount",   T.DoubleType()),
        ]),
    )
    RawType = schema_of(raw)

    @enforce
    def stage1(df: RawType): ...

    stage1(raw)   # exact match

    enriched = raw.withColumn("revenue", F.col("amount") * 2)
    with pytest.raises(Exception):
        stage1(enriched)   # extra column


def test_sparkschema_subset_accepts_enriched(spark):
    """SparkSchema annotation uses subset matching: extra columns are OK."""
    @enforce
    def stage1(df: RawSchema): ...

    raw = spark.createDataFrame(
        [(1, 9.9)],
        T.StructType([
            T.StructField("order_id", T.LongType()),
            T.StructField("amount",   T.DoubleType()),
        ]),
    )
    enriched = raw.withColumn("revenue", F.col("amount") * 2)

    stage1(raw)       # ok
    stage1(enriched)  # ok: subset match, extra column is fine


def test_sparkschema_rejects_missing_field(spark):
    """SparkSchema annotation fails when a required field is absent."""
    @enforce
    def process(df: EnrichedSchema): ...

    raw = spark.createDataFrame(
        [(1, 9.9)],
        T.StructType([
            T.StructField("order_id", T.LongType()),
            T.StructField("amount",   T.DoubleType()),
        ]),
    )
    with pytest.raises(Exception):
        process(raw)   # missing revenue


def test_sparkschema_rejects_wrong_type(spark):
    """SparkSchema annotation fails when a field has the wrong type."""
    @enforce
    def process(df: RawSchema): ...

    wrong = spark.createDataFrame(
        [("1", 9.9)],
        T.StructType([
            T.StructField("order_id", T.StringType()),   # should be Long
            T.StructField("amount",   T.DoubleType()),
        ]),
    )
    with pytest.raises(Exception):
        process(wrong)


def test_schema_of_and_sparkschema_interoperable(spark):
    """
    schema_of() and SparkSchema can both be used in the same pipeline.
    schema_of() is exact (per-stage snapshot), SparkSchema is structural (contract).
    """
    raw = spark.createDataFrame(
        [(1, 9.9)],
        T.StructType([
            T.StructField("order_id", T.LongType()),
            T.StructField("amount",   T.DoubleType()),
        ]),
    )
    RawType = schema_of(raw)

    @enforce
    def validate_input(df: RawSchema): ...    # structural: SparkSchema

    @enforce
    def enrich(df: RawType):                  # exact: schema_of snapshot
        return df.withColumn("revenue", F.col("amount") * 2)

    validate_input(raw)
    result = enrich(raw)
    assert "revenue" in result.columns
