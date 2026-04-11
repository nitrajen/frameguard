"""
End-to-end pipeline: enrich -> flag -> aggregate -> score.
Verifies that stage-boundary enforcement works and that
passing the wrong DataFrame fails immediately.
"""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dfguard.pyspark import dataset, enforce, schema_of

ADDRESS = T.StructType([
    T.StructField("street", T.StringType(), nullable=True),
    T.StructField("city",   T.StringType(), nullable=True),
    T.StructField("zip",    T.StringType(), nullable=True),
])

RAW_SCHEMA = T.StructType([
    T.StructField("order_id",    T.LongType(),                False),
    T.StructField("customer_id", T.LongType(),                False),
    T.StructField("amount",      T.DoubleType(),              False),
    T.StructField("quantity",    T.IntegerType(),             False),
    T.StructField("tags",        T.ArrayType(T.StringType()), True),
    T.StructField("address",     ADDRESS,                     True),
])

ROWS = [
    (1, 100, 250.0, 3, ["new", "sale"],    ("10 Main St", "Boston", "02101")),
    (2, 101,  49.99, 1, ["clearance"],     ("20 Oak Ave", "Austin", "73301")),
    (3, 100, 999.0, 2, ["premium"],        ("10 Main St", "Boston", "02101")),
    (4, 101, 3500.0, 1, ["luxury", "gift"],("20 Oak Ave", "Austin", "73301")),
]


@pytest.fixture()
def raw_df(spark):
    return spark.createDataFrame(ROWS, RAW_SCHEMA)


def test_stage_boundary_enforcement(spark, raw_df):
    """Wrong DataFrame passed to any stage raises before Spark runs."""
    RawType = schema_of(raw_df)

    @enforce
    def enrich(df: RawType):
        return (
            df
            .withColumn("revenue",  F.col("amount") * F.col("quantity"))
            .withColumn("discount",
                        F.when(F.col("revenue") > 500, F.lit(50.0)).otherwise(F.lit(0.0)))
        )

    enriched_df  = enrich(raw_df)
    EnrichedType = schema_of(enriched_df)

    @enforce
    def flag(df: EnrichedType):
        return df.withColumn("is_high_value", F.col("revenue") > 500)

    # Correct ordering works
    flagged_df = flag(enriched_df)
    assert "is_high_value" in flagged_df.columns

    # Skipping enrichment -> immediate failure
    with pytest.raises(Exception):
        flag(raw_df)


def test_full_pipeline_schema_and_data(spark, raw_df):
    """Run all four stages; verify final schema and row-level results."""
    RawType = schema_of(raw_df)

    @enforce
    def enrich(df: RawType):
        return (
            df
            .withColumn("revenue",  F.col("amount") * F.col("quantity"))
            .withColumn("discount",
                        F.when(F.col("revenue") > 500, F.lit(50.0)).otherwise(F.lit(0.0)))
        )

    enriched_df  = enrich(raw_df)
    EnrichedType = schema_of(enriched_df)

    @enforce
    def flag(df: EnrichedType):
        return df.withColumn("is_high_value", F.col("revenue") > 500)

    flagged_df = flag(enriched_df)
    FlaggedType = schema_of(flagged_df)

    @enforce
    def aggregate(df: FlaggedType):
        return df.groupBy("customer_id").agg(
            F.count("order_id") .alias("order_count"),
            F.sum("revenue")    .alias("total_revenue"),
        )

    agg_df  = aggregate(flagged_df)
    AggType = schema_of(agg_df)

    @enforce
    def score(df: AggType):
        return df.withColumn(
            "risk_label",
            F.when(F.col("total_revenue") > 2000, F.lit("high")).otherwise(F.lit("low")),
        )

    final = score(agg_df)

    cols = {f.name for f in final.schema.fields}
    assert {"customer_id", "order_count", "total_revenue", "risk_label"} <= cols

    rows = {r.customer_id: r for r in final.collect()}
    assert rows[100].order_count == 2
    assert rows[101].risk_label  == "high"


def test_schema_history_tracks_evolution(spark, raw_df):
    """dataset() wrapper records every schema mutation."""
    ds = dataset(raw_df)
    ds = ds.withColumn("revenue",      F.col("amount") * F.col("quantity"))
    ds = ds.withColumn("is_high_value",F.col("revenue") > 500)
    ds = ds.drop("tags", "address")

    ops    = [c.operation for c in ds.schema_history.changes]
    added  = [f.name for c in ds.schema_history.changes for f in c.added]
    dropped= [n for c in ds.schema_history.changes for n in c.dropped]

    assert any("revenue"       in op for op in ops)
    assert any("is_high_value" in op for op in ops)
    assert "revenue"       in added
    assert "tags"          in dropped
