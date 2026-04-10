"""Pure transformation functions — each decorated with @fg.enforce.

In Airflow, DAG tasks receive paths and config as arguments, create a
SparkSession internally, do the work, and write results. DataFrames never
cross task boundaries (they're too large for XCom).

These helpers are what each task calls internally. @fg.enforce catches
schema problems at the call site, before any Spark execution begins.
"""

import frameguard.pyspark as fg
from pyspark.sql import DataFrame, functions as F

from pipeline.schemas import EnrichedOrderSchema, RawOrderSchema


@fg.enforce
def enrich(raw: RawOrderSchema) -> DataFrame:
    """Add revenue and flag high-value orders."""
    return (
        raw
        .withColumn("revenue", F.col("amount") * F.col("quantity"))
        .withColumn("is_high_value", F.col("revenue") > 500.0)
    )


@fg.enforce
def summarise(enriched: EnrichedOrderSchema) -> DataFrame:
    """Aggregate revenue and order count per customer."""
    return (
        enriched
        .groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
        )
    )
