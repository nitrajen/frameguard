"""Pipeline nodes — each function is a pure transformation.

frameguard enforces schema contracts at the function boundary.
No validation logic inside the functions themselves.
"""

import frameguard.pyspark as fg
from pyspark.sql import functions as F
from orders_pipeline.schemas import EnrichedOrderSchema, RawOrderSchema


@fg.enforce
def enrich_orders(raw: RawOrderSchema):
    """Add revenue and flag high-value orders.

    Accepts any DataFrame that satisfies RawOrderSchema (subset=True by default).
    Fails immediately if required columns are missing or have wrong types.
    """
    return (
        raw
        .withColumn("revenue", F.col("amount") * F.col("quantity"))
        .withColumn("is_high_value", F.col("revenue") > 500.0)
    )


@fg.enforce
def summarise_by_customer(enriched: EnrichedOrderSchema):
    """Aggregate per customer.

    Requires all EnrichedOrderSchema columns. Fails if revenue or is_high_value
    are missing — catching the case where the wrong stage output is passed in.
    """
    return (
        enriched
        .groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
        )
    )
