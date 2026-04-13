"""Pipeline node functions -- pure transformations.

dfg.arm() in orders_pipeline/__init__.py covers this entire package.
Schema contracts are enforced automatically from the type annotations.
No @dfg.enforce decorator needed on each function.
"""

from pyspark.sql import functions as F

import dfguard.pyspark as dfg
from orders_pipeline.schemas import EnrichedOrderSchema, RawOrderSchema


def enrich_orders(raw: RawOrderSchema):
    """Add revenue and flag high-value orders."""
    return (
        raw
        .withColumn("revenue", F.col("amount") * F.col("quantity"))
        .withColumn("is_high_value", F.col("revenue") > 500.0)
    )


# subset=False: output written to catalog must match exactly, no extra columns
@dfg.enforce(subset=False)
def summarise_by_customer(enriched: EnrichedOrderSchema):
    """Aggregate per customer."""
    return (
        enriched
        .groupBy("customer_id")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("order_count"),
        )
    )
