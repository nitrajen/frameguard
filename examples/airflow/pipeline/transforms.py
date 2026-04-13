"""Pure transformation functions for the orders pipeline.

dfg.arm() in __init__.py covers this entire package -- no @dfg.enforce
decorator needed on most functions. @dfg.enforce(subset=False) is used
where an exact schema match is required, such as before writing to storage.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import dfguard.pyspark as dfg
from pipeline.schemas import EnrichedOrderSchema, RawOrderSchema


# Covered by dfg.arm() -- no decorator needed
def enrich(raw: RawOrderSchema) -> DataFrame:
    """Add revenue and flag high-value orders."""
    return (
        raw
        .withColumn("revenue", F.col("amount") * F.col("quantity"))
        .withColumn("is_high_value", F.col("revenue") > 500.0)
    )


# subset=False: the summary written to storage must match SummarySchema exactly
@dfg.enforce(subset=False)
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
