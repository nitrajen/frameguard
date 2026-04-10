"""Shared schema contracts for the orders pipeline.

Define schemas here and import them in node files. This gives you a single
source of truth for every stage boundary in the pipeline.
"""

import frameguard.pyspark as fg
from pyspark.sql import types as T
from typing import Optional


class RawOrderSchema(fg.SparkSchema):
    order_id:    T.LongType()
    customer_id: T.LongType()
    amount:      T.DoubleType()
    quantity:    T.IntegerType()
    status:      T.StringType()


class EnrichedOrderSchema(RawOrderSchema):
    revenue:     T.DoubleType()
    is_high_value: T.BooleanType()


class SummarySchema(fg.SparkSchema):
    customer_id:   T.LongType()
    total_revenue: T.DoubleType()
    order_count:   T.LongType()
