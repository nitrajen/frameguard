"""Shared schema contracts for the orders pipeline."""

import dfguard.pyspark as dfg
from pyspark.sql import types as T


class RawOrderSchema(dfg.SparkSchema):
    order_id:    T.LongType()
    customer_id: T.LongType()
    amount:      T.DoubleType()
    quantity:    T.IntegerType()
    status:      T.StringType()


class EnrichedOrderSchema(RawOrderSchema):
    revenue:       T.DoubleType()
    is_high_value: T.BooleanType()


class SummarySchema(dfg.SparkSchema):
    customer_id:   T.LongType()
    total_revenue: T.DoubleType()
    order_count:   T.LongType()
