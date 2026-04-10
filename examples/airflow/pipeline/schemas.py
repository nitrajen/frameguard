"""Shared schema contracts for the orders pipeline."""

import frameguard.pyspark as fg
from pyspark.sql import types as T


class RawOrderSchema(fg.SparkSchema):
    order_id:    T.LongType()
    customer_id: T.LongType()
    amount:      T.DoubleType()
    quantity:    T.IntegerType()
    status:      T.StringType()


class EnrichedOrderSchema(RawOrderSchema):
    revenue:      T.DoubleType()
    is_high_value: T.BooleanType()


class SummarySchema(fg.SparkSchema):
    customer_id:   T.LongType()
    total_revenue: T.DoubleType()
    order_count:   T.LongType()
