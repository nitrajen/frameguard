"""Shared fixtures for the PySpark test suite."""

import pytest
from pyspark.sql import types as T

from dfguard.pyspark import Optional, SparkSchema, dataset


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("dfguard-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


class AddressSchema(SparkSchema):
    street: T.StringType()
    city:   T.StringType()
    zip:    Optional[T.StringType()]


class OrderSchema(SparkSchema):
    order_id:  T.LongType()
    customer:  T.StringType()
    amount:    T.DoubleType()
    quantity:  T.IntegerType()
    active:    T.BooleanType()
    tags:      T.ArrayType(T.StringType())
    address:   AddressSchema


class EnrichedOrderSchema(OrderSchema):
    revenue:  T.DoubleType()
    discount: Optional[T.DoubleType()]


@pytest.fixture()
def order_df(spark):
    rows = [
        (1, "Alice", 99.9,  2, True,  ["sale"],      ("123 Main St", "Springfield", "12345")),
        (2, "Bob",   199.0, 1, False, ["clearance"], ("456 Oak Ave", "Shelbyville", None)),
    ]
    return spark.createDataFrame(rows, OrderSchema.to_struct())


@pytest.fixture()
def order_ds(order_df):
    return dataset(order_df)
