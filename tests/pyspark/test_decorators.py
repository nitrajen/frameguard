"""@typed_transform and @check_schema: input/output validation."""

import pytest
from pyspark.sql import functions as F
from pyspark.sql import types as T

from dfguard.pyspark import SparkSchema, check_schema, typed_transform
from dfguard.pyspark.exceptions import SchemaValidationError


class InputSchema(SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()


class OutputSchema(InputSchema):
    revenue: T.DoubleType()


@pytest.fixture()
def input_df(spark):
    return spark.createDataFrame(
        [(1, 10.0), (2, 20.0)],
        InputSchema.to_struct(),
    )


def test_typed_transform_validates_input(spark, input_df):
    @typed_transform(input_schema=InputSchema)
    def enrich(df):
        return df.withColumn("revenue", F.col("amount") * 2)

    result = enrich(input_df)
    assert "revenue" in result.columns


def test_typed_transform_rejects_wrong_input(spark, input_df):
    @typed_transform(input_schema=OutputSchema)   # requires revenue: not present
    def process(df):
        return df

    with pytest.raises(SchemaValidationError):
        process(input_df)


def test_typed_transform_validates_output(spark, input_df):
    @typed_transform(input_schema=InputSchema, output_schema=OutputSchema)
    def enrich(df):
        return df.withColumn("revenue", F.col("amount") * 2)

    enrich(input_df)   # must not raise


def test_typed_transform_catches_bad_output(spark, input_df):
    @typed_transform(input_schema=InputSchema, output_schema=OutputSchema)
    def bad_enrich(df):
        return df   # missing revenue

    with pytest.raises(SchemaValidationError):
        bad_enrich(input_df)


def test_check_schema_validates_return(spark, input_df):
    @check_schema(OutputSchema)
    def enrich(df):
        return df.withColumn("revenue", F.col("amount") * 2)

    enrich(input_df)   # must not raise


def test_check_schema_raises_on_missing(spark, input_df):
    @check_schema(OutputSchema)
    def bad(df):
        return df

    with pytest.raises(SchemaValidationError):
        bad(input_df)
