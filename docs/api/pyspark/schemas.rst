Schemas
=======

There are two ways to define a schema. The right choice depends on what
you have available and what kind of matching you need.

.. list-table::
   :header-rows: 1
   :widths: 25 37 38

   * -
     - ``schema_of(df)``
     - ``SparkSchema``
   * - **Matching**
     - Exact: same columns, same types, nothing extra
     - Subset: df must have at least the declared fields
   * - **Needs a live DataFrame**
     - Yes, snapshots the schema at that moment
     - No, declare the contract upfront
   * - **Best for**
     - Per-stage snapshots in a pipeline
     - Shared contracts, Kedro nodes, team APIs


schema_of
---------

Captures a live DataFrame's schema as a Python type. Use it at each
pipeline stage that changes the shape of the data.

.. code-block:: python

   from pyspark.sql import SparkSession, functions as F
   from frameguard.pyspark import schema_of, enforce

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   RawSchema   = schema_of(raw_df)
   enriched_df = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))
   EnrichedSchema = schema_of(enriched_df)   # new type, includes revenue

   @enforce
   def enrich(df: RawSchema) -> EnrichedSchema:
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df)        # OK
   enrich(enriched_df)   # raises: enriched_df has extra columns

``schema_of`` performs **exact matching**. A DataFrame with extra columns
does not satisfy the contract. Capture a new type at each stage::

   RawSchema      = schema_of(raw_df)        # order_id, amount, quantity
   EnrichedSchema = schema_of(enriched_df)   # order_id, amount, quantity, revenue

.. autofunction:: frameguard.pyspark.dataset.schema_of


SparkSchema
-----------

Declare a schema as a class using real PySpark types. No live DataFrame
needed. ``SparkSchema`` performs **subset matching**: the DataFrame must
have at least every declared field, but extra columns are fine.

.. code-block:: python

   from pyspark.sql import types as T
   from typing import Optional
   from frameguard.pyspark import SparkSchema, enforce

   class OrderSchema(SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()
       zip:      Optional[T.StringType()]   # nullable

   class EnrichedSchema(OrderSchema):       # inherits all OrderSchema fields
       revenue: T.DoubleType()

   @enforce
   def process(df: OrderSchema): ...

   # A DataFrame with only order_id, amount, quantity, zip passes.
   # A DataFrame with those fields plus extras also passes.
   # A DataFrame missing order_id raises immediately.

.. autoclass:: frameguard.pyspark.schema.SparkSchema
   :members: to_struct, validate, assert_valid, empty, from_struct, to_code, diff
   :member-order: bysource
