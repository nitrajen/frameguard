Schemas
=======

There are two ways to define a schema. The right choice depends on whether
you have a live DataFrame at definition time.

.. list-table::
   :header-rows: 1
   :widths: 25 37 38

   * -
     - ``fg.schema_of(df)``
     - ``fg.SparkSchema``
   * - **Defined by**
     - Inferred from a live DataFrame
     - Declared upfront in code
   * - **Needs a live DataFrame**
     - Yes, snapshots the schema at that moment
     - No, declare the contract upfront
   * - **Best for**
     - Per-stage snapshots in a pipeline
     - Shared contracts, Kedro nodes, team APIs


fg.schema_of
------------

Captures a live DataFrame's schema as a Python type. Use it at each
pipeline stage that changes the shape of the data.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   RawSchema      = fg.schema_of(raw_df)
   enriched_df    = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))
   EnrichedSchema = fg.schema_of(enriched_df)   # new type, includes revenue

   @fg.enforce
   def enrich(df: RawSchema) -> EnrichedSchema:
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df)        # OK
   enrich(enriched_df)   # raises: enriched_df has extra columns

``fg.schema_of`` does **exact matching**: same columns, same types, nothing extra.
Capture a new type at each stage that changes the schema::

   RawSchema      = fg.schema_of(raw_df)        # order_id, amount, quantity
   EnrichedSchema = fg.schema_of(enriched_df)   # order_id, amount, quantity, revenue

.. autofunction:: frameguard.pyspark.dataset.schema_of


fg.SparkSchema
--------------

Declare a schema as a class using real PySpark types. No live DataFrame needed.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import types as T
   from typing import Optional

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()
       zip:      Optional[T.StringType()]   # nullable

   class EnrichedSchema(OrderSchema):       # inherits all OrderSchema fields
       revenue: T.DoubleType()

   @fg.enforce
   def process(df: OrderSchema): ...

   # A DataFrame with only order_id, amount, quantity, zip passes.
   # A DataFrame missing order_id raises immediately.

.. autoclass:: frameguard.pyspark.schema.SparkSchema
   :members: to_struct, validate, assert_valid, empty, from_struct, to_code, diff
   :member-order: bysource
