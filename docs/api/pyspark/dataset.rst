Dataset
=======

``dfg.dataset(df)`` wraps a plain DataFrame in a tracked instance. Every
transform is recorded in ``schema_history``, so when validation fails
you can see exactly where the schema diverged.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3, ["vip"], "10001")],
       "order_id LONG, amount DOUBLE, quantity INT, tags ARRAY<STRING>, zip STRING",
   )

   ds = dfg.dataset(raw_df)
   ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
   ds = ds.drop("tags")

   print(ds.schema_history)
   # [0] input                  order_id:long, amount:double, quantity:int, tags:array<string>, zip:string
   # [1] withColumn('revenue')  + revenue:double
   # [2] drop(['tags'])         - tags

All standard DataFrame methods (``withColumn``, ``select``, ``drop``,
``filter``, ``join``, etc.) work normally. The tracked instance adds
schema history on top.

Validate against a schema at any point:

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   ds = dfg.dataset(raw_df)
   ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
   ds.validate(OrderSchema)   # raises SchemaValidationError if columns are missing

When ``validate`` fails, the error message includes the full history so
you can see which step introduced the mismatch.

.. autoclass:: dfguard.pyspark.dataset._TypedDatasetBase
   :members: validate, schema_history
   :member-order: bysource

.. autoclass:: dfguard.pyspark.dataset.TypedGroupedData
   :members:
   :undoc-members:
