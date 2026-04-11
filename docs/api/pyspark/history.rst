Schema History
==============

When you use ``dfg.dataset(df)``, every schema-changing operation is
recorded. ``schema_history`` gives you the full chain of transforms
that produced the current DataFrame.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3, ["vip"])],
       "order_id LONG, amount DOUBLE, quantity INT, tags ARRAY<STRING>",
   )

   ds = dfg.dataset(raw_df)
   ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
   ds = ds.drop("tags")

   for change in ds.schema_history:
       print(change)
   # [0] input                  order_id:long, amount:double, quantity:int, tags:array<string>
   # [1] withColumn('revenue')  + revenue:double
   # [2] drop(['tags'])         - tags

When ``validate()`` raises a ``SchemaValidationError``, the exception
includes the full history so you know which step broke the contract.

.. autoclass:: dfguard.pyspark.history.SchemaHistory
   :members:
   :undoc-members:

.. autoclass:: dfguard.pyspark.history.SchemaChange
   :members:
   :undoc-members:
