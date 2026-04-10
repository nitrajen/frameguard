Dataset
=======

``dataset(df)`` wraps a plain DataFrame in a tracked instance. Every
transform is recorded in ``schema_history``, so when validation fails
you can see exactly where the schema diverged.

.. code-block:: python

   from pyspark.sql import functions as F
   from frameguard.pyspark import dataset

   ds = dataset(raw_df)
   ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
   ds = ds.drop("tags")

   print(ds.schema_history)
   # [0] input                  order_id:long, amount:double, quantity:int, tags:array<string>
   # [1] withColumn('revenue')  + revenue:double
   # [2] drop(['tags'])         - tags

All standard DataFrame methods (``withColumn``, ``select``, ``drop``,
``filter``, ``join``, etc.) work normally. The tracked instance adds
schema history on top.

Validate against a schema at any point::

   from frameguard.pyspark import SparkSchema
   from pyspark.sql import types as T

   class OrderSchema(SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   ds.validate(OrderSchema)   # raises SchemaValidationError if columns are missing

When ``validate`` fails, the error message includes the full history so
you can see which step introduced the mismatch.

.. autoclass:: frameguard.pyspark.dataset._TypedDatasetBase
   :members: validate, schema_history
   :member-order: bysource

.. autoclass:: frameguard.pyspark.dataset.TypedGroupedData
   :members:
   :undoc-members:
