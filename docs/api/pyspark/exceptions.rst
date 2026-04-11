Exceptions
==========

All dfguard exceptions inherit from ``DfTypesError``, so you can catch
everything with a single ``except DfTypesError`` if needed.

.. code-block:: python

   import dfguard.pyspark as dfg
   from dfguard.pyspark.exceptions import DfTypesError, SchemaValidationError
   from pyspark.sql import SparkSession, functions as F, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       revenue:  T.DoubleType()   # not yet in raw_df

   ds = dfg.dataset(raw_df)

   try:
       ds.validate(OrderSchema)
   except SchemaValidationError as e:
       print(e.errors)    # list of field-level mismatches
       print(e.history)   # full schema history up to the failure

----

.. autoclass:: dfguard.pyspark.exceptions.DfTypesError
   :members:

.. autoclass:: dfguard.pyspark.exceptions.SchemaValidationError
   :members:

.. autoclass:: dfguard.pyspark.exceptions.TypeAnnotationError
   :members:

.. autoclass:: dfguard.pyspark.exceptions.ColumnNotFoundError
   :members:
