Decorators
==========

These decorators attach schema validation explicitly to a function's
input or output, using ``SparkSchema`` classes you declare upfront.
They are an alternative to the ``@enforce`` + annotation approach,
useful when you want explicit validation logic that is separate from
the type annotation.

check_schema
------------

Validates only the **return value**. Use this when the input does not
need guarding but you want to guarantee the output shape.

.. code-block:: python

   from pyspark.sql import types as T
   from frameguard.pyspark import SparkSchema, check_schema

   class EnrichedSchema(SparkSchema):
       order_id: T.LongType()
       revenue:  T.DoubleType()

   @check_schema(EnrichedSchema)
   def enrich(df):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   # If the returned DataFrame is missing 'revenue', raises SchemaValidationError.

.. autofunction:: frameguard.pyspark.decorators.check_schema

----

typed_transform
---------------

Validates both the **input** and the **return value**. Use this for
functions at critical pipeline boundaries where both sides matter.

.. code-block:: python

   from pyspark.sql import types as T
   from frameguard.pyspark import SparkSchema, typed_transform

   class RawSchema(SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   class EnrichedSchema(RawSchema):
       revenue: T.DoubleType()

   @typed_transform(input_schema=RawSchema, output_schema=EnrichedSchema)
   def enrich(df):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   # Wrong input schema raises on the way in.
   # Wrong output schema raises on the way out.

.. autofunction:: frameguard.pyspark.decorators.typed_transform
