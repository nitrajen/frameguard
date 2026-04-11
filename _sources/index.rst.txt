frameguard
==========

Data pipelines fail late. You pass the wrong DataFrame into a function, the
job runs, and eventually crashes with an error pointing at the wrong place.
The actual bug was earlier, at the function call.

**frameguard moves that failure to the function call.** The wrong DataFrame is
rejected immediately with a precise error: which function, which argument, what
schema was expected, what arrived.

Currently supports PySpark. pandas and polars support coming soon.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   RawSchema = fg.schema_of(raw_df)

   @fg.enforce
   def enrich(df: RawSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   EnrichedSchema = fg.schema_of(enrich(raw_df))

   @fg.enforce
   def flag_high_value(df: EnrichedSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

   flag_high_value(raw_df)
   # TypeError: Schema mismatch in flag_high_value() argument 'df':
   #   expected: order_id:long, amount:double, quantity:int, revenue:double
   #   received: order_id:long, amount:double, quantity:int
   #
   # Raised before Spark plans a single task.

No validation logic inside the function. No waiting for Spark.
The wrong DataFrame simply cannot enter the wrong function.

``pip install frameguard[pyspark]`` installs only PySpark, which you already
have. Enforcement is pure Python ``isinstance()`` checks. Only schema-annotated
arguments are validated; ``str``, ``int``, and everything else passes through
untouched.

Two ways to define a schema
----------------------------

**Capture from a live DataFrame** (exact matching, snapshot at each stage):

.. code-block:: python

   RawSchema      = fg.schema_of(raw_df)
   EnrichedSchema = fg.schema_of(enriched_df)

**Declare upfront** (no DataFrame required):

.. code-block:: python

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   class EnrichedSchema(OrderSchema):   # inherits all parent fields
       revenue: T.DoubleType()

``fg.schema_of`` is precise: exact schema, exact stage.
``fg.SparkSchema`` is contractual: declare what you need upfront.

See the :doc:`quickstart` for the full walkthrough.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   pipelines
   airflow
   kedro
   api/index
