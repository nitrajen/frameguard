dfguard
==========

Data pipelines fail late. A DataFrame with the wrong schema enters a function
without complaint, the job runs, and the crash surfaces somewhere downstream
with an error that tells you nothing about where the mismatch started.

**dfguard moves that failure to the function call.** The wrong DataFrame is
rejected immediately with a precise error: which function, which argument, what
schema was expected, what arrived.

Currently supports PySpark. pandas and polars support coming soon.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F
   from pyspark.sql import types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )

   # Declare the input contract upfront -- no live DataFrame needed
   class RawSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()

   @dfg.enforce
   def enrich(df: RawSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   # Capture the output schema from the live result
   EnrichedSchema = dfg.schema_of(enrich(raw_df))

   @dfg.enforce
   def flag_high_value(df: EnrichedSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

   flag_high_value(raw_df)
   # TypeError: Schema mismatch in flag_high_value() argument 'df':
   #   expected: order_id:bigint, amount:double, quantity:int, revenue:double
   #   received: order_id:bigint, amount:double, quantity:int

No validation logic inside the functions.
The wrong DataFrame simply cannot enter the wrong function.

For package-wide enforcement without decorating each function, call
``dfg.arm()`` once from your package entry point -- see the :doc:`quickstart`.

``pip install dfguard[pyspark]`` installs only PySpark, which you already
have. Enforcement is pure Python ``isinstance()`` checks. Only schema-annotated
arguments are validated; ``str``, ``int``, and everything else passes through
untouched.

Two ways to define a schema
----------------------------

**Capture from a live DataFrame** (exact matching, snapshot at each stage):

.. code-block:: python

   RawSchema      = dfg.schema_of(raw_df)
   EnrichedSchema = dfg.schema_of(enriched_df)

**Declare upfront** (no DataFrame required):

.. code-block:: python

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   class EnrichedSchema(OrderSchema):   # inherits all parent fields
       revenue: T.DoubleType()

``dfg.schema_of`` is precise: exact schema, exact stage.
``dfg.SparkSchema`` is contractual: declare what you need upfront.

See the :doc:`quickstart` for the full walkthrough.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   pipelines
   airflow
   kedro
   api/index
