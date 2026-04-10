frameguard
==========

PySpark schema errors fail at the worst possible time. A function receives
the wrong DataFrame, Spark plans the job, serializes the query, ships work
to executors, and then surfaces this:

.. code-block:: text

   AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] 'revenue' cannot be resolved.
   Did you mean one of: [order_id, amount, quantity, customer_id]?
     at org.apache.spark.sql.catalyst.analysis ...
     (47 more lines)

By then the context is gone. The mismatch happened at the call site, before
Spark touched anything.

**frameguard raises there instead.**

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

   enriched_df    = enrich(raw_df)
   EnrichedSchema = fg.schema_of(enriched_df)

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

Zero extra dependencies. ``pip install frameguard[pyspark]`` installs only
PySpark, which you already have. The enforcement is pure Python ``isinstance()``
checks. Only your DataFrame arguments are validated — ``str``, ``int``, and
everything else passes through untouched.

Two ways to define a schema
----------------------------

**Capture from a live DataFrame** — exact matching, snapshot at each stage:

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )
   enriched_df = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   RawSchema      = fg.schema_of(raw_df)
   EnrichedSchema = fg.schema_of(enriched_df)

**Declare upfront** — no DataFrame required:

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import types as T

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   class EnrichedSchema(OrderSchema):   # inherits all parent fields
       revenue: T.DoubleType()

``fg.schema_of`` is precise: exact schema, exact stage.
``fg.SparkSchema`` is contractual: declare what you need upfront.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   pipelines
   api/index
