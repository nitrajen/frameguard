frameguard
==========

You've seen this error.

.. code-block:: text

   AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION] 'revenue' cannot be resolved.
   Did you mean one of: [order_id, amount, quantity, customer_id]?
     at org.apache.spark.sql.catalyst.analysis ...
     (47 more lines)

You refactored a pipeline stage. Wired it up slightly wrong. Spark planned
the job, serialized the query, shipped it to the executors, and then — forty
lines of JVM stack trace to tell you a column is missing.

The column was missing at the *call site*. Before Spark ran a single thing.

**frameguard catches it there.**

.. code-block:: python

   from frameguard.pyspark import schema_of, enforce

   RawSchema = schema_of(raw_df)

   @enforce
   def enrich(df: RawSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enriched_df    = enrich(raw_df)
   EnrichedSchema = schema_of(enriched_df)

   @enforce
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

.. code-block:: python

   # Capture from a live DataFrame: exact matching, per-stage snapshot
   RawSchema      = schema_of(raw_df)
   EnrichedSchema = schema_of(enriched_df)

   # Declare upfront: subset matching, no live DataFrame needed
   from frameguard.pyspark import SparkSchema
   from pyspark.sql import types as T

   class OrderSchema(SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   class EnrichedSchema(OrderSchema):   # inherits all parent fields
       revenue: T.DoubleType()

``schema_of`` is precise: exact schema, exact stage.
``SparkSchema`` is contractual: declare what you need, extra columns are fine.

.. toctree::
   :maxdepth: 2
   :caption: Contents

   quickstart
   pipelines
   api/index
