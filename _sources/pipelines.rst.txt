Data Pipeline Usage
===================

frameguard works differently depending on your pipeline framework, because
Kedro and Airflow handle DataFrames differently. This page explains what to
reach for in each case.

Quick reference
---------------

+------------------------------------------+------------------------------------------+
| What you want                            | What to use                              |
+==========================================+==========================================+
| Enforce one function                     | ``@enforce``                             |
+------------------------------------------+------------------------------------------+
| Enforce all functions in a file          | ``arm()`` at the bottom of the file      |
+------------------------------------------+------------------------------------------+
| Enforce an entire package from one place | ``arm(package="my_pipeline.nodes")``     |
+------------------------------------------+------------------------------------------+
| Validate a DataFrame right after loading | ``MySchema.assert_valid(df)``            |
+------------------------------------------+------------------------------------------+
| Turn off enforcement in tests or CI      | ``disable()`` / ``enable_enforcement()`` |
+------------------------------------------+------------------------------------------+

One thing to know before you start
------------------------------------

In Kedro, DataFrames come from the catalog at runtime. You cannot call
``schema_of(raw_df)`` at module level because ``raw_df`` does not exist
when the module is imported. **Use SparkSchema for all Kedro nodes.**

``schema_of(df)`` is still useful in scripts and notebooks where you have a
live DataFrame in hand.

----

Kedro
-----

The recommended structure is: ``SparkSchema`` for every node schema,
``arm(package=...)`` once in ``settings.py``. That's it. No decoration on
individual functions, no ``arm()`` scattered across every file.

Define schemas and nodes
~~~~~~~~~~~~~~~~~~~~~~~~~

Schemas live alongside the functions that use them. Keep them in the same
file or a shared ``schemas.py`` module — whichever reads better for your team.

.. code-block:: python

   # my_pipeline/nodes/raw_nodes.py
   from pyspark.sql import functions as F, types as T
   from frameguard.pyspark import SparkSchema


   class RawOrderSchema(SparkSchema):
       order_id:    T.LongType()
       amount:      T.DoubleType()
       quantity:    T.IntegerType()
       customer_id: T.LongType()


   def validate_orders(df: RawOrderSchema):
       return df.filter(F.col("amount") > 0)


   def deduplicate(df: RawOrderSchema):
       return df.dropDuplicates(["order_id"])

.. code-block:: python

   # my_pipeline/nodes/enrichment_nodes.py
   from pyspark.sql import functions as F, types as T
   from frameguard.pyspark import SparkSchema
   from my_pipeline.nodes.raw_nodes import RawOrderSchema


   class EnrichedOrderSchema(RawOrderSchema):
       revenue: T.DoubleType()   # inherits all RawOrderSchema fields


   def enrich(df: RawOrderSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))


   def flag_high_value(df: EnrichedOrderSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

Arm everything from settings.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kedro loads ``settings.py`` before any pipeline runs. Put ``arm`` there and
every node in the package gets enforced, with no changes to the node files.

.. code-block:: python

   # my_pipeline/settings.py
   from frameguard.pyspark import arm

   arm(package="my_pipeline.nodes")

That single call walks every module under ``my_pipeline.nodes``, finds every
function with a schema annotation, and wraps it. Functions without schema
annotations are left alone. Arguments that aren't DataFrames (``str``,
``int``, etc.) inside wrapped functions are also not touched.

What you get
~~~~~~~~~~~~~

After ``settings.py`` loads, Kedro's runner calls your nodes normally. The
wrong DataFrame raises before any Spark work happens:

.. code-block:: python

   # raw_df has order_id, amount, quantity, customer_id
   validate_orders(raw_df)       # OK

   # enriched_df has all raw fields + revenue
   # RawOrderSchema uses subset matching, so extra columns are fine
   validate_orders(enriched_df)  # OK

   # raw_df is missing 'revenue', which EnrichedOrderSchema requires
   flag_high_value(raw_df)
   # TypeError: Schema mismatch in flag_high_value() argument 'df':
   #   expected: order_id:long, amount:double, quantity:int, customer_id:long, revenue:double
   #   received: order_id:long, amount:double, quantity:int, customer_id:long

   # wrong_df has completely different columns
   validate_orders(wrong_df)
   # TypeError: Schema mismatch in validate_orders() argument 'df':
   #   expected: order_id:long, amount:double, quantity:int, customer_id:long
   #   received: user_id:long, name:string

----

Airflow
-------

Airflow tasks are Python callables. They receive paths or config, create a
SparkSession inside, do the work, and write the result. DataFrames are never
passed between tasks directly — only serializable values are.

This means ``@enforce`` on task functions doesn't buy you much (the args are
strings, not DataFrames). The useful tools are:

- ``SparkSchema.assert_valid(df)`` right after loading — catches schema drift
  from upstream the moment data enters your task, not halfway through processing
- ``@enforce`` on helper functions that the task calls internally

Validate after loading
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # my_dag.py
   from pyspark.sql import types as T
   from frameguard.pyspark import SparkSchema, enforce


   class RawOrderSchema(SparkSchema):
       order_id:    T.LongType()
       amount:      T.DoubleType()
       quantity:    T.IntegerType()
       customer_id: T.LongType()


   class EnrichedOrderSchema(RawOrderSchema):
       revenue: T.DoubleType()


   @enforce
   def compute_revenue(df: RawOrderSchema):
       from pyspark.sql import functions as F
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))


   @enforce
   def flag_vip(df: EnrichedOrderSchema):
       from pyspark.sql import functions as F
       return df.withColumn("is_vip", F.col("revenue") > 1000)


   def enrich_orders_task(input_path: str, output_path: str) -> str:
       from pyspark.sql import SparkSession
       spark = SparkSession.getOrCreate()

       df = spark.read.parquet(input_path)

       # Validate right after loading. If upstream changed the schema,
       # you find out here — not in a cryptic Spark error three steps later.
       RawOrderSchema.assert_valid(df)

       enriched = compute_revenue(df)   # @enforce checks df's schema here
       flagged  = flag_vip(enriched)    # @enforce checks enriched's schema

       flagged.write.mode("overwrite").parquet(output_path)
       return output_path

``assert_valid`` raises ``SchemaValidationError`` with a clear message
listing every missing or mismatched field.

----

Global control
--------------

Turn enforcement off
~~~~~~~~~~~~~~~~~~~~~

``disable()`` makes every ``@enforce`` wrapper and every function wrapped by
``arm()`` a straight pass-through. Nothing is checked.

.. code-block:: python

   from frameguard.pyspark import disable, enable_enforcement

   disable()
   process(completely_wrong_df)   # no error, runs normally

   enable_enforcement()
   process(completely_wrong_df)   # back to raising TypeError

The flag is global to the Python process. It affects everything, regardless
of when or where the functions were imported.

Environment variable
~~~~~~~~~~~~~~~~~~~~~

A clean way to disable enforcement without touching code:

.. code-block:: python

   # somewhere at startup, e.g. conftest.py or settings.py
   import os
   from frameguard.pyspark import disable

   if os.getenv("FRAMEGUARD_DISABLED"):
       disable()

Then in CI or non-production environments:

.. code-block:: bash

   FRAMEGUARD_DISABLED=1 kedro run
   FRAMEGUARD_DISABLED=1 pytest

In pytest
~~~~~~~~~

If you want most tests to run without schema enforcement (useful when you're
testing logic, not schemas), add this to ``conftest.py``:

.. code-block:: python

   import pytest
   from frameguard.pyspark import disable, enable_enforcement

   @pytest.fixture(autouse=True)
   def no_schema_enforcement():
       disable()
       yield
       enable_enforcement()

Tests that specifically test enforcement can call ``enable_enforcement()``
at the start, or just remove the ``autouse`` and opt in explicitly.
