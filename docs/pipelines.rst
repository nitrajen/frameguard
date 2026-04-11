Data Pipeline Usage
===================

frameguard works differently depending on your pipeline framework, because
Kedro and Airflow handle DataFrames differently. This page explains what to
reach for in each case.

Quick reference
---------------

+------------------------------------------+--------------------------------------------------+
| What you want                            | What to use                                      |
+==========================================+==================================================+
| Enforce one function                     | ``@fg.enforce``                                  |
+------------------------------------------+--------------------------------------------------+
| Enforce an entire package from one place | ``fg.arm()`` in ``__init__.py`` (Kedro: see      |
|                                          | :doc:`kedro`)                                    |
+------------------------------------------+--------------------------------------------------+
| Validate a DataFrame right after loading | ``MySchema.assert_valid(df)``                    |
+------------------------------------------+--------------------------------------------------+
| Turn off enforcement in tests or CI      | ``fg.disable()`` / ``fg.enable_enforcement()``   |
+------------------------------------------+--------------------------------------------------+

One thing to know before you start
------------------------------------

In Kedro, DataFrames come from the catalog at runtime. You cannot call
``fg.schema_of(raw_df)`` at module level because ``raw_df`` does not exist
when the module is imported. **Use fg.SparkSchema for all Kedro nodes.**

``fg.schema_of(df)`` is still useful in scripts and notebooks where you have a
live DataFrame in hand.

----

Kedro
-----

`Kedro <https://docs.kedro.org>`_ is a pipeline framework that structures projects
into nodes, pipelines, and a data catalog. See :doc:`kedro` for a full working
example with runnable code.

The recommended structure is: ``fg.SparkSchema`` for every node schema, defined
alongside the functions that use them. ``fg.arm()`` called once in ``settings.py``
wraps every annotated function in the package automatically, so node functions need
no ``@fg.enforce`` decorator.

Define schemas and nodes
~~~~~~~~~~~~~~~~~~~~~~~~~

Schemas live alongside the functions that use them.

.. code-block:: python

   # my_pipeline/nodes/raw_nodes.py  (enforced by fg.arm() in settings.py)
   import frameguard.pyspark as fg
   from pyspark.sql import functions as F, types as T


   class RawOrderSchema(fg.SparkSchema):
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
   import frameguard.pyspark as fg
   from pyspark.sql import functions as F, types as T
   from my_pipeline.nodes.raw_nodes import RawOrderSchema


   class EnrichedOrderSchema(RawOrderSchema):
       revenue: T.DoubleType()   # inherits all RawOrderSchema fields


   def enrich(df: RawOrderSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))


   def flag_high_value(df: EnrichedOrderSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

Arm everything from settings.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Kedro loads ``settings.py`` before any pipeline runs. One call there wraps
every annotated function in the package. Node files keep their schema
definitions and type annotations; those are the contract. What they do not
need is the ``@fg.enforce`` decorator on each function.

.. code-block:: python

   # my_pipeline/settings.py
   import frameguard.pyspark as fg

   fg.arm()

That single call walks every module in the package, finds every function
with a schema annotation, and wraps it. Functions without schema annotations
are left alone. Arguments that are not DataFrames (``str``, ``int``, etc.)
inside wrapped functions are also not touched.

Run with:

.. code-block:: bash

   kedro run

What you get
~~~~~~~~~~~~~

The wrong DataFrame raises before any Spark work happens:

.. code-block:: python

   # raw_df has order_id, amount, quantity, customer_id
   validate_orders(raw_df)       # OK

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

`Apache Airflow <https://airflow.apache.org>`_ tasks are Python callables. They
receive paths or config, create a SparkSession inside, do the work, and write
the result. DataFrames are never passed between tasks directly; only serializable
values are. See :doc:`airflow` for a full working example.

This means ``@fg.enforce`` on task functions doesn't buy you much (the args are
strings, not DataFrames). The useful tools are:

- ``MySchema.assert_valid(df)`` right after loading, which catches schema drift
  from upstream the moment data enters your task, not halfway through processing
- ``@fg.enforce`` on helper functions that the task calls internally

Validate after loading
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   # my_dag.py
   import frameguard.pyspark as fg
   from pyspark.sql import types as T


   class RawOrderSchema(fg.SparkSchema):
       order_id:    T.LongType()
       amount:      T.DoubleType()
       quantity:    T.IntegerType()
       customer_id: T.LongType()


   class EnrichedOrderSchema(RawOrderSchema):
       revenue: T.DoubleType()


   @fg.enforce
   def compute_revenue(df: RawOrderSchema):
       from pyspark.sql import functions as F
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))


   @fg.enforce
   def flag_vip(df: EnrichedOrderSchema):
       from pyspark.sql import functions as F
       return df.withColumn("is_vip", F.col("revenue") > 1000)


   def enrich_orders_task(input_path: str, output_path: str) -> str:
       from pyspark.sql import SparkSession
       spark = SparkSession.getOrCreate()

       df = spark.read.parquet(input_path)

       # Validate right after loading. If upstream changed the schema,
       # you find out here, not in a cryptic Spark error three steps later.
       RawOrderSchema.assert_valid(df)

       enriched = compute_revenue(df)   # @fg.enforce checks df's schema here
       flagged  = flag_vip(enriched)    # @fg.enforce checks enriched's schema

       flagged.write.mode("overwrite").parquet(output_path)
       return output_path

``assert_valid`` raises ``SchemaValidationError`` with a clear message
listing every missing or mismatched field.

Run with:

.. code-block:: bash

   airflow dags trigger orders_pipeline

Or trigger via the Airflow web UI at ``http://localhost:8080``.

----

Global control
--------------

Turn enforcement off
~~~~~~~~~~~~~~~~~~~~~

``fg.disable()`` makes every ``@fg.enforce`` wrapper and every function wrapped by
``fg.arm()`` a straight pass-through. Nothing is checked.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()
   wrong_df = spark.createDataFrame([(1, "Alice")], "user_id LONG, name STRING")

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   @fg.enforce
   def process(df: OrderSchema):
       return df

   fg.disable()
   process(wrong_df)        # no error, enforcement is off

   fg.enable_enforcement()
   process(wrong_df)        # raises TypeError again

The flag is global to the Python process. It affects everything, regardless
of when or where the functions were imported.

Environment variable
~~~~~~~~~~~~~~~~~~~~~

A clean way to disable enforcement without touching code:

.. code-block:: python

   # somewhere at startup, e.g. conftest.py or settings.py
   import os
   import frameguard.pyspark as fg

   if os.getenv("FRAMEGUARD_DISABLED"):
       fg.disable()

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
   import frameguard.pyspark as fg

   @pytest.fixture(autouse=True)
   def no_schema_enforcement():
       fg.disable()
       yield
       fg.enable_enforcement()

Tests that specifically test enforcement can call ``fg.enable_enforcement()``
at the start, or just remove the ``autouse`` and opt in explicitly.
