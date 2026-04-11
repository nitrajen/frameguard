Using dfguard in Airflow
===========================

In Airflow, each task is a Python callable. Tasks receive file paths and
configuration, create their own SparkSession, do the work, and write results
back to storage. DataFrames never cross task boundaries; they are too large
for XCom.

This means ``@dfg.enforce`` on the *task function itself* gives you little:
the arguments are strings and integers, not DataFrames. The useful pattern is:

- ``@dfg.enforce`` on the transformation helpers the task calls internally
- ``Schema.assert_valid(df)`` right after loading from storage, to catch
  schema drift from upstream before processing starts

The working example lives at ``examples/airflow/`` in the dfguard repository.

File structure
--------------

.. code-block:: text

   airflow/
   ├── requirements.txt
   ├── pipeline/
   │   ├── __init__.py     # arms enforcement globally
   │   ├── schemas.py      # SparkSchema definitions
   │   └── transforms.py   # transformation helpers
   └── dags/
       └── orders_dag.py   # DAG and task callables

pipeline/__init__.py
--------------------

Arm once here. Every function in the package with a schema annotation is
enforced automatically -- no decorator needed on each one.

.. code-block:: python

   # pipeline/__init__.py
   import dfguard.pyspark as dfg

   dfg.arm()
   # dfg.arm(subset=False)  # strict: no extra columns anywhere in the package

   # To disable enforcement globally (e.g. in tests or non-prod environments):
   # dfg.disarm()

schemas.py
----------

Define schema contracts once, shared by all tasks.

.. code-block:: python

   # pipeline/schemas.py
   import dfguard.pyspark as dfg
   from pyspark.sql import types as T

   class RawOrderSchema(dfg.SparkSchema):
       order_id:    T.LongType()
       customer_id: T.LongType()
       amount:      T.DoubleType()
       quantity:    T.IntegerType()
       status:      T.StringType()

   class EnrichedOrderSchema(RawOrderSchema):
       revenue:       T.DoubleType()
       is_high_value: T.BooleanType()

   class SummarySchema(dfg.SparkSchema):
       customer_id:   T.LongType()
       total_revenue: T.DoubleType()
       order_count:   T.LongType()

transforms.py
-------------

Pure transformation functions. ``dfg.arm()`` in ``__init__.py`` covers them
all. ``@dfg.enforce(subset=False)`` is added where an exact schema is required
-- no extra columns allowed, useful before writing to a fixed-schema sink.

.. code-block:: python

   # pipeline/transforms.py
   import dfguard.pyspark as dfg
   from pyspark.sql import DataFrame, functions as F
   from pipeline.schemas import EnrichedOrderSchema, RawOrderSchema, SummarySchema

   # Covered by dfg.arm() -- no decorator needed
   def enrich(raw: RawOrderSchema) -> DataFrame:
       return (
           raw
           .withColumn("revenue", F.col("amount") * F.col("quantity"))
           .withColumn("is_high_value", F.col("revenue") > 500.0)
       )

   # subset=False: the summary written to storage must match SummarySchema exactly
   @dfg.enforce(subset=False)
   def summarise(enriched: EnrichedOrderSchema) -> DataFrame:
       return (
           enriched
           .groupBy("customer_id")
           .agg(
               F.sum("revenue").alias("total_revenue"),
               F.count("*").alias("order_count"),
           )
       )

orders_dag.py
-------------

Each task validates the DataFrame immediately after loading, then calls the
appropriate transform. Tasks are completely independent; each creates and
stops its own SparkSession.

.. code-block:: python

   # dags/orders_dag.py
   import sys, os
   from datetime import datetime
   from airflow import DAG
   from airflow.operators.python import PythonOperator

   sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

   INPUT_PATH   = "/data/orders/raw_orders.parquet"
   ENRICH_PATH  = "/data/orders/enriched_orders.parquet"
   SUMMARY_PATH = "/data/orders/customer_summary.parquet"

   def task_enrich(**context):
       from pyspark.sql import SparkSession
       from pipeline.schemas import RawOrderSchema
       from pipeline.transforms import enrich

       spark = SparkSession.builder.appName("orders-enrich").getOrCreate()

       raw = spark.read.parquet(INPUT_PATH)
       RawOrderSchema.assert_valid(raw)   # fail fast if upstream schema changed

       enriched = enrich(raw)             # @dfg.enforce guards the function
       enriched.write.mode("overwrite").parquet(ENRICH_PATH)
       spark.stop()

   def task_summarise(**context):
       from pyspark.sql import SparkSession
       from pipeline.schemas import EnrichedOrderSchema
       from pipeline.transforms import summarise

       spark = SparkSession.builder.appName("orders-summarise").getOrCreate()

       enriched = spark.read.parquet(ENRICH_PATH)
       EnrichedOrderSchema.assert_valid(enriched)

       summary = summarise(enriched)
       summary.write.mode("overwrite").parquet(SUMMARY_PATH)
       spark.stop()

   with DAG(
       dag_id="orders_pipeline",
       start_date=datetime(2024, 1, 1),
       schedule="@daily",
       catchup=False,
   ) as dag:

       enrich_task   = PythonOperator(task_id="enrich_orders",   python_callable=task_enrich)
       summarise_task = PythonOperator(task_id="summarise_orders", python_callable=task_summarise)

       enrich_task >> summarise_task

Running locally
---------------

.. code-block:: bash

   pip install -r examples/airflow/requirements.txt
   export AIRFLOW_HOME=~/airflow
   airflow db migrate
   airflow dags trigger orders_pipeline

What a schema error looks like
-------------------------------

If the enrich task is skipped and ``task_summarise`` runs directly on raw
data, it fails at the function call:

.. code-block:: text

   TypeError: Schema mismatch in summarise() argument 'enriched':
     expected: order_id:bigint, customer_id:bigint, amount:double,
               quantity:int, status:string, revenue:double, is_high_value:boolean
     received: order_id:bigint, customer_id:bigint, amount:double,
               quantity:int, status:string

The error is raised at the function call. The message tells you which function,
which argument, what was expected, and what was actually passed.

If upstream data changes shape and ``assert_valid`` catches it:

.. code-block:: text

   SchemaValidationError: Schema validation failed:
     ✗ Missing column 'revenue' (expected double, nullable=False)

``assert_valid`` reports all missing and mismatched fields in one message,
not just the first one it finds.

.. tip::

   Use ``@dfg.enforce`` on every transformation helper. Use
   ``Schema.assert_valid(df)`` at the start of every task that reads from
   storage. Together they give you two layers: one at the storage boundary
   (schema drift from upstream), one at the function boundary (wrong data
   passed to wrong transform).
