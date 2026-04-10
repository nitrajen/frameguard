Using frameguard in Kedro
=========================

Kedro is a pipeline framework that structures data projects into nodes,
pipelines, and a data catalog. A node is a pure Python function; Kedro
wires inputs and outputs through the catalog.

frameguard fits naturally: annotate node functions with ``@fg.enforce`` and
schema errors surface at the node boundary, before any Spark execution. The
working example lives at ``examples/kedro/`` in the frameguard repository.

File structure
--------------

.. code-block:: text

   kedro/
   ├── requirements.txt
   ├── pyproject.toml
   ├── conf/base/
   │   └── catalog.yml
   ├── data/
   │   └── raw_orders.csv
   └── src/orders_pipeline/
       ├── __init__.py
       ├── pipeline_registry.py
       ├── schemas.py                        # shared SparkSchema definitions
       └── pipelines/processing/
           ├── __init__.py
           ├── nodes.py                      # node functions with @fg.enforce
           └── pipeline.py                   # pipeline assembly

schemas.py
----------

Define schema contracts in one file and import them wherever needed. This
is the single source of truth for what each stage of the pipeline produces
and consumes.

.. code-block:: python

   # src/orders_pipeline/schemas.py
   import frameguard.pyspark as fg
   from pyspark.sql import types as T

   class RawOrderSchema(fg.SparkSchema):
       order_id:    T.LongType()
       customer_id: T.LongType()
       amount:      T.DoubleType()
       quantity:    T.IntegerType()
       status:      T.StringType()

   class EnrichedOrderSchema(RawOrderSchema):
       revenue:       T.DoubleType()
       is_high_value: T.BooleanType()

nodes.py
--------

Each node is a plain Python function. ``@fg.enforce`` is the only addition —
no validation logic inside the function body.

.. code-block:: python

   # src/orders_pipeline/pipelines/processing/nodes.py
   import frameguard.pyspark as fg
   from pyspark.sql import functions as F
   from orders_pipeline.schemas import EnrichedOrderSchema, RawOrderSchema

   @fg.enforce
   def enrich_orders(raw: RawOrderSchema):
       return (
           raw
           .withColumn("revenue", F.col("amount") * F.col("quantity"))
           .withColumn("is_high_value", F.col("revenue") > 500.0)
       )

   @fg.enforce
   def summarise_by_customer(enriched: EnrichedOrderSchema):
       return (
           enriched
           .groupBy("customer_id")
           .agg(
               F.sum("revenue").alias("total_revenue"),
               F.count("*").alias("order_count"),
           )
       )

pipeline.py
-----------

.. code-block:: python

   # src/orders_pipeline/pipelines/processing/pipeline.py
   from kedro.pipeline import Pipeline, node, pipeline
   from .nodes import enrich_orders, summarise_by_customer

   def create_pipeline(**kwargs) -> Pipeline:
       return pipeline([
           node(
               func=enrich_orders,
               inputs="raw_orders",
               outputs="enriched_orders",
               name="enrich_orders_node",
           ),
           node(
               func=summarise_by_customer,
               inputs="enriched_orders",
               outputs="customer_summary",
               name="summarise_node",
           ),
       ])

catalog.yml
-----------

.. code-block:: yaml

   # conf/base/catalog.yml
   raw_orders:
     type: spark.SparkDataset
     filepath: data/raw_orders.csv
     file_format: csv
     load_args:
       header: true
       inferSchema: true

   enriched_orders:
     type: spark.SparkDataset
     filepath: data/enriched_orders.parquet
     file_format: parquet

   customer_summary:
     type: spark.SparkDataset
     filepath: data/customer_summary.parquet
     file_format: parquet

Running the pipeline
--------------------

.. code-block:: bash

   cd examples/kedro
   pip install -r requirements.txt
   pip install -e src/
   kedro run

What a schema error looks like
-------------------------------

If the catalog wires ``raw_orders`` directly into ``summarise_by_customer``
(missing the enrich step), Kedro propagates the node failure with the full
frameguard message:

.. code-block:: text

   TypeError: Schema mismatch in summarise_by_customer() argument 'enriched':
     expected: order_id:bigint, customer_id:bigint, amount:double,
               quantity:int, status:string, revenue:double, is_high_value:boolean
     received: order_id:bigint, customer_id:bigint, amount:double,
               quantity:int, status:string

If the CSV source adds an unexpected column or changes a type, the error
appears at the first node that touches that column — not deep in a shuffle or
aggregation.

Where to define schemas
-----------------------

Put them in ``schemas.py`` at the package root. Node files import from there.
This keeps schema definitions separate from transformation logic and gives you
one place to update when the upstream contract changes.

For very large projects with many pipelines, a ``schemas/`` directory with
one file per domain works well:

.. code-block:: text

   src/orders_pipeline/schemas/
   ├── __init__.py   # re-exports all schema classes
   ├── orders.py
   ├── customers.py
   └── products.py

Using fg.arm() instead of @fg.enforce
--------------------------------------

For projects where you want enforcement on every function without adding
``@fg.enforce`` individually, call ``fg.arm()`` at startup instead. A Kedro
hook is the right place for this:

.. code-block:: python

   # src/orders_pipeline/hooks.py
   import frameguard.pyspark as fg
   from kedro.framework.hooks import hook_impl

   class FrameguardHook:
       @hook_impl
       def before_pipeline_run(self, run_params, pipeline, catalog):
           fg.arm(package="orders_pipeline.pipelines")

Then register it in ``settings.py``:

.. code-block:: python

   # src/orders_pipeline/settings.py
   from orders_pipeline.hooks import FrameguardHook

   HOOKS = (FrameguardHook(),)

With this setup, every public function in ``orders_pipeline.pipelines`` is
automatically wrapped. Node functions need no decorators at all.

.. note::

   ``@fg.enforce`` on individual nodes is more explicit and easier to reason
   about. ``fg.arm()`` via hooks is better for large projects where you want
   blanket coverage without touching every file.
