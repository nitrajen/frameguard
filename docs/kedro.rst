Using frameguard in Kedro
=========================

`Kedro <https://docs.kedro.org>`_ is a pipeline framework that structures data
projects into nodes, pipelines, and a data catalog. A node is a pure Python
function; Kedro wires inputs and outputs through the catalog.

frameguard fits naturally into Kedro. Schemas live in the node files alongside
the functions that use them. ``fg.arm()`` in ``settings.py`` arms the entire
package from one place, so node functions need no ``@fg.enforce`` decorator.

The working example lives at ``examples/kedro/`` in the frameguard repository.

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

Each node is a plain Python function with a schema-annotated argument.
No ``@fg.enforce`` decorator needed; ``fg.arm()`` in ``settings.py``
handles that for the whole package. No validation logic inside the function body.

.. code-block:: python

   # src/orders_pipeline/pipelines/processing/nodes.py
   from pyspark.sql import functions as F
   from orders_pipeline.schemas import EnrichedOrderSchema, RawOrderSchema

   # No @fg.enforce needed; fg.arm() in settings.py covers the whole package.
   # The type annotations on the arguments are the contract.

   def enrich_orders(raw: RawOrderSchema):
       return (
           raw
           .withColumn("revenue", F.col("amount") * F.col("quantity"))
           .withColumn("is_high_value", F.col("revenue") > 500.0)
       )

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

   # Run a specific pipeline
   kedro run --pipeline processing

   # Run a single node
   kedro run --node enrich_orders_node

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
appears at the first node that touches that column, not deep in a shuffle or
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

Using @fg.enforce instead of fg.arm()
--------------------------------------

If you prefer explicit decoration over package-wide arming, add ``@fg.enforce``
to individual node functions instead of calling ``fg.arm()`` in ``settings.py``.
Both approaches work; the choice is style:

.. code-block:: python

   # src/orders_pipeline/hooks.py
   import frameguard.pyspark as fg
   from kedro.framework.hooks import hook_impl

   class FrameguardHook:
       @hook_impl
       def before_pipeline_run(self, run_params, pipeline, catalog):
           fg.arm(package="orders_pipeline.pipelines")

.. code-block:: python

   # src/orders_pipeline/pipelines/processing/nodes.py  (with explicit decoration)
   import frameguard.pyspark as fg
   from pyspark.sql import functions as F
   from orders_pipeline.schemas import EnrichedOrderSchema, RawOrderSchema

   @fg.enforce
   def enrich_orders(raw: RawOrderSchema):
       return raw.withColumn("revenue", F.col("amount") * F.col("quantity"))

   @fg.enforce
   def summarise_by_customer(enriched: EnrichedOrderSchema):
       return enriched.groupBy("customer_id").agg(
           F.sum("revenue").alias("total_revenue"),
           F.count("*").alias("order_count"),
       )

+----------------------------------+--------------------------------------------+
| ``fg.arm()`` in ``settings.py``  | One call, whole package covered, no        |
|                                  | decorator on each function                 |
+----------------------------------+--------------------------------------------+
| ``@fg.enforce`` per function     | Explicit, visible at the function, easier  |
|                                  | to reason about in isolation               |
+----------------------------------+--------------------------------------------+
