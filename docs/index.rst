dfguard
==========

Data pipelines fail late. A DataFrame with the wrong schema enters a function
without complaint, the job runs, and the crash surfaces somewhere downstream
with an error that tells you nothing about where the mismatch started.

**dfguard moves that failure to the function call.** The wrong DataFrame is
rejected immediately with a precise error: which function, which argument, what
schema was expected, what arrived. **Lightweight**: enforcement is pure metadata
inspection: dfguard reads the schema struct from your DataFrame, no data is
scanned, no Spark jobs are triggered. Unlike `pandera <https://pandera.readthedocs.io/en/stable/>`_, which introduces its own
type system, dfguard uses the types your library already ships with:
``T.LongType()`` for PySpark, ``pl.Int64`` for Polars, ``np.dtype("int64")``
for pandas.

Compatibility
-------------

.. list-table::
   :header-rows: 1
   :widths: 30 25 25

   * - Backend
     - Version
     - Python
   * - PySpark
     - >= 3.3
     - >= 3.10
   * - pandas
     - >= 1.5
     - >= 3.10
   * - Polars
     - >= 0.20
     - >= 3.10

----

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, functions as F, types as T

         spark = SparkSession.builder.getOrCreate()
         raw_df = spark.createDataFrame(
             [(1, 10.0, 3), (2, 5.0, 7)],
             "order_id LONG, amount DOUBLE, quantity INT",
         )

         # Option A: arm() -- covers the whole package, no decorator on each function
         # Place dfg.arm() in my_pipeline/__init__.py
         dfg.arm()

         class RawSchema(dfg.SparkSchema):
             order_id = T.LongType()
             amount   = T.DoubleType()
             quantity = T.IntegerType()

         def enrich(df: RawSchema):                            # enforced by arm()
             return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

         # captures schema of the returned DataFrame
         EnrichedSchema = dfg.schema_of(enrich(raw_df))

         @dfg.enforce                                          # subset=True by default
         def flag_high_value(df: EnrichedSchema):
             return df.withColumn("is_vip", F.col("revenue") > 1000)

         flag_high_value(raw_df)
         # TypeError: Schema mismatch in flag_high_value() argument 'df':
         #   expected: order_id:bigint, amount:double, quantity:int, revenue:double
         #   received: order_id:bigint, amount:double, quantity:int

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         raw_df = pd.DataFrame({
             "order_id": pd.array([1, 2, 3], dtype="int64"),
             "amount":   pd.array([10.0, 5.0, 8.5], dtype="float64"),
             "quantity": pd.array([3, 1, 2], dtype="int64"),
         })

         # Option A: arm() -- covers the whole package, no decorator on each function
         dfg.arm()

         class RawSchema(dfg.PandasSchema):
             order_id = np.dtype("int64")
             amount   = np.dtype("float64")
             quantity = np.dtype("int64")

         def enrich(df: RawSchema):              # enforced by arm()
             return df.assign(revenue=df["amount"] * df["quantity"])

         # captures schema of the returned DataFrame
         EnrichedSchema = dfg.schema_of(enrich(raw_df))

         @dfg.enforce                            # subset=True by default
         def flag_high_value(df: EnrichedSchema):
             return df.assign(is_vip=df["revenue"] > 1000)

         flag_high_value(raw_df)
         # TypeError: Schema mismatch in flag_high_value() argument 'df':
         #   expected: order_id:int64, amount:float64, quantity:int64, revenue:float64
         #   received: order_id:int64, amount:float64, quantity:int64

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         raw_df = pl.DataFrame({
             "order_id": pl.Series([1, 2, 3], dtype=pl.Int64),
             "amount":   pl.Series([10.0, 5.0, 8.5], dtype=pl.Float64),
             "quantity": pl.Series([3, 1, 2], dtype=pl.Int32),
         })

         # Option A: arm() -- covers the whole package, no decorator on each function
         dfg.arm()

         class RawSchema(dfg.PolarsSchema):
             order_id = pl.Int64
             amount   = pl.Float64
             quantity = pl.Int32

         def enrich(df: RawSchema) -> pl.DataFrame:   # enforced by arm()
             return df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

         # captures schema of the returned DataFrame
         EnrichedSchema = dfg.schema_of(enrich(raw_df))

         @dfg.enforce                                  # subset=True by default
         def flag_high_value(df: EnrichedSchema) -> pl.DataFrame:
             return df.with_columns(is_vip=pl.col("revenue") > 1000)

         flag_high_value(raw_df)
         # TypeError: Schema mismatch in flag_high_value() argument 'df':
         #   expected: order_id:Int64, amount:Float64, quantity:Int32, revenue:Float64
         #   received: order_id:Int64, amount:Float64, quantity:Int32

No validation logic inside the functions.
The wrong DataFrame simply cannot enter the wrong function.

Call ``dfg.arm()`` once from your package ``__init__.py`` to protect the whole
package. No decorator needed on each function. See the :doc:`quickstart`.

Two ways to define a schema
----------------------------

**Option A: Capture from a live DataFrame**

.. code-block:: python

   RawSchema      = dfg.schema_of(raw_df)
   EnrichedSchema = dfg.schema_of(enriched_df)

Useful for quick scripts and existing code where you already have a DataFrame.
No boilerplate. The schema is locked to that DataFrame's shape at that moment.

**Option B: Declare upfront as a class**

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         from dfguard.pyspark import Optional

         class OrderSchema(dfg.SparkSchema):
             order_id   = T.LongType()
             amount     = T.DoubleType()
             line_items = T.ArrayType(T.StructType([       # array of structs
                 T.StructField("sku",      T.StringType()),
                 T.StructField("quantity", T.IntegerType()),
                 T.StructField("price",    T.DoubleType()),
             ]))
             zip_code   = Optional[T.StringType()]         # nullable field

         class EnrichedSchema(OrderSchema):                # inherits all parent fields
             revenue = T.DoubleType()

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import pyarrow as pa
         from dfguard.pandas import Optional

         class OrderSchema(dfg.PandasSchema):
             order_id   = np.dtype("int64")
             amount     = np.dtype("float64")
             line_items = pd.ArrowDtype(pa.list_(pa.struct([  # nested via PyArrow
                 pa.field("sku",      pa.string()),
                 pa.field("quantity", pa.int32()),
                 pa.field("price",    pa.float64()),
             ])))
             zip_code   = Optional[pd.StringDtype()]           # nullable field

         class EnrichedSchema(OrderSchema):                    # inherits all parent fields
             revenue = np.dtype("float64")

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         from dfguard.polars import Optional

         class OrderSchema(dfg.PolarsSchema):
             order_id   = pl.Int64
             amount     = pl.Float64
             line_items = pl.List(pl.Struct({               # list of structs
                 "sku":      pl.String,
                 "quantity": pl.Int32,
                 "price":    pl.Float64,
             }))
             zip_code   = Optional[pl.String]               # nullable field

         class EnrichedSchema(OrderSchema):                 # inherits all parent fields
             revenue = pl.Float64

No live DataFrame needed. Subclasses inherit parent fields. Supports complex
nested types. The schema class is a regular Python class: go-to-definition and
class-level navigation work in your IDE.

**For data pipelines, Option B is preferred.** Schemas are defined once,
shared across modules, visible in version control, and discoverable by your
IDE. Option A is convenient for exploration or when adding dfguard to existing
code you do not want to change.

See the :doc:`quickstart` for the full walkthrough.

.. toctree::
   :maxdepth: 1
   :caption: User Guide

   self
   quickstart
   types
   pipelines
   airflow
   kedro

.. toctree::
   :maxdepth: 1
   :caption: API Reference

   api/schemas
   api/enforcement
   api/dataset
   api/history
   api/exceptions
