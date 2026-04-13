Quickstart
==========

.. code-block:: bash

   pip install dfguard[pyspark]   # PySpark
   pip install dfguard[pandas]    # pandas
   pip install dfguard[polars]    # Polars
   pip install dfguard[all]       # all backends

Requires Python >= 3.10.

Interactive notebooks with end-to-end examples for each backend are available
on GitHub:

- `PySpark quickstart notebook <https://github.com/nitrajen/dfguard/blob/main/examples/notebooks/pyspark_quickstart.ipynb>`_
- `pandas quickstart notebook <https://github.com/nitrajen/dfguard/blob/main/examples/notebooks/pandas_quickstart.ipynb>`_
- `Polars quickstart notebook <https://github.com/nitrajen/dfguard/blob/main/examples/notebooks/polars_quickstart.ipynb>`_

Defining a schema type
-----------------------

There are two approaches. Choose based on whether you have a live DataFrame at
definition time.

dfg.schema_of(df): exact snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Captures the schema of a real DataFrame and returns it as a Python type class.
Assign in PascalCase. It is a type, not a value.

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
         enriched_df = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))

         RawSchema      = dfg.schema_of(raw_df)       # exact: same columns, same types, nothing extra
         EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding revenue column

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
         enriched_df = raw_df.assign(revenue=raw_df["amount"] * raw_df["quantity"])

         RawSchema      = dfg.schema_of(raw_df)       # exact snapshot
         EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding revenue column

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
         enriched_df = raw_df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

         RawSchema      = dfg.schema_of(raw_df)       # exact snapshot
         EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding revenue column

The isinstance check is **exact**: a DataFrame with extra columns does *not*
satisfy ``RawSchema``. Capture a new type at each stage boundary.

Upfront declaration
~~~~~~~~~~~~~~~~~~~~

Declare the schema as a class. No live DataFrame needed. Subclasses inherit
parent fields. All three backends support nested types fully.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from dfguard.pyspark import Optional
         from pyspark.sql import SparkSession, types as T

         spark = SparkSession.builder.getOrCreate()

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

         # Use the schema when creating a DataFrame
         order_df = spark.createDataFrame(
             [(1, 99.0, [("SKU-1", 2, 19.99)], "78701")],
             OrderSchema.to_struct(),
         )

         OrderSchema.assert_valid(order_df)   # passes: all declared fields present

      .. note::

         Use ``Optional`` from ``dfguard.pyspark``, not from ``typing``. PySpark
         ``DataType`` instances are not Python classes and ``typing.Optional``
         raises ``TypeError`` on Python 3.10 with non-class arguments.

         ``Optional`` marks the field ``nullable=True`` in the schema metadata.
         ``assert_valid`` checks that the nullable flag matches between schemas.
         Null values in the data are not checked: dfguard reads schema metadata only,
         not data.

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import pyarrow as pa
         import dfguard.pandas as dfg
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

      .. note::

         ``pd.ArrowDtype`` gives pandas full nested-type enforcement: arrays,
         structs, and maps at arbitrary depth, the same as PySpark and Polars.
         Without PyArrow, pandas dtype enforcement is limited to flat scalar types
         (``np.dtype``, ``pd.StringDtype``, etc.). Install with
         ``pip install dfguard[pandas] pyarrow``.

         Use ``Optional`` from ``dfguard.pandas`` to mark a field nullable in the
         schema declaration. Null values in the data are not checked: dfguard reads
         schema metadata only, not data.

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg
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

      .. note::

         Use ``Optional`` from ``dfguard.polars`` to mark a field nullable in the
         schema declaration. Null values in the data are not checked: dfguard reads
         schema metadata only, not data.

Choosing between the two
~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``dfg.schema_of(df)`` when you have a live DataFrame and want an exact
runtime snapshot of that stage. Use the class form when you want to declare the
contract upfront without needing a DataFrame: better for shared schemas, nodes,
and IDE support.

.. note:: **IDE navigation**

   The schema class is a regular Python class: go-to-definition works on the
   class name and its fields are visible in the class body. That is the extent
   of static tool support. The annotation ``df: RawSchema`` on a function
   parameter does not give you column-level autocomplete on ``df``, and mypy
   does not understand the column names or types. Schema enforcement is a
   runtime concern.

   ``dfg.schema_of(df)`` types are created at runtime, so static tools have
   no information about them at all.

Enforcement
-----------

There are two ways to add enforcement. Both accept the same ``subset`` parameter.

``@dfg.enforce`` per function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Decorate individual functions. Use this in scripts, notebooks, and any place
where you want explicit, visible contracts.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, functions as F, types as T

         spark = SparkSession.builder.getOrCreate()
         raw_df = spark.createDataFrame(
             [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
         )
         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce                    # subset=True by default
         def enrich(df: RawSchema):
             return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

         @dfg.enforce(subset=False)      # exact match required for this function
         def write_final(df: RawSchema): ...

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         raw_df = pd.DataFrame({
             "order_id": pd.array([1], dtype="int64"),
             "amount":   pd.array([10.0], dtype="float64"),
             "quantity": pd.array([3], dtype="int64"),
         })
         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce                    # subset=True by default
         def enrich(df: RawSchema):
             return df.assign(revenue=df["amount"] * df["quantity"])

         @dfg.enforce(subset=False)      # exact match required for this function
         def write_final(df: RawSchema): ...

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         raw_df = pl.DataFrame({
             "order_id": pl.Series([1], dtype=pl.Int64),
             "amount":   pl.Series([10.0], dtype=pl.Float64),
             "quantity": pl.Series([3], dtype=pl.Int32),
         })
         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce                    # subset=True by default
         def enrich(df: RawSchema) -> pl.DataFrame:
             return df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

         @dfg.enforce(subset=False)      # exact match required for this function
         def write_final(df: RawSchema) -> pl.DataFrame: ...

``dfg.arm()`` whole package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call once from your package ``__init__.py`` or entry point. It walks the entire
package and wraps every annotated public function automatically. No
``@dfg.enforce`` decorator needed on each function.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.pyspark as dfg

         dfg.arm()                 # subset=True globally (default)
         # dfg.arm(subset=False)   # exact match everywhere

      .. code-block:: python

         # my_pipeline/nodes.py
         from pyspark.sql import types as T
         import dfguard.pyspark as dfg

         class RawSchema(dfg.SparkSchema):
             order_id = T.LongType()
             amount   = T.DoubleType()
             quantity = T.IntegerType()

         def enrich(df: RawSchema):   # enforced by dfg.arm(), no decorator needed
             return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

         def clean(df: RawSchema):    # also enforced
             return df.dropDuplicates(["order_id"])

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.pandas as dfg

         dfg.arm()                 # subset=True globally (default)
         # dfg.arm(subset=False)   # exact match everywhere

      .. code-block:: python

         # my_pipeline/nodes.py
         import numpy as np
         import dfguard.pandas as dfg

         class RawSchema(dfg.PandasSchema):
             order_id = np.dtype("int64")
             amount   = np.dtype("float64")
             quantity = np.dtype("int64")

         def enrich(df: RawSchema):   # enforced by dfg.arm(), no decorator needed
             return df.assign(revenue=df["amount"] * df["quantity"])

         def clean(df: RawSchema):    # also enforced
             return df.drop_duplicates(subset=["order_id"])

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.polars as dfg

         dfg.arm()                 # subset=True globally (default)
         # dfg.arm(subset=False)   # exact match everywhere

      .. code-block:: python

         # my_pipeline/nodes.py
         import polars as pl
         import dfguard.polars as dfg

         class RawSchema(dfg.PolarsSchema):
             order_id = pl.Int64
             amount   = pl.Float64
             quantity = pl.Int32

         def enrich(df: RawSchema) -> pl.DataFrame:   # enforced by dfg.arm(), no decorator needed
             return df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

         def clean(df: RawSchema) -> pl.DataFrame:    # also enforced
             return df.unique(subset=["order_id"])

.. warning::

   ``dfg.arm()`` has no effect when a module is run directly as a script
   (``python nodes.py``).

   The reason: ``dfg.arm()`` works by walking Python's module registry. When you
   run a file as a script, Python loads it as ``__main__``, not under its package
   name. Use ``@dfg.enforce`` in scripts and notebooks.

For pipeline framework integration see :doc:`kedro` and :doc:`airflow`.

To turn off all enforcement globally, call ``dfg.disarm()``. See
`Disabling enforcement`_ below.

The subset parameter
--------------------

``subset`` is available on both ``@dfg.enforce`` and ``dfg.arm()``. Default is ``True``.

- **subset=True**: all declared columns must be present with correct types; extra columns are fine.
- **subset=False**: declared columns must be present and no extras are allowed.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, types as T

         spark = SparkSession.builder.getOrCreate()

         class OrderSchema(dfg.SparkSchema):
             order_id = T.LongType()
             amount   = T.DoubleType()

         # enriched_df has an extra 'revenue' column
         enriched_df = spark.createDataFrame(
             [(1, 99.0, 300.0)],
             "order_id LONG, amount DOUBLE, revenue DOUBLE",
         )

         @dfg.enforce(subset=True)        # default: extra columns are fine
         def process(df: OrderSchema): return df

         @dfg.enforce(subset=False)       # strict: no extra columns allowed
         def write_orders(df: OrderSchema): return df

         process(enriched_df)            # passes
         write_orders(enriched_df)       # raises: 'revenue' is not in OrderSchema
         # TypeError: Schema mismatch in write_orders() argument 'df':
         #   expected: order_id:bigint, amount:double
         #   received: order_id:bigint, amount:double, revenue:double

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         class OrderSchema(dfg.PandasSchema):
             order_id = np.dtype("int64")
             amount   = np.dtype("float64")

         # enriched_df has an extra 'revenue' column
         enriched_df = pd.DataFrame({
             "order_id": pd.array([1], dtype="int64"),
             "amount":   pd.array([99.0], dtype="float64"),
             "revenue":  pd.array([300.0], dtype="float64"),
         })

         @dfg.enforce(subset=True)        # default: extra columns are fine
         def process(df: OrderSchema): return df

         @dfg.enforce(subset=False)       # strict: no extra columns allowed
         def write_orders(df: OrderSchema): return df

         process(enriched_df)            # passes
         write_orders(enriched_df)       # raises: 'revenue' is not in OrderSchema
         # TypeError: Schema mismatch in write_orders() argument 'df':
         #   expected: order_id:int64, amount:float64
         #   received: order_id:int64, amount:float64, revenue:float64

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         class OrderSchema(dfg.PolarsSchema):
             order_id = pl.Int64
             amount   = pl.Float64

         # enriched_df has an extra 'revenue' column
         enriched_df = pl.DataFrame({
             "order_id": pl.Series([1], dtype=pl.Int64),
             "amount":   pl.Series([99.0], dtype=pl.Float64),
             "revenue":  pl.Series([300.0], dtype=pl.Float64),
         })

         @dfg.enforce(subset=True)        # default: extra columns are fine
         def process(df: OrderSchema) -> pl.DataFrame: return df

         @dfg.enforce(subset=False)       # strict: no extra columns allowed
         def write_orders(df: OrderSchema) -> pl.DataFrame: return df

         process(enriched_df)            # passes
         write_orders(enriched_df)       # raises: 'revenue' is not in OrderSchema
         # TypeError: Schema mismatch in write_orders() argument 'df':
         #   expected: order_id:Int64, amount:Float64
         #   received: order_id:Int64, amount:Float64, revenue:Float64

**Global default via dfg.arm(), per-function override via @dfg.enforce()**

``dfg.arm(subset=...)`` sets the global default. ``@dfg.enforce(subset=...)`` overrides
it for that function only. Function level always wins.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         dfg.arm(subset=False)            # global: strict everywhere

         @dfg.enforce                     # inherits global subset=False
         def write_orders(df: OrderSchema): return df

         @dfg.enforce(subset=True)        # overrides global for this function only
         def inspect(df: OrderSchema): return df

         write_orders(enriched_df)       # raises: global subset=False applies
         inspect(enriched_df)            # passes: function-level subset=True wins

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         dfg.arm(subset=False)            # global: strict everywhere

         @dfg.enforce                     # inherits global subset=False
         def write_orders(df: OrderSchema): return df

         @dfg.enforce(subset=True)        # overrides global for this function only
         def inspect(df: OrderSchema): return df

         write_orders(enriched_df)       # raises: global subset=False applies
         inspect(enriched_df)            # passes: function-level subset=True wins

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         dfg.arm(subset=False)            # global: strict everywhere

         @dfg.enforce                     # inherits global subset=False
         def write_orders(df: OrderSchema) -> pl.DataFrame: return df

         @dfg.enforce(subset=True)        # overrides global for this function only
         def inspect(df: OrderSchema) -> pl.DataFrame: return df

         write_orders(enriched_df)       # raises: global subset=False applies
         inspect(enriched_df)            # passes: function-level subset=True wins

``dfg.schema_of(df)`` types always use exact matching regardless of ``subset``.
A snapshot is a snapshot.

Multi-stage pipeline
--------------------

Each stage captures its output schema, which becomes the contract for the next stage.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, functions as F

         spark = SparkSession.builder.getOrCreate()
         raw_df = spark.createDataFrame(
             [(1, 10.0, 3, 101), (2, 5.0, 7, 102)],
             "order_id LONG, amount DOUBLE, quantity INT, customer_id LONG",
         )

         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce
         def enrich(df: RawSchema):
             return (
                 df
                 .withColumn("revenue",  F.col("amount") * F.col("quantity"))
                 .withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
             )

         enriched_df    = enrich(raw_df)
         EnrichedSchema = dfg.schema_of(enriched_df)

         @dfg.enforce
         def flag_high_value(df: EnrichedSchema):
             return df.withColumn("is_vip", F.col("revenue") > 1000)

         flagged_df    = flag_high_value(enriched_df)
         FlaggedSchema = dfg.schema_of(flagged_df)

         @dfg.enforce
         def aggregate(df: FlaggedSchema):
             return df.groupBy("customer_id").agg(
                 F.sum("revenue").alias("total_revenue"),
                 F.count("*")   .alias("order_count"),
             )

         aggregate(raw_df)        # raises: missing revenue, is_vip
         aggregate(enriched_df)   # raises: missing is_vip
         aggregate(flagged_df)    # passes

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         raw_df = pd.DataFrame({
             "order_id":   pd.array([1, 2], dtype="int64"),
             "amount":     pd.array([10.0, 5.0], dtype="float64"),
             "quantity":   pd.array([3, 7], dtype="int64"),
             "customer_id": pd.array([101, 102], dtype="int64"),
         })

         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce
         def enrich(df: RawSchema):
             df = df.assign(revenue=df["amount"] * df["quantity"])
             return df.assign(discount=df["revenue"].where(df["revenue"] <= 500, 50.0))

         enriched_df    = enrich(raw_df)
         EnrichedSchema = dfg.schema_of(enriched_df)

         @dfg.enforce
         def flag_high_value(df: EnrichedSchema):
             return df.assign(is_vip=df["revenue"] > 1000)

         flagged_df    = flag_high_value(enriched_df)
         FlaggedSchema = dfg.schema_of(flagged_df)

         @dfg.enforce
         def aggregate(df: FlaggedSchema):
             return df.groupby("customer_id").agg(
                 total_revenue=("revenue", "sum"),
                 order_count=("order_id", "count"),
             ).reset_index()

         aggregate(raw_df)        # raises: missing revenue, is_vip
         aggregate(enriched_df)   # raises: missing is_vip
         aggregate(flagged_df)    # passes

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         raw_df = pl.DataFrame({
             "order_id":    pl.Series([1, 2], dtype=pl.Int64),
             "amount":      pl.Series([10.0, 5.0], dtype=pl.Float64),
             "quantity":    pl.Series([3, 7], dtype=pl.Int32),
             "customer_id": pl.Series([101, 102], dtype=pl.Int64),
         })

         RawSchema = dfg.schema_of(raw_df)

         @dfg.enforce
         def enrich(df: RawSchema) -> pl.DataFrame:
             return df.with_columns(
                 revenue=pl.col("amount") * pl.col("quantity"),
             ).with_columns(
                 discount=pl.when(pl.col("revenue") > 500).then(50.0).otherwise(0.0),
             )

         enriched_df    = enrich(raw_df)
         EnrichedSchema = dfg.schema_of(enriched_df)

         @dfg.enforce
         def flag_high_value(df: EnrichedSchema) -> pl.DataFrame:
             return df.with_columns(is_vip=pl.col("revenue") > 1000)

         flagged_df    = flag_high_value(enriched_df)
         FlaggedSchema = dfg.schema_of(flagged_df)

         @dfg.enforce
         def aggregate(df: FlaggedSchema) -> pl.DataFrame:
             return df.group_by("customer_id").agg(
                 pl.sum("revenue").alias("total_revenue"),
                 pl.count("order_id").alias("order_count"),
             )

         aggregate(raw_df)        # raises: missing revenue, is_vip
         aggregate(enriched_df)   # raises: missing is_vip
         aggregate(flagged_df)    # passes

Validating at load time
-----------------------

``@dfg.enforce`` guards function boundaries. For storage boundaries (reading from
parquet, CSV, a data catalog) use ``Schema.assert_valid(df)`` immediately after
loading. This catches upstream schema drift before any processing starts.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         raw = spark.read.parquet("/data/orders/raw.parquet")
         RawOrderSchema.assert_valid(raw)   # raises SchemaValidationError if schema changed

         enriched = enrich(raw)             # @dfg.enforce then guards the function call

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         raw = pd.read_parquet("/data/orders/raw.parquet")
         RawOrderSchema.assert_valid(raw)   # raises SchemaValidationError if schema changed

         enriched = enrich(raw)             # @dfg.enforce then guards the function call

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         raw = pl.read_parquet("/data/orders/raw.parquet")
         RawOrderSchema.assert_valid(raw)   # raises SchemaValidationError if schema changed

         enriched = enrich(raw)             # @dfg.enforce then guards the function call

``assert_valid`` reports all problems at once, not just the first one:

.. code-block:: text

   SchemaValidationError: Schema validation failed:
     ✗ Column 'revenue': type mismatch: expected double, got string
     ✗ Missing column 'is_high_value' (expected boolean, nullable=False)

``validate()`` does the same check but returns a list of errors instead of raising,
useful when you want to inspect or log problems without stopping execution.

Disabling enforcement
---------------------

``dfg.arm()`` and ``dfg.disarm()`` are the global on/off switch for enforcement.
``arm()`` activates enforcement across your package. ``disarm()`` turns it off
entirely: every guarded function passes through without checking, regardless
of how it was armed or decorated.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         dfg.arm()               # enforcement active
         enrich(wrong_df)        # raises: schema mismatch
         dfg.disarm()
         enrich(wrong_df)        # passes: enforcement is off

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         dfg.arm()               # enforcement active
         enrich(wrong_df)        # raises: schema mismatch
         dfg.disarm()
         enrich(wrong_df)        # passes: enforcement is off

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         dfg.arm()               # enforcement active
         enrich(wrong_df)        # raises: schema mismatch
         dfg.disarm()
         enrich(wrong_df)        # passes: enforcement is off

This is useful in tests where you want to exercise transformation logic without
providing schema-valid fixtures.

Schema history
--------------

``dfg.dataset(df)`` wraps a DataFrame and records every schema-changing operation.
When ``validate()`` fails, the error includes the full history.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, functions as F

         spark = SparkSession.builder.getOrCreate()
         raw_df = spark.createDataFrame(
             [(1, "Alice", 10.0, ["vip"], "home")],
             "order_id LONG, customer STRING, amount DOUBLE, tags ARRAY<STRING>, address STRING",
         )

         ds = dfg.dataset(raw_df)
         ds = ds.withColumn("revenue",  F.col("amount") * 1.1)
         ds = ds.withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
         ds = ds.drop("tags", "address")
         ds = ds.withColumnRenamed("customer", "customer_name")

         ds.schema_history.print()
         # ────────────────────────────────────────────────────────────
         # Schema Evolution
         # ────────────────────────────────────────────────────────────
         #   [ 0] input
         #        struct<order_id:bigint,customer:string,amount:double,...>  (no schema change)
         #   [ 1] withColumn('revenue')
         #        added: revenue:double
         #   [ 2] withColumn('discount')
         #        added: discount:double
         #   [ 3] drop(['tags', 'address'])
         #        dropped: tags, address
         #   [ 4] withColumnRenamed('customer'→'customer_name')
         #        added: customer_name:string | dropped: customer
         # ────────────────────────────────────────────────────────────

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         raw_df = pd.DataFrame({
             "order_id":  pd.array([1], dtype="int64"),
             "customer":  pd.array(["Alice"], dtype=object),
             "amount":    pd.array([10.0], dtype="float64"),
         })

         ds = dfg.dataset(raw_df)
         ds = ds.assign(revenue=ds["amount"] * 1.1)
         ds = ds.assign(discount=ds["revenue"].where(ds["revenue"] <= 500, 50.0))
         ds = ds.rename(columns={"customer": "customer_name"})

         ds.schema_history.print()
         # ────────────────────────────────────────────────────────────
         # Schema Evolution
         # ────────────────────────────────────────────────────────────
         #   [ 0] input
         #        order_id:int64, customer:object, amount:float64  (no schema change)
         #   [ 1] assign('revenue')
         #        added: revenue:float64
         #   [ 2] assign('discount')
         #        added: discount:float64
         #   [ 3] rename({'customer'→'customer_name'})
         #        added: customer_name:object | dropped: customer
         # ────────────────────────────────────────────────────────────

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         raw_df = pl.DataFrame({
             "order_id":  pl.Series([1], dtype=pl.Int64),
             "customer":  pl.Series(["Alice"], dtype=pl.String),
             "amount":    pl.Series([10.0], dtype=pl.Float64),
         })

         ds = dfg.dataset(raw_df)
         ds = ds.with_columns(revenue=pl.col("amount") * 1.1)
         ds = ds.with_columns(discount=pl.when(pl.col("revenue") > 500).then(50.0).otherwise(0.0))
         ds = ds.drop("customer")
         ds = ds.rename({"customer": "customer_name"})

         ds.schema_history.print()
         # ────────────────────────────────────────────────────────────
         # Schema Evolution
         # ────────────────────────────────────────────────────────────
         #   [ 0] input
         #        order_id:Int64, customer:String, amount:Float64  (no schema change)
         #   [ 1] with_columns('revenue')
         #        added: revenue:Float64
         #   [ 2] with_columns('discount')
         #        added: discount:Float64
         #   [ 3] drop(['customer'])
         #        dropped: customer
         # ────────────────────────────────────────────────────────────

Schema utilities
-----------------

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         import dfguard.pyspark as dfg
         from pyspark.sql import SparkSession, types as T

         spark = SparkSession.builder.getOrCreate()
         raw_df = spark.createDataFrame(
             [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
         )

         class OrderSchema(dfg.SparkSchema):
             order_id = T.LongType()
             amount   = T.DoubleType()

         # Use the schema when creating a DataFrame
         df = spark.createDataFrame([(1, 10.0), (2, 5.0)], OrderSchema.to_struct())

         # Create an empty DataFrame with the right schema (useful in tests)
         empty_ds = OrderSchema.empty(spark)

         # Build a schema class from a live StructType
         Discovered = dfg.SparkSchema.from_struct(raw_df.schema, name="Discovered")

         # Generate copy-pasteable Python source for a schema
         Schema = dfg.SparkSchema.from_struct(raw_df.schema, name="OrderSchema")
         print(Schema.to_code())
         # import dfguard.pyspark as dfg
         # from pyspark.sql import types as T
         #
         # class OrderSchema(dfg.SparkSchema):
         #     order_id: T.LongType()
         #     amount:   T.DoubleType()
         #     ...

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg

         raw_df = pd.DataFrame({
             "order_id": pd.array([1], dtype="int64"),
             "amount":   pd.array([10.0], dtype="float64"),
         })

         class OrderSchema(dfg.PandasSchema):
             order_id = np.dtype("int64")
             amount   = np.dtype("float64")

         # Create an empty DataFrame with the right schema (useful in tests)
         empty_df = OrderSchema.empty()

         # Build a schema class from a dtype dict
         Discovered = dfg.PandasSchema.from_struct(dict(raw_df.dtypes), name="Discovered")

         # Generate copy-pasteable Python source for a schema
         Schema = dfg.PandasSchema.from_struct(dict(raw_df.dtypes), name="OrderSchema")
         print(Schema.to_code())
         # import numpy as np
         # import dfguard.pandas as dfg
         #
         # class OrderSchema(dfg.PandasSchema):
         #     order_id = np.dtype('int64')
         #     amount   = np.dtype('float64')

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg

         raw_df = pl.DataFrame({
             "order_id": pl.Series([1], dtype=pl.Int64),
             "amount":   pl.Series([10.0], dtype=pl.Float64),
         })

         class OrderSchema(dfg.PolarsSchema):
             order_id = pl.Int64
             amount   = pl.Float64

         # Create an empty DataFrame with the right schema (useful in tests)
         empty_df = OrderSchema.empty()

         # Build a schema class from a schema dict
         Discovered = dfg.PolarsSchema.from_struct(dict(raw_df.schema), name="Discovered")

         # Generate copy-pasteable Python source for a schema
         Schema = dfg.PolarsSchema.from_struct(dict(raw_df.schema), name="OrderSchema")
         print(Schema.to_code())
         # import polars as pl
         # import dfguard.polars as dfg
         #
         # class OrderSchema(dfg.PolarsSchema):
         #     order_id = pl.Int64
         #     amount   = pl.Float64
