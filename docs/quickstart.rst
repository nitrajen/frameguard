Quickstart
==========

.. code-block:: bash

   pip install dfguard[pyspark]

Requires Python >= 3.10, PySpark >= 3.3.

.. note::

   dfguard currently supports **PySpark**. Support for additional DataFrame
   libraries (pandas, polars, and others) is planned.

All examples on this page assume the following setup:

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F
   from pyspark.sql import types as T

   spark = SparkSession.builder.getOrCreate()

   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )
   enriched_df = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))

Defining a schema type
-----------------------

There are two approaches. Choose based on whether you have a live DataFrame at
definition time.

dfg.schema_of(df): exact snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Captures the schema of a real DataFrame and returns it as a Python type class.
Assign in PascalCase. It is a type, not a value.

.. code-block:: python

   # raw_df and enriched_df defined in setup above
   RawSchema      = dfg.schema_of(raw_df)       # exact: same columns, same types, nothing extra
   EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding revenue column

The isinstance check is **exact**: a DataFrame with extra columns does *not*
satisfy ``RawSchema``. Capture a new type at each stage boundary.

dfg.SparkSchema: upfront declaration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declare the schema as a class. No live DataFrame needed.

.. code-block:: python

   import dfguard.pyspark as dfg
   from dfguard.pyspark import Optional
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()

   # Nested struct: AddressSchema is used as a field type inside OrderSchema
   class AddressSchema(dfg.SparkSchema):
       street: T.StringType()
       city:   T.StringType()
       zip:    Optional[T.StringType()]   # nullable field

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       address:  AddressSchema            # nested struct

   class EnrichedSchema(OrderSchema):    # inherits all OrderSchema fields
       revenue: T.DoubleType()

   # Use the schema directly when creating a DataFrame (no manual StructType needed)
   order_df = spark.createDataFrame(
       [(1, 99.0, ("123 Main St", "Austin", "78701"))],
       OrderSchema.to_struct(),
   )

   OrderSchema.assert_valid(order_df)   # passes: all declared fields present

.. note::

   For nullable fields, use ``Optional`` imported from ``dfguard.pyspark``, not from
   ``typing``. PySpark ``DataType`` instances are not Python classes, and ``typing.Optional``
   raises ``TypeError`` on Python 3.10 with non-class arguments. ``dfguard.pyspark.Optional``
   is a drop-in replacement that works on all supported Python versions.

Choosing between the two
~~~~~~~~~~~~~~~~~~~~~~~~~

Use ``dfg.schema_of(df)`` when you have a live DataFrame and want an exact runtime snapshot of that stage. Use ``dfg.SparkSchema`` when you want to declare the contract upfront without needing a DataFrame -- better for shared schemas, Kedro nodes, and IDE support.

.. note:: **IDE and mypy support**

   ``dfg.SparkSchema`` subclasses are regular Python classes, so VSCode, PyRight,
   and mypy all understand them fully. Hover-over works, go-to-definition works,
   and ``isinstance`` checks are statically verified.

   ``dfg.schema_of(df)`` types are created at runtime from a live DataFrame, so
   static tools see them as ``type[_TypedDatasetBase]`` rather than a named
   schema. The runtime enforcement still works perfectly, but mypy will not
   know the specific column names.

   **If IDE and mypy support matters to you, prefer ``dfg.SparkSchema``.**
   Use ``dfg.schema_of(df)`` when you want a quick runtime snapshot and static
   analysis is not a priority for that function.

Enforcement
-----------

There are two ways to add enforcement. Both accept the same ``subset`` parameter.

``@dfg.enforce`` per function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Decorate individual functions. Use this in scripts, notebooks, and any place
where you want explicit, visible contracts.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F

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

``dfg.arm()`` whole package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call once from your package ``__init__.py`` or entry point. It walks the entire
package and wraps every annotated public function automatically. Functions with
schema-annotated arguments are enforced automatically -- no ``@dfg.enforce``
decorator needed on each one.

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
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()

   def enrich(df: RawSchema):   # enforced by dfg.arm(), no decorator needed
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   def clean(df: RawSchema):    # also enforced
       return df.dropDuplicates(["order_id"])

.. warning::

   ``dfg.arm()`` has no effect when a module is run directly as a script
   (``python nodes.py``).

   The reason: ``dfg.arm()`` works by walking Python's module registry. When you
   run a file as a script, Python loads it as ``__main__``, not under its package
   name (e.g. ``my_pipeline.nodes``). So ``dfg.arm(package="my_pipeline")`` finds
   no modules to wrap. ``@dfg.enforce`` works in scripts because it is applied
   directly to the function at definition time, before any module name matters.

   Use ``@dfg.enforce`` in scripts and notebooks.

For pipeline framework integration see :doc:`kedro` and :doc:`airflow`.

To turn off all enforcement globally -- for example in tests or non-production
environments -- call ``dfg.disarm()``. This silences every guarded function,
whether wrapped by ``dfg.arm()`` or decorated with ``@dfg.enforce``. Call
``dfg.arm()`` again to re-enable. See `Disabling enforcement`_ below.

The subset parameter
--------------------

``subset`` is available on both ``@dfg.enforce`` and ``dfg.arm()``. Default is ``True``.

- **subset=True**: all declared columns must be present with correct types; extra columns are fine.
- **subset=False**: declared columns must be present and no extras are allowed.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

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

**Global default via dfg.arm(), per-function override via @dfg.enforce()**

``dfg.arm(subset=...)`` sets the global default. ``@dfg.enforce(subset=...)`` overrides
it for that function only. Function level always wins.

.. code-block:: python

   dfg.arm(subset=False)            # global: strict everywhere

   @dfg.enforce                     # inherits global subset=False
   def write_orders(df: OrderSchema): return df

   @dfg.enforce(subset=True)        # overrides global for this function only
   def inspect(df: OrderSchema): return df

   write_orders(enriched_df)       # raises: global subset=False applies
   inspect(enriched_df)            # passes: function-level subset=True wins

``dfg.schema_of(df)`` types always use exact matching regardless of ``subset``.
A snapshot is a snapshot.

Multi-stage pipeline
--------------------

Each stage captures its output schema, which becomes the contract for the next stage.

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3, 101), (2, 5.0, 7, 102)],
       "order_id LONG, amount DOUBLE, quantity INT, customer_id LONG",
   )

   # Stage 1
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

   # Stage 2
   @dfg.enforce
   def flag_high_value(df: EnrichedSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

   flagged_df    = flag_high_value(enriched_df)
   FlaggedSchema = dfg.schema_of(flagged_df)

   # Stage 3: aggregation produces a completely different schema
   @dfg.enforce
   def aggregate(df: FlaggedSchema):
       return df.groupBy("customer_id").agg(
           F.sum("revenue").alias("total_revenue"),
           F.count("*")   .alias("order_count"),
       )

   # Passing the wrong stage fails immediately:
   aggregate(raw_df)        # raises: missing revenue, is_vip
   aggregate(enriched_df)   # raises: missing is_vip
   aggregate(flagged_df)    # OK

Validating at load time
-----------------------

``@dfg.enforce`` guards function boundaries. For storage boundaries (reading from
parquet, CSV, a data catalog) use ``Schema.assert_valid(df)`` immediately after
loading. This catches upstream schema drift before any processing starts.

.. code-block:: python

   raw = spark.read.parquet("/data/orders/raw.parquet")
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
entirely -- every guarded function passes through without checking, regardless
of how it was armed or decorated.

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

SparkSchema utilities
---------------------

.. code-block:: python

   import dfguard.pyspark as dfg
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
   )

   class OrderSchema(dfg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   # Use the schema when creating a DataFrame
   df = spark.createDataFrame(
       [(1, 10.0), (2, 5.0)],
       OrderSchema.to_struct(),
   )

   # Create an empty DataFrame with the right schema (useful in tests)
   empty_ds = OrderSchema.empty(spark)

   # Build a SparkSchema class from a live StructType
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
