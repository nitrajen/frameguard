Quickstart
==========

.. code-block:: bash

   pip install frameguard[pyspark]

Requires Python >= 3.10, PySpark >= 3.3.

.. note::

   frameguard currently supports **PySpark**. Support for additional DataFrame
   libraries (pandas, polars, and others) is planned.

All examples on this page assume the following setup:

.. code-block:: python

   import frameguard.pyspark as fg
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

fg.schema_of(df): exact snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Captures the schema of a real DataFrame and returns it as a Python type class.
Assign in PascalCase. It is a type, not a value.

.. code-block:: python

   # raw_df and enriched_df defined in setup above
   RawSchema      = fg.schema_of(raw_df)       # exact: same columns, same types, nothing extra
   EnrichedSchema = fg.schema_of(enriched_df)  # new type after adding revenue column

The isinstance check is **exact**: a DataFrame with extra columns does *not*
satisfy ``RawSchema``. Capture a new type at each stage boundary.

fg.SparkSchema: upfront declaration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declare the schema as a class. No live DataFrame needed.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, types as T
   from typing import Optional

   spark = SparkSession.builder.getOrCreate()

   # Nested struct: AddressSchema is used as a field type inside OrderSchema
   class AddressSchema(fg.SparkSchema):
       street: T.StringType()
       city:   T.StringType()
       zip:    Optional[T.StringType()]   # nullable field

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       address:  AddressSchema            # nested struct

   class EnrichedSchema(OrderSchema):    # inherits all OrderSchema fields
       revenue: T.DoubleType()

   # Create a DataFrame that matches OrderSchema (including the nested address struct)
   address_type = T.StructType([
       T.StructField("street", T.StringType()),
       T.StructField("city",   T.StringType()),
       T.StructField("zip",    T.StringType(), nullable=True),
   ])
   order_df = spark.createDataFrame(
       [(1, 99.0, ("123 Main St", "Austin", "78701"))],
       T.StructType([
           T.StructField("order_id", T.LongType()),
           T.StructField("amount",   T.DoubleType()),
           T.StructField("address",  address_type),
       ]),
   )

   OrderSchema.assert_valid(order_df)   # passes: all declared fields present

.. note::

   PySpark ``DataType`` instances (``T.StringType()``, ``T.LongType()``, etc.) do not
   support Python's ``X | None`` union syntax. Use ``Optional[T.XxxType()]`` for
   nullable primitive fields. ``NestedSchema | None`` works for nested SparkSchema
   subclasses because those are ordinary Python classes.

Choosing between the two
~~~~~~~~~~~~~~~~~~~~~~~~~

+---------------------+-------------------------------------+----------------------------------+
| ``fg.schema_of(df)``| Inferred from a live DataFrame      | Per-stage snapshots              |
+---------------------+-------------------------------------+----------------------------------+
| ``fg.SparkSchema``  | Declared upfront in code            | Shared contracts, Kedro nodes    |
+---------------------+-------------------------------------+----------------------------------+

.. note:: **IDE and mypy support**

   ``fg.SparkSchema`` subclasses are regular Python classes, so VSCode, PyRight,
   and mypy all understand them fully. Hover-over works, go-to-definition works,
   and ``isinstance`` checks are statically verified.

   ``fg.schema_of(df)`` types are created at runtime from a live DataFrame, so
   static tools see them as ``type[_TypedDatasetBase]`` rather than a named
   schema. The runtime enforcement still works perfectly, but mypy will not
   know the specific column names.

   **If IDE and mypy support matters to you, prefer ``fg.SparkSchema``.**
   Use ``fg.schema_of(df)`` when you want a quick runtime snapshot and static
   analysis is not a priority for that function.

Enforcement
-----------

There are two ways to add enforcement. Both accept the same ``subset`` parameter.

``@fg.enforce`` per function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Decorate individual functions. Use this in scripts, notebooks, and any place
where you want explicit, visible contracts.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
   )
   RawSchema = fg.schema_of(raw_df)

   @fg.enforce                    # subset=True by default
   def enrich(df: RawSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   @fg.enforce(subset=False)      # exact match required for this function
   def write_final(df: RawSchema): ...

``fg.arm()`` whole package
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call once from your package ``__init__.py`` or entry point. It walks the entire
package and wraps every annotated public function automatically. Node functions
need no ``@fg.enforce`` decorator; the annotation on the argument is enough.

.. code-block:: python

   # my_pipeline/__init__.py
   import frameguard.pyspark as fg

   fg.arm()                 # subset=True globally (default)
   # fg.arm(subset=False)   # exact match everywhere

.. code-block:: python

   # my_pipeline/nodes.py
   from pyspark.sql import types as T
   import frameguard.pyspark as fg

   class RawSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()

   def enrich(df: RawSchema):   # enforced by fg.arm(), no decorator needed
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   def clean(df: RawSchema):    # also enforced
       return df.dropDuplicates(["order_id"])

.. warning::

   ``fg.arm()`` has no effect when a module is run directly as a script
   (``python nodes.py``). It emits a warning in that case. Use ``@fg.enforce``
   in scripts and notebooks.

For pipeline framework integration see :doc:`kedro` and :doc:`airflow`.

The subset parameter
--------------------

``subset`` is available on both ``@fg.enforce`` and ``fg.arm()``. Default is ``True``.

- **subset=True**: all declared columns must be present with correct types; extra columns are fine.
- **subset=False**: declared columns must be present and no extras are allowed.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   # enriched_df has an extra 'revenue' column
   enriched_df = spark.createDataFrame(
       [(1, 99.0, 300.0)],
       "order_id LONG, amount DOUBLE, revenue DOUBLE",
   )

   @fg.enforce(subset=True)        # default: extra columns are fine
   def process(df: OrderSchema): return df

   @fg.enforce(subset=False)       # strict: no extra columns allowed
   def write_orders(df: OrderSchema): return df

   process(enriched_df)            # passes
   write_orders(enriched_df)       # raises: 'revenue' is not in OrderSchema
   # TypeError: Schema mismatch in write_orders() argument 'df':
   #   expected: order_id:bigint, amount:double
   #   received: order_id:bigint, amount:double, revenue:double

**Global default via fg.arm(), per-function override via @fg.enforce()**

``fg.arm(subset=...)`` sets the global default. ``@fg.enforce(subset=...)`` overrides
it for that function only. Function level always wins.

.. code-block:: python

   fg.arm(subset=False)            # global: strict everywhere

   @fg.enforce                     # inherits global subset=False
   def write_orders(df: OrderSchema): return df

   @fg.enforce(subset=True)        # overrides global for this function only
   def inspect(df: OrderSchema): return df

   write_orders(enriched_df)       # raises: global subset=False applies
   inspect(enriched_df)            # passes: function-level subset=True wins

``fg.schema_of(df)`` types always use exact matching regardless of ``subset``.
A snapshot is a snapshot.

+----------------------+---------------------------------------+------------------+
| ``subset=True``      | Declared columns present, extras OK   | Default          |
+----------------------+---------------------------------------+------------------+
| ``subset=False``     | Declared columns present, no extras   | Strict mode      |
+----------------------+---------------------------------------+------------------+
| ``schema_of`` types  | Always exact, names and types         | Ignores subset   |
+----------------------+---------------------------------------+------------------+

Multi-stage pipeline
--------------------

Each stage captures its output schema, which becomes the contract for the next stage.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3, 101), (2, 5.0, 7, 102)],
       "order_id LONG, amount DOUBLE, quantity INT, customer_id LONG",
   )

   # Stage 1
   RawSchema = fg.schema_of(raw_df)

   @fg.enforce
   def enrich(df: RawSchema):
       return (
           df
           .withColumn("revenue",  F.col("amount") * F.col("quantity"))
           .withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
       )

   enriched_df    = enrich(raw_df)
   EnrichedSchema = fg.schema_of(enriched_df)

   # Stage 2
   @fg.enforce
   def flag_high_value(df: EnrichedSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

   flagged_df    = flag_high_value(enriched_df)
   FlaggedSchema = fg.schema_of(flagged_df)

   # Stage 3: aggregation produces a completely different schema
   @fg.enforce
   def aggregate(df: FlaggedSchema):
       return df.groupBy("customer_id").agg(
           F.sum("revenue").alias("total_revenue"),
           F.count("*")   .alias("order_count"),
       )

   # Passing the wrong stage fails immediately:
   aggregate(raw_df)        # raises: missing revenue, is_vip
   aggregate(enriched_df)   # raises: missing is_vip
   aggregate(flagged_df)    # OK

Schema history
--------------

``fg.dataset(df)`` wraps a DataFrame and records every schema-changing operation.
When ``validate()`` fails, the error includes the full history.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, "Alice", 10.0, ["vip"], "home")],
       "order_id LONG, customer STRING, amount DOUBLE, tags ARRAY<STRING>, address STRING",
   )

   ds = fg.dataset(raw_df)
   ds = ds.withColumn("revenue",  F.col("amount") * 1.1)
   ds = ds.withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
   ds = ds.drop("tags", "address")
   ds = ds.withColumnRenamed("customer", "customer_name")

   print(ds.schema_history)
   # [0] input                    order_id:long, customer:string, amount:double, ...
   # [1] withColumn('revenue')    + revenue:double
   # [2] withColumn('discount')   + discount:double
   # [3] drop(['tags','address']) - tags, - address
   # [4] withColumnRenamed(...)   customer -> customer_name

SparkSchema utilities
---------------------

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, types as T

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
   )

   class OrderSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()

   # Create an empty DataFrame with the right schema (useful in tests)
   empty_ds = OrderSchema.empty(spark)

   # Build a SparkSchema class from a live StructType
   Discovered = fg.SparkSchema.from_struct(raw_df.schema, name="Discovered")

   # Generate copy-pasteable Python source for a schema
   Schema = fg.SparkSchema.from_struct(raw_df.schema, name="OrderSchema")
   print(Schema.to_code())
   # import frameguard.pyspark as fg
   # from pyspark.sql import types as T
   #
   # class OrderSchema(fg.SparkSchema):
   #     order_id: T.LongType()
   #     amount:   T.DoubleType()
   #     ...
