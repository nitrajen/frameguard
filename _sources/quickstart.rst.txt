Quickstart
==========

.. code-block:: bash

   pip install frameguard[pyspark]

Requires Python >= 3.10, PySpark >= 3.3.

All examples on this page assume the following setup:

.. code-block:: python

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

schema_of(df): exact snapshot
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Captures the schema of a real DataFrame and returns it as a Python type class.
Assign in PascalCase. It is a type, not a value.

.. code-block:: python

   from frameguard.pyspark import schema_of

   # raw_df and enriched_df defined in setup above
   RawSchema      = schema_of(raw_df)       # exact: same columns, same types, nothing extra
   EnrichedSchema = schema_of(enriched_df)  # new type after adding revenue column

The isinstance check is **exact**: a DataFrame with extra columns does *not*
satisfy ``RawSchema``. Capture a new type at each stage boundary.

SparkSchema: upfront declaration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declare the schema as a class. No live DataFrame needed.
The isinstance check is **structural**: the DataFrame must have *at least* these
fields, extra columns are fine.

.. code-block:: python

   from pyspark.sql import types as T
   from typing import Optional
   from frameguard.pyspark import SparkSchema

   class AddressSchema(SparkSchema):
       street: T.StringType()
       city:   T.StringType()
       zip:    Optional[T.StringType()]   # nullable: use Optional, not X | None

   class OrderSchema(SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       address:  AddressSchema            # nested struct

   class EnrichedSchema(OrderSchema):    # inherits all OrderSchema fields
       revenue: T.DoubleType()

.. note::

   PySpark ``DataType`` instances (``T.StringType()``, ``T.LongType()``, etc.) do not
   support Python's ``X | None`` union syntax. Use ``Optional[T.XxxType()]`` for
   nullable primitive fields. ``NestedSchema | None`` works for nested SparkSchema
   subclasses because those are ordinary Python classes.

Choosing between the two
~~~~~~~~~~~~~~~~~~~~~~~~~

+-------------------+-------------------+------------------------------------------+
| ``schema_of(df)`` | Exact matching    | Per-stage snapshots in a pipeline        |
+-------------------+-------------------+------------------------------------------+
| ``SparkSchema``   | Subset matching   | Shared contracts, Kedro nodes, libraries |
+-------------------+-------------------+------------------------------------------+

.. note:: **IDE and mypy support**

   ``SparkSchema`` subclasses are regular Python classes, so VSCode, PyRight,
   and mypy all understand them fully. Hover-over works, go-to-definition works,
   and ``isinstance`` checks are statically verified.

   ``schema_of(df)`` types are created at runtime from a live DataFrame, so
   static tools see them as ``type[_TypedDatasetBase]`` rather than a named
   schema. The runtime enforcement still works perfectly, but mypy will not
   know the specific column names.

   **If IDE and mypy support matters to you, prefer ``SparkSchema``.**
   Use ``schema_of(df)`` when you want a quick runtime snapshot and static
   analysis is not a priority for that function.

Enforcement
-----------

Packages (Kedro, Airflow, any importable module)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call ``arm()`` after all function definitions in the module. Every annotated
function is enforced automatically, no per-function decoration.

.. code-block:: python

   # my_pipeline/nodes.py
   from frameguard.pyspark import schema_of, arm

   # raw_df comes from the catalog or a load step at runtime
   RawSchema = schema_of(raw_df)

   def enrich(df: RawSchema):   # enforced
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   def clean(df: RawSchema):    # enforced, no extra work
       return df.dropDuplicates(["order_id"])

   arm()   # call after all function definitions

Scripts and notebooks
~~~~~~~~~~~~~~~~~~~~~

Use ``@enforce`` per function.

.. code-block:: python

   from frameguard.pyspark import schema_of, enforce

   RawSchema = schema_of(raw_df)

   @enforce
   def enrich(df: RawSchema):
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

.. warning::

   ``arm()`` has no effect when a module is run directly as a script
   (``python nodes.py``). It emits a warning in that case. Use ``@enforce`` in
   scripts and notebooks.

Multi-stage pipeline
--------------------

Each stage captures its output schema, which becomes the contract for the next stage.

.. code-block:: python

   from frameguard.pyspark import schema_of, enforce

   # Stage 1
   RawSchema = schema_of(raw_df)

   @enforce
   def enrich(df: RawSchema):
       return (
           df
           .withColumn("revenue",  F.col("amount") * F.col("quantity"))
           .withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
       )

   enriched_df    = enrich(raw_df)
   EnrichedSchema = schema_of(enriched_df)

   # Stage 2
   @enforce
   def flag_high_value(df: EnrichedSchema):
       return df.withColumn("is_vip", F.col("revenue") > 1000)

   flagged_df    = flag_high_value(enriched_df)
   FlaggedSchema = schema_of(flagged_df)

   # Stage 3: aggregation produces a completely different schema
   @enforce
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

``dataset(df)`` wraps a DataFrame and records every schema-changing operation.
When ``validate()`` fails, the error includes the full history.

.. code-block:: python

   from frameguard.pyspark import dataset

   ds = dataset(raw_df)
   ds = ds.withColumn("revenue",  F.col("amount") * F.col("quantity"))
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

   # Create an empty DataFrame with the right schema (useful in tests)
   empty_ds = OrderSchema.empty(spark)

   # Build a SparkSchema class from a live StructType
   Discovered = SparkSchema.from_struct(df.schema, name="Discovered")

   # Generate copy-pasteable Python source for a schema
   Schema = SparkSchema.from_struct(df.schema, name="OrderSchema")
   print(Schema.to_code())
   # from pyspark.sql import types as T
   # from typing import Optional
   #
   # class OrderSchema(SparkSchema):
   #     order_id: T.LongType()
   #     amount:   T.DoubleType()
   #     ...
