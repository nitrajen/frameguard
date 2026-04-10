Enforcement
===========

frameguard enforces schema annotations at the function call site, before
Spark plans anything. There are two ways to enable this, depending on
whether you are writing a package or a script.

Pick one. You do not need both.

----

frameguard.pyspark.arm
-----------------------

The preferred approach for packages (Kedro nodes, Airflow operators,
any module that gets *imported* rather than run directly). Call it once
from your entry point and every annotated function in the entire package
is enforced automatically.

.. code-block:: python

   # my_pipeline/settings.py
   import frameguard.pyspark as fg

   fg.arm()   # walks and arms the entire calling package

Node files stay clean — no imports, no decorators needed:

.. code-block:: python

   # my_pipeline/nodes.py
   import frameguard.pyspark as fg
   from pyspark.sql import functions as F, types as T

   class RawSchema(fg.SparkSchema):
       order_id: T.LongType()
       amount:   T.DoubleType()
       quantity: T.IntegerType()

   def enrich(df: RawSchema):         # enforced automatically
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   def clean(df: RawSchema):          # also enforced, no extra work
       return df.dropna()

``fg.arm()`` has no effect and emits a warning when called from
``__main__`` (a script run directly). Use ``@fg.enforce`` there instead.

.. autofunction:: frameguard.pyspark._enforcement.arm

----

frameguard.pyspark.enforce
---------------------------

A per-function decorator for scripts and notebooks.

Only intercepts parameters annotated with a ``fg.schema_of`` type or a
``fg.SparkSchema`` subclass. All other arguments (``str``, ``int``, ``list``,
custom classes) are left completely alone.

.. code-block:: python

   import frameguard.pyspark as fg
   from pyspark.sql import SparkSession, functions as F

   spark = SparkSession.builder.getOrCreate()
   raw_df = spark.createDataFrame(
       [(1, 10.0, 3), (2, 5.0, 7)],
       "order_id LONG, amount DOUBLE, quantity INT",
   )
   users_df = spark.createDataFrame(
       [(101, "Alice"), (102, "Bob")],
       "user_id LONG, name STRING",
   )

   RawSchema = fg.schema_of(raw_df)

   @fg.enforce
   def enrich(df: RawSchema, label: str, count: int):
       # only df is checked; label and count are not touched
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df, "production", 10)    # OK
   enrich(users_df, "production", 10)  # raises TypeError: wrong schema on df

.. autofunction:: frameguard.pyspark._enforcement.enforce
