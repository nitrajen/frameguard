Enforcement
===========

frameguard enforces schema annotations at the function call site, before
Spark plans anything. There are two ways to enable this, depending on
whether you are writing a package or a script.

Pick one. You do not need both.

----

arm
---

The preferred approach for packages (Kedro nodes, Airflow operators,
any module that gets *imported* rather than run directly). Call it once
**after** all function definitions in the module. Every annotated function
with a ``schema_of`` type or ``SparkSchema`` annotation is enforced
automatically, with no decorator on each function.

.. code-block:: python

   # my_pipeline/transforms.py
   from frameguard.pyspark import schema_of, arm

   RawSchema = schema_of(raw_df)

   def enrich(df: RawSchema):         # enforced automatically
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   def clean(df: RawSchema):          # also enforced, no extra work
       return df.dropna()

   arm()   # call after all function definitions

``arm()`` wraps every public function in the calling module's globals with
``@enforce``. It has no effect and emits a warning when called from
``__main__`` (a script run directly). Use ``@enforce`` there instead.

.. autofunction:: frameguard.pyspark._enforcement.arm

----

enforce
-------

A per-function decorator for scripts and notebooks, or for any place
where ``arm()`` cannot be used (e.g. a top-level ``__main__`` script).

Only intercepts parameters annotated with a ``schema_of`` type or a
``SparkSchema`` subclass. All other arguments (``str``, ``int``, ``list``,
custom classes) are left completely alone.

.. code-block:: python

   from frameguard.pyspark import schema_of, enforce

   RawSchema = schema_of(raw_df)

   @enforce
   def enrich(df: RawSchema, label: str, count: int):
       # only df is checked; label and count are not touched
       return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

   enrich(raw_df, "production", 10)   # OK
   enrich(users_df, "production", 10) # raises TypeError: wrong schema on df

.. autofunction:: frameguard.pyspark._enforcement.enforce
