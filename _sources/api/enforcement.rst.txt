Enforcement
===========

dfguard enforces schema annotations at the function call site. There are two
ways to enable this. Pick one; you do not need both.

----

arm() / disarm()
----------------

``arm()`` is the preferred approach for packages. Call it once from your entry
point and every annotated function in the entire package is enforced
automatically. No decorator on each function.

``disarm()`` silences all enforcement globally. Useful in tests where you want
to exercise transform logic without schema-valid fixtures.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.pyspark as dfg
         dfg.arm()                 # subset=True: extra columns fine
         # dfg.arm(subset=False)   # strict: exact match everywhere

      .. autofunction:: dfguard.pyspark._enforcement.arm

      .. autofunction:: dfguard.pyspark._enforcement.disarm

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.pandas as dfg
         dfg.arm()

      .. autofunction:: dfguard.pandas._enforcement.arm

      .. autofunction:: dfguard.pandas._enforcement.disarm

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         # my_pipeline/__init__.py
         import dfguard.polars as dfg
         dfg.arm()

      .. autofunction:: dfguard.polars._enforcement.arm

      .. autofunction:: dfguard.polars._enforcement.disarm

``arm()`` has no effect and emits a warning when called from ``__main__``
(a file run directly as a script). Use ``@enforce`` there instead.

----

@enforce
--------

A per-function decorator for scripts and notebooks. Only checks parameters
annotated with a schema type; all other arguments pass through untouched.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         @dfg.enforce
         def enrich(df: OrderSchema, label: str, limit: int = 10):
             # only df is checked; label and limit are not touched
             return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

      .. autofunction:: dfguard.pyspark._enforcement.enforce

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         @dfg.enforce
         def enrich(df: OrderSchema, label: str):
             return df.assign(revenue=df["amount"] * df["quantity"])

      .. autofunction:: dfguard.pandas._enforcement.enforce

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         @dfg.enforce
         def enrich(df: OrderSchema, label: str):
             return df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

      .. autofunction:: dfguard.polars._enforcement.enforce

----

The subset flag
---------------

Both ``arm()`` and ``@enforce`` accept a ``subset`` parameter.

- ``subset=True`` (default): declared columns must be present with correct types;
  extra columns in the DataFrame are fine.
- ``subset=False``: exact match required; extra columns are also an error.

``arm(subset=False)`` sets the global default. ``@enforce(subset=True)`` overrides
it for that function only. Function level always wins.

``schema_of(df)`` types always use exact matching, regardless of ``subset``.
