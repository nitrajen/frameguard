Schemas
=======

Two ways to define a schema:

- ``schema_of(df)``: captures the schema of a live DataFrame as a Python type.
  By default the resulting type uses subset matching (extra columns are fine);
  pass ``subset=False`` to the ``@enforce`` decorator or ``arm()`` call to
  require an exact match.
- Schema subclass (``SparkSchema`` / ``PandasSchema`` / ``PolarsSchema``): declare
  the contract upfront without a DataFrame. Subset matching by default: extra
  columns are fine. Child classes inherit all parent fields.

.. note::

   ``Optional[dtype]`` is documentation: it signals that nulls are expected in
   that column. dfguard checks the column dtype but never inspects the data for
   null presence. A column passes whether it has zero nulls or all nulls. In
   PySpark strict mode (``subset=False``), the ``nullable`` flag in the schema
   metadata is compared, but no Spark job is triggered and no data is read.

----

schema_of
---------

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. autofunction:: dfguard.pyspark.dataset.schema_of

   .. tab-item:: pandas
      :sync: pandas

      .. autofunction:: dfguard.pandas.dataset.schema_of

   .. tab-item:: Polars
      :sync: polars

      Also accepts ``pl.LazyFrame``.

      .. autofunction:: dfguard.polars.dataset.schema_of

----

Schema class
------------

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. code-block:: python

         from pyspark.sql import types as T
         import dfguard.pyspark as dfg
         from dfguard.pyspark import Optional

         class OrderSchema(dfg.SparkSchema):
             order_id = T.LongType()
             amount   = T.DoubleType()
             tags     = T.ArrayType(T.StringType())
             zip      = Optional[T.StringType()]   # nullable

         class EnrichedSchema(OrderSchema):       # inherits all fields
             revenue = T.DoubleType()

      .. autoclass:: dfguard.pyspark.schema.SparkSchema
         :members: to_struct, validate, assert_valid, empty, from_struct, to_code, diff
         :member-order: bysource

   .. tab-item:: pandas
      :sync: pandas

      .. code-block:: python

         import numpy as np
         import pandas as pd
         import dfguard.pandas as dfg
         from dfguard.pandas import Optional

         class OrderSchema(dfg.PandasSchema):
             order_id = np.dtype("int64")
             amount   = np.dtype("float64")
             label    = pd.StringDtype()
             zip      = Optional[pd.StringDtype()]

         class EnrichedSchema(OrderSchema):
             revenue = np.dtype("float64")

      .. autoclass:: dfguard.pandas.schema.PandasSchema
         :members: to_struct, to_dtype_dict, validate, assert_valid, empty, from_struct, to_code, diff
         :member-order: bysource

   .. tab-item:: Polars
      :sync: polars

      .. code-block:: python

         import polars as pl
         import dfguard.polars as dfg
         from dfguard.polars import Optional

         class OrderSchema(dfg.PolarsSchema):
             order_id = pl.Int64
             amount   = pl.Float64
             tags     = pl.List(pl.String)
             zip      = Optional[pl.String]

         class EnrichedSchema(OrderSchema):
             revenue = pl.Float64

      .. autoclass:: dfguard.polars.schema.PolarsSchema
         :members: to_struct, validate, assert_valid, empty, from_struct, to_code, diff
         :member-order: bysource
