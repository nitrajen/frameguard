Dataset
=======

``dfg.dataset(df)`` wraps a DataFrame and records every schema-changing
operation in ``schema_history``. The wrapper exposes the same mutation API
as the underlying library; each method returns a new wrapped instance with
an updated history entry.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. autofunction:: dfguard.pyspark.dataset._make_dataset

      **Available methods:** ``withColumn``, ``withColumns``, ``drop``, ``select``,
      ``filter``, ``where``, ``withColumnRenamed``, ``join``, ``union``,
      ``unionByName``, ``groupBy``, ``orderBy``, ``sort``, ``limit``,
      ``distinct``, ``dropDuplicates``, ``coalesce``, ``repartition``,
      ``cache``, ``persist``, ``unpersist``, ``alias``

   .. tab-item:: pandas
      :sync: pandas

      .. autofunction:: dfguard.pandas.dataset._make_dataset

      **Available methods:** ``assign``, ``drop``, ``rename``, ``select``,
      ``filter``, ``query``, ``merge``, ``join``, ``groupby``, ``sort_values``,
      ``drop_duplicates``, ``reset_index``, ``set_index``

   .. tab-item:: Polars
      :sync: polars

      Works with both ``pl.DataFrame`` and ``pl.LazyFrame``.

      .. autofunction:: dfguard.polars.dataset.dataset

      **Available methods:** ``with_columns``, ``drop``, ``rename``, ``select``,
      ``filter``, ``sort``, ``unique``, ``join``, ``group_by``

schema_history
--------------

Every dataset wrapper exposes ``schema_history``, an immutable record of all
schema-changing operations since the DataFrame was wrapped.

.. code-block:: python

   ds = dfg.dataset(raw_df)
   ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
   ds = ds.drop("tags")

   ds.schema_history.print()
   # Schema Evolution
   #   [ 0] input
   #   [ 1] withColumn('revenue')  -- added: revenue:double
   #   [ 2] drop(['tags'])         -- dropped: tags

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. autoclass:: dfguard.pyspark.history.SchemaHistory
         :members:

      .. autoclass:: dfguard.pyspark.history.SchemaChange
         :members:

   .. tab-item:: pandas
      :sync: pandas

      .. autoclass:: dfguard.pandas.history.PandasSchemaHistory
         :members:

      .. autoclass:: dfguard.pandas.history.PandasSchemaChange
         :members:

   .. tab-item:: Polars
      :sync: polars

      .. autoclass:: dfguard.polars.history.PolarsSchemaHistory
         :members:

      .. autoclass:: dfguard.polars.history.PolarsSchemaChange
         :members:
