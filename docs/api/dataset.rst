Dataset
=======

.. warning::

   ``dataset()`` is an internal utility and is not part of the stable public API.
   It may change or be removed in future releases.

``dfg.dataset(df)`` wraps a DataFrame and records schema-changing operations
in ``schema_history``. Schema tracking is limited to a fixed set of explicitly
wrapped methods listed below. Calling any other method on the wrapper passes
through to the underlying DataFrame but breaks the tracking chain: the returned
object is a plain DataFrame, not a tracked dataset.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      .. autofunction:: dfguard.pyspark.dataset._make_dataset

      **Tracked methods:** ``withColumn``, ``withColumns``, ``withColumnRenamed``,
      ``withColumnsRenamed``, ``withMetadata``, ``drop``, ``select``, ``selectExpr``,
      ``toDF``, ``filter``, ``where``, ``limit``, ``sample``, ``distinct``,
      ``dropDuplicates``, ``orderBy``, ``repartition``, ``repartitionByRange``,
      ``coalesce``, ``union``, ``unionByName``, ``intersect``, ``intersectAll``,
      ``subtract``, ``join``, ``crossJoin``, ``groupBy``, ``rollup``, ``cube``,
      ``na``, ``stat``, ``transform``, ``unpivot``, ``agg``, ``count``, ``mean``,
      ``avg``, ``sum``, ``min``, ``max``, ``pivot``, ``apply``, ``applyInPandas``

   .. tab-item:: pandas
      :sync: pandas

      .. autofunction:: dfguard.pandas.dataset._make_dataset

      **Tracked methods:** ``assign``, ``rename``, ``drop``, ``select``, ``astype``,
      ``filter``, ``query``, ``head``, ``tail``, ``sample``, ``drop_duplicates``,
      ``sort_values``, ``reset_index``, ``merge``, ``join``, ``groupby``, ``melt``,
      ``pivot_table``, ``explode``, ``agg``, ``sum``, ``mean``, ``count``, ``min``,
      ``max``, ``first``

   .. tab-item:: Polars
      :sync: polars

      Works with both ``pl.DataFrame`` and ``pl.LazyFrame``.

      .. autofunction:: dfguard.polars.dataset.dataset

      **Tracked methods:** ``with_columns``, ``rename``, ``drop``, ``select``,
      ``filter``, ``sort``, ``unique``, ``join``, ``group_by``, ``agg``

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
