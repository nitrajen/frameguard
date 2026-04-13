Supported Types
===============

dfguard accepts whatever types your library accepts as annotations. There is no
closed list compiled into the library; type dispatch is structural. This means
any new type added in a future library release works automatically without a
dfguard update. The scalar type tests in the test suite are generated at runtime
from the installed library version to verify this claim.

.. tab-set::

   .. tab-item:: PySpark
      :sync: pyspark

      All annotations are **instances**, not classes (``T.LongType()``, not
      ``T.LongType``). Complex types take arguments and enforce inner types
      recursively at the schema level.

      **Numeric**

      .. list-table::
         :header-rows: 1
         :widths: 40 35 25

         * - Annotation
           - Spark SQL type
           - Notes
         * - ``T.ByteType()``
           - ``byte``
           - 8-bit signed integer
         * - ``T.ShortType()``
           - ``smallint``
           - 16-bit signed integer
         * - ``T.IntegerType()``
           - ``int``
           -
         * - ``T.LongType()``
           - ``bigint``
           -
         * - ``T.FloatType()``
           - ``float``
           - 32-bit
         * - ``T.DoubleType()``
           - ``double``
           - 64-bit
         * - ``T.DecimalType(precision, scale)``
           - ``decimal(p,s)``
           - arbitrary precision

      **String and binary**

      .. list-table::
         :header-rows: 1
         :widths: 40 35 25

         * - Annotation
           - Spark SQL type
           - Notes
         * - ``T.StringType()``
           - ``string``
           -
         * - ``T.CharType(n)``
           - ``char(n)``
           - fixed-length (>= 3.3)
         * - ``T.VarcharType(n)``
           - ``varchar(n)``
           - variable-length with max (>= 3.3)
         * - ``T.BinaryType()``
           - ``binary``
           - raw bytes

      **Boolean and temporal**

      .. list-table::
         :header-rows: 1
         :widths: 40 35 25

         * - Annotation
           - Spark SQL type
           - Notes
         * - ``T.BooleanType()``
           - ``boolean``
           -
         * - ``T.DateType()``
           - ``date``
           -
         * - ``T.TimestampType()``
           - ``timestamp``
           - with timezone (legacy Spark behaviour)
         * - ``T.TimestampNTZType()``
           - ``timestamp_ntz``
           - no timezone (>= 3.4)
         * - ``T.DayTimeIntervalType(start, end)``
           - ``interval``
           - day-time interval (>= 3.3)
         * - ``T.YearMonthIntervalType(start, end)``
           - ``interval``
           - year-month interval (>= 3.3)

      **Complex and nested**

      .. list-table::
         :header-rows: 1
         :widths: 40 35 25

         * - Annotation
           - Spark SQL type
           - Notes
         * - ``T.ArrayType(elementType)``
           - ``array<T>``
           - inner type enforced
         * - ``T.MapType(keyType, valueType)``
           - ``map<K,V>``
           - key and value types enforced
         * - ``T.StructType([T.StructField(...), ...])``
           - ``struct<...>``
           - fully recursive
         * - ``SparkSchema`` subclass (as a field type)
           - ``struct<...>``
           - converted to StructType automatically

      **Nullability**

      .. list-table::
         :header-rows: 1
         :widths: 40 60

         * - Annotation
           - Meaning
         * - ``T.LongType()``
           - declares ``nullable=False`` in the schema; only checked in strict mode (``subset=False``)
         * - ``Optional[T.StringType()]``
           - declares ``nullable=True``; use ``dfguard.pyspark.Optional`` for dtype instances

      `Full PySpark DataTypes reference
      <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html>`_

   .. tab-item:: pandas
      :sync: pandas

      .. note::

         Use ``pd.ArrowDtype`` for nested types. ``pd.ArrowDtype(pa.list_(pa.struct([...])))``
         gives pandas full inner-type enforcement at every depth, the same as PySpark and
         Polars. This is where PyArrow-backed pandas surpasses every other pandas dtype.

      dfguard dispatches on the *kind* of annotation, not a hard-coded list. Any
      dtype that is an instance of ``np.dtype`` or ``pd.api.extensions.ExtensionDtype``
      is accepted automatically, including third-party extension types.

      **NumPy dtypes**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - pandas dtype
           - Notes
         * - ``np.dtype("int8")`` / ``np.int8``
           - ``int8``
           -
         * - ``np.dtype("int16")`` / ``np.int16``
           - ``int16``
           -
         * - ``np.dtype("int32")`` / ``np.int32``
           - ``int32``
           -
         * - ``np.dtype("int64")`` / ``np.int64``
           - ``int64``
           -
         * - ``np.dtype("uint8")`` / ``np.uint8``
           - ``uint8``
           -
         * - ``np.dtype("uint16")`` / ``np.uint16``
           - ``uint16``
           -
         * - ``np.dtype("uint32")`` / ``np.uint32``
           - ``uint32``
           -
         * - ``np.dtype("uint64")`` / ``np.uint64``
           - ``uint64``
           -
         * - ``np.dtype("float16")`` / ``np.float16``
           - ``float16``
           -
         * - ``np.dtype("float32")`` / ``np.float32``
           - ``float32``
           -
         * - ``np.dtype("float64")`` / ``np.float64``
           - ``float64``
           -
         * - ``np.dtype("complex64")`` / ``np.complex64``
           - ``complex64``
           -
         * - ``np.dtype("complex128")`` / ``np.complex128``
           - ``complex128``
           -
         * - ``np.dtype("bool")`` / ``np.bool_``
           - ``bool``
           -
         * - ``np.dtype("object")``
           - ``object``
           - Python objects (see note below)
         * - ``np.dtype("datetime64[ns]")``
           - ``datetime64[ns]``
           -
         * - ``np.dtype("timedelta64[ns]")``
           - ``timedelta64[ns]``
           -

      **pandas nullable extension dtypes**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.Int8Dtype()``
           - ``Int8``
           - nullable
         * - ``pd.Int16Dtype()``
           - ``Int16``
           - nullable
         * - ``pd.Int32Dtype()``
           - ``Int32``
           - nullable
         * - ``pd.Int64Dtype()``
           - ``Int64``
           - nullable
         * - ``pd.UInt8Dtype()``
           - ``UInt8``
           - nullable
         * - ``pd.UInt16Dtype()``
           - ``UInt16``
           - nullable
         * - ``pd.UInt32Dtype()``
           - ``UInt32``
           - nullable
         * - ``pd.UInt64Dtype()``
           - ``UInt64``
           - nullable
         * - ``pd.Float32Dtype()``
           - ``Float32``
           - nullable
         * - ``pd.Float64Dtype()``
           - ``Float64``
           - nullable
         * - ``pd.StringDtype()``
           - ``string``
           - nullable
         * - ``pd.BooleanDtype()``
           - ``boolean``
           - nullable
         * - ``pd.CategoricalDtype(categories, ordered)``
           - ``category``
           -
         * - ``pd.DatetimeTZDtype(tz=...)``
           - ``datetime64[ns, tz]``
           - timezone-aware
         * - ``pd.IntervalDtype(subtype)``
           - ``interval``
           -
         * - ``pd.SparseDtype(dtype)``
           - ``Sparse[dtype]``
           -
         * - ``pd.PeriodDtype(freq)``
           - ``period[freq]``
           -

      **PyArrow-backed dtypes (pandas >= 1.5)**

      ``pd.ArrowDtype`` wraps any ``pyarrow.DataType`` and is a subclass of
      ``pd.api.extensions.ExtensionDtype``. dfguard accepts it through the same
      structural path as every other extension dtype: no special handling, no
      hard-coded list. Any ``pa.*`` type, including ones not listed here, works
      automatically.

      *Integer and unsigned*

      .. list-table::
         :header-rows: 1
         :widths: 45 30 25

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.ArrowDtype(pa.int8())``
           - ``int8[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.int16())``
           - ``int16[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.int32())``
           - ``int32[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.int64())``
           - ``int64[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.uint8())``
           - ``uint8[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.uint16())``
           - ``uint16[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.uint32())``
           - ``uint32[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.uint64())``
           - ``uint64[pyarrow]``
           -

      *Float and decimal*

      .. list-table::
         :header-rows: 1
         :widths: 45 30 25

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.ArrowDtype(pa.float16())``
           - ``halffloat[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.float32())``
           - ``float[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.float64())``
           - ``double[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.decimal128(10, 2))``
           - ``decimal128(10, 2)[pyarrow]``
           - arbitrary precision

      *Boolean, string, and binary*

      .. list-table::
         :header-rows: 1
         :widths: 45 30 25

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.ArrowDtype(pa.bool_())``
           - ``bool[pyarrow]``
           -
         * - ``pd.ArrowDtype(pa.string())``
           - ``string[pyarrow]``
           - UTF-8 variable-length
         * - ``pd.ArrowDtype(pa.large_string())``
           - ``large_string[pyarrow]``
           - 64-bit offsets
         * - ``pd.ArrowDtype(pa.binary())``
           - ``binary[pyarrow]``
           - variable-length bytes
         * - ``pd.ArrowDtype(pa.large_binary())``
           - ``large_binary[pyarrow]``
           - 64-bit offsets
         * - ``pd.ArrowDtype(pa.fixed_size_binary(16))``
           - ``fixed_size_binary[16][pyarrow]``
           - fixed-width bytes (e.g. UUIDs)

      *Temporal*

      .. list-table::
         :header-rows: 1
         :widths: 45 30 25

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.ArrowDtype(pa.date32())``
           - ``date32[day][pyarrow]``
           - days since epoch
         * - ``pd.ArrowDtype(pa.date64())``
           - ``date64[ms][pyarrow]``
           - milliseconds since epoch
         * - ``pd.ArrowDtype(pa.time32("ms"))``
           - ``time32[ms][pyarrow]``
           - ``"s"`` or ``"ms"``
         * - ``pd.ArrowDtype(pa.time64("us"))``
           - ``time64[us][pyarrow]``
           - ``"us"`` or ``"ns"``
         * - ``pd.ArrowDtype(pa.timestamp("us"))``
           - ``timestamp[us][pyarrow]``
           - optional tz: ``pa.timestamp("us", tz="UTC")``
         * - ``pd.ArrowDtype(pa.duration("ms"))``
           - ``duration[ms][pyarrow]``
           - ``"s"``, ``"ms"``, ``"us"``, ``"ns"``

      *Complex and nested*

      This is where PyArrow-backed pandas surpasses every other pandas dtype.
      Inner types are enforced at full depth, exactly like PySpark and Polars.

      .. list-table::
         :header-rows: 1
         :widths: 45 30 25

         * - Annotation
           - pandas dtype
           - Notes
         * - ``pd.ArrowDtype(pa.list_(pa.int64()))``
           - ``list<item: int64>[pyarrow]``
           - inner type enforced
         * - ``pd.ArrowDtype(pa.large_list(pa.string()))``
           - ``large_list<item: string>[pyarrow]``
           - 64-bit offsets
         * - ``pd.ArrowDtype(pa.list_(pa.list_(pa.float64())))``
           - ``list<item: list<item: double>>[pyarrow]``
           - nested list
         * - ``pd.ArrowDtype(pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.string())]))``
           - ``struct<x: int64, y: string>[pyarrow]``
           - struct with named fields, fully recursive
         * - ``pd.ArrowDtype(pa.list_(pa.struct([pa.field("id", pa.int64()), pa.field("val", pa.float32())])))``
           - ``list<item: struct<id: int64, val: float>>[pyarrow]``
           - list of dicts
         * - ``pd.ArrowDtype(pa.map_(pa.string(), pa.int64()))``
           - ``map<string, int64>[pyarrow]``
           - key and value types enforced
         * - ``pd.ArrowDtype(pa.map_(pa.string(), pa.list_(pa.float64())))``
           - ``map<string, list<item: double>>[pyarrow]``
           - map of string to list
         * - ``pd.ArrowDtype(pa.dictionary(pa.int32(), pa.string()))``
           - ``dictionary<values=string, indices=int32>[pyarrow]``
           - dictionary-encoded (categorical)
         * - ``pd.ArrowDtype(pa.fixed_size_list(pa.float32(), 3))``
           - ``fixed_size_list<item: float>[3][pyarrow]``
           - fixed-width list (e.g. embeddings)

      *Deeply nested example*

      .. code-block:: python

         import pyarrow as pa, pandas as pd

         # list of structs, where one field is itself a list of floats
         embedding_type = pd.ArrowDtype(
             pa.list_(
                 pa.struct([
                     pa.field("label", pa.string()),
                     pa.field("scores", pa.list_(pa.float32())),
                 ])
             )
         )

         class ModelOutput(dfg.PandasSchema):
             doc_id  = pd.ArrowDtype(pa.int64())
             results = embedding_type

      Any ``pa.*`` type, including ones not shown here, is accepted without a
      dfguard update. The dispatch is structural, not a lookup table.

      .. note::

         ``pd.ArrowDtype`` gives pandas columns the same nested-type precision as
         Polars and PySpark. Use ``pd.ArrowDtype(pa.list_(pa.int64()))`` instead of
         ``list[int]`` when inner-type enforcement matters. The ``object`` dtype
         limitation does not apply to PyArrow-backed columns.

      **Python builtins and generics**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - pandas dtype
           - Notes
         * - ``int``
           - ``int64``
           -
         * - ``float``
           - ``float64``
           -
         * - ``str``
           - ``object``
           -
         * - ``bool``
           - ``bool``
           -
         * - ``datetime.datetime``
           - ``datetime64[ns]``
           -
         * - ``datetime.timedelta``
           - ``timedelta64[ns]``
           -
         * - ``list[T]``, ``dict``, ``tuple``, ``set``
           - ``object``
           - inner type not enforced (use ArrowDtype instead)

      **Nullability**

      .. list-table::
         :header-rows: 1
         :widths: 40 60

         * - Annotation
           - Meaning
         * - ``np.dtype("int64")``
           - non-nullable (NaN collapses to float)
         * - ``pd.Int64Dtype()``
           - nullable integer (no NaN collapse)
         * - ``Optional[pd.StringDtype()]``
           - marks nullable intent; use ``dfguard.pandas.Optional`` for dtype instances
         * - ``np.int64 | None``
           - native Python union syntax, also accepted

      `pandas dtype reference
      <https://pandas.pydata.org/docs/reference/arrays.html>`_ |
      `pd.ArrowDtype reference
      <https://pandas.pydata.org/docs/reference/api/pandas.ArrowDtype.html>`_ |
      `pandas PyArrow integration guide
      <https://pandas.pydata.org/docs/user_guide/arrow.html>`_

   .. tab-item:: Polars
      :sync: polars

      Polars dtypes work as both **classes** (``pl.Int64``) and **instances**
      (``pl.Datetime("ms", "UTC")``). Both are accepted. Complex types enforce
      inner types at the schema level.

      **Integer**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``pl.Int8``
           - ``Int8``
           - 8-bit signed
         * - ``pl.Int16``
           - ``Int16``
           -
         * - ``pl.Int32``
           - ``Int32``
           -
         * - ``pl.Int64``
           - ``Int64``
           -
         * - ``pl.UInt8``
           - ``UInt8``
           - unsigned
         * - ``pl.UInt16``
           - ``UInt16``
           -
         * - ``pl.UInt32``
           - ``UInt32``
           -
         * - ``pl.UInt64``
           - ``UInt64``
           -

      **Float and numeric**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``pl.Float32``
           - ``Float32``
           -
         * - ``pl.Float64``
           - ``Float64``
           -
         * - ``pl.Decimal(precision, scale)``
           - ``Decimal(p,s)``
           - arbitrary precision

      **String, binary, and boolean**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``pl.String`` (alias ``pl.Utf8``)
           - ``String``
           -
         * - ``pl.Binary``
           - ``Binary``
           - raw bytes
         * - ``pl.Boolean``
           - ``Boolean``
           -
         * - ``pl.Categorical``
           - ``Categorical``
           -
         * - ``pl.Enum(["a", "b"])``
           - ``Enum``
           - fixed set of strings

      **Temporal**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``pl.Date``
           - ``Date``
           -
         * - ``pl.Datetime`` / ``pl.Datetime("ms", "UTC")``
           - ``Datetime``
           - optional time unit + timezone
         * - ``pl.Duration`` / ``pl.Duration("ms")``
           - ``Duration``
           -
         * - ``pl.Time``
           - ``Time``
           -

      **Complex and nested**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``pl.List(pl.String)``
           - ``List(String)``
           - inner type enforced
         * - ``pl.Array(pl.Int64, 4)``
           - ``Array(Int64, 4)``
           - fixed-width, inner type enforced
         * - ``pl.Struct({"a": pl.Int64})``
           - ``Struct``
           - recursive, all field types enforced
         * - ``pl.Object``
           - ``Object``
           - arbitrary Python objects
         * - ``pl.Null``
           - ``Null``
           - all-null column

      **Python builtins and generics**

      .. list-table::
         :header-rows: 1
         :widths: 40 30 30

         * - Annotation
           - Polars dtype
           - Notes
         * - ``int``
           - ``Int64``
           -
         * - ``float``
           - ``Float64``
           -
         * - ``str``
           - ``String``
           -
         * - ``bool``
           - ``Boolean``
           -
         * - ``bytes``
           - ``Binary``
           -
         * - ``list[T]``
           - ``List(T)``
           - inner type preserved
         * - ``datetime.datetime``
           - ``Datetime``
           -
         * - ``datetime.date``
           - ``Date``
           -
         * - ``datetime.timedelta``
           - ``Duration``
           -

      **Nullability**

      .. list-table::
         :header-rows: 1
         :widths: 40 60

         * - Annotation
           - Meaning
         * - ``pl.Int64``
           - physically nullable (all Polars columns are)
         * - ``Optional[pl.String]``
           - declares that nulls are intentional in this column
         * - ``pl.String | None``
           - native Python union syntax, also accepted

      `Polars dtype reference
      <https://docs.pola.rs/api/python/stable/reference/datatypes.html>`_

Runtime type coverage
---------------------

Scalar types for PySpark and Polars are tested via runtime discovery: the test
suite walks ``T.DataType.__subclasses__()`` and ``pl.DataType.__subclasses__()``
recursively at test time and runs every concrete, no-argument-constructible type
through the conversion pipeline. New types added in future library releases are
covered automatically. Complex nested types are tested with multi-level
constructions (three-level nested struct, array of structs containing maps, etc.)
to verify that inner types are enforced at every depth.
