<div align="center">

# dfguard

**Catch DataFrame schema mismatches at the function call, not deep in your pipeline.**

[![PyPI](https://img.shields.io/pypi/v/dfguard?color=blue&label=PyPI)](https://pypi.org/project/dfguard/)
[![Python](https://img.shields.io/pypi/pyversions/dfguard)](https://pypi.org/project/dfguard/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![CI](https://github.com/nitrajen/dfguard/actions/workflows/ci.yml/badge.svg)](https://github.com/nitrajen/dfguard/actions)
[![Coverage](https://img.shields.io/codecov/c/github/nitrajen/dfguard?label=Coverage)](https://app.codecov.io/gh/nitrajen/dfguard)
[![Docs](https://img.shields.io/badge/docs-nitrajen.github.io/dfguard-blue)](https://nitrajen.github.io/dfguard/)

**[Documentation](https://nitrajen.github.io/dfguard/)** | [Quickstart](https://nitrajen.github.io/dfguard/quickstart.html) | [API Reference](https://nitrajen.github.io/dfguard/api/index.html)

</div>

---

Data pipelines fail late. A DataFrame with the wrong schema enters a function without complaint, the job runs, and the crash surfaces somewhere downstream with an error that tells you nothing about where the mismatch started.

**dfguard moves that failure to the function call.** The wrong DataFrame is rejected immediately with a precise error: which function, which argument, what schema was expected, what arrived. **Lightweight**: enforcement is pure metadata inspection — dfguard reads the schema struct from your DataFrame, no data is scanned, no Spark jobs triggered. Unlike [pandera](https://pandera.readthedocs.io/en/stable/), which introduces its own type system, dfguard uses the types your library already ships with: `T.LongType()` for PySpark, `pl.Int64` for Polars, `np.dtype("int64")` for pandas.

## Compatibility

| Backend | Version | Python |
|---------|---------|--------|
| PySpark | >= 3.3  | >= 3.10 |
| pandas  | >= 1.5  | >= 3.10 |
| Polars  | >= 0.20 | >= 3.10 |

## Install

```bash
pip install 'dfguard[pyspark]'            # PySpark
pip install 'dfguard[pandas]' pyarrow    # pandas (pyarrow recommended for nested types)
pip install 'dfguard[polars]'            # Polars
pip install 'dfguard[all]'               # all backends
```

Requires Python >= 3.10. No other mandatory dependencies.

---

<!-- tabs-start -->

**PySpark**

```python
import dfguard.pyspark as dfg
from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3), (2, 5.0, 7)],
    "order_id LONG, amount DOUBLE, quantity INT",
)

class RawSchema(dfg.SparkSchema):
    order_id = T.LongType()
    amount   = T.DoubleType()
    quantity = T.IntegerType()

@dfg.enforce
def enrich(df: RawSchema):
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

EnrichedSchema = dfg.schema_of(enrich(raw_df))

@dfg.enforce
def flag_high_value(df: EnrichedSchema):
    return df.withColumn("is_vip", F.col("revenue") > 1000)

flag_high_value(raw_df)
# TypeError: Schema mismatch in flag_high_value() argument 'df':
#   expected: order_id:bigint, amount:double, quantity:int, revenue:double
#   received: order_id:bigint, amount:double, quantity:int
```

**pandas**

```python
import numpy as np
import pandas as pd
import dfguard.pandas as dfg

raw_df = pd.DataFrame({
    "order_id": pd.array([1, 2, 3], dtype="int64"),
    "amount":   pd.array([10.0, 5.0, 8.5], dtype="float64"),
    "quantity": pd.array([3, 1, 2], dtype="int64"),
})

class RawSchema(dfg.PandasSchema):
    order_id = np.dtype("int64")
    amount   = np.dtype("float64")
    quantity = np.dtype("int64")

@dfg.enforce
def enrich(df: RawSchema):
    return df.assign(revenue=df["amount"] * df["quantity"])

EnrichedSchema = dfg.schema_of(enrich(raw_df))

@dfg.enforce
def flag_high_value(df: EnrichedSchema):
    return df.assign(is_vip=df["revenue"] > 1000)

flag_high_value(raw_df)
# TypeError: Schema mismatch in flag_high_value() argument 'df':
#   expected: order_id:int64, amount:float64, quantity:int64, revenue:float64
#   received: order_id:int64, amount:float64, quantity:int64
```

**Polars**

```python
import polars as pl
import dfguard.polars as dfg

raw_df = pl.DataFrame({
    "order_id": pl.Series([1, 2, 3], dtype=pl.Int64),
    "amount":   pl.Series([10.0, 5.0, 8.5], dtype=pl.Float64),
    "quantity": pl.Series([3, 1, 2], dtype=pl.Int32),
})

class RawSchema(dfg.PolarsSchema):
    order_id = pl.Int64
    amount   = pl.Float64
    quantity = pl.Int32

@dfg.enforce
def enrich(df: RawSchema) -> pl.DataFrame:
    return df.with_columns(revenue=pl.col("amount") * pl.col("quantity"))

EnrichedSchema = dfg.schema_of(enrich(raw_df))

@dfg.enforce
def flag_high_value(df: EnrichedSchema) -> pl.DataFrame:
    return df.with_columns(is_vip=pl.col("revenue") > 1000)

flag_high_value(raw_df)
# TypeError: Schema mismatch in flag_high_value() argument 'df':
#   expected: order_id:Int64, amount:Float64, quantity:Int32, revenue:Float64
#   received: order_id:Int64, amount:Float64, quantity:Int32
```

<!-- tabs-end -->

No validation logic inside functions. The wrong DataFrame simply cannot enter the wrong function.

For package-wide enforcement without decorating each function, call `dfg.arm()` once from your package entry point.

---

## Two ways to define a schema

### Capture from a live DataFrame

```python
RawSchema      = dfg.schema_of(raw_df)       # exact snapshot of this stage
EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding columns
```

Exact matching: a DataFrame with extra columns does **not** satisfy `RawSchema`. Capture a new type at each stage boundary.

### Declare upfront

No live DataFrame needed. Subclasses inherit parent fields. All three backends support nested types fully.

**PySpark** — arrays, structs, maps via `T.ArrayType` / `T.StructType` / `T.MapType`:

```python
from dfguard.pyspark import Optional
from pyspark.sql import types as T

class OrderSchema(dfg.SparkSchema):
    order_id   = T.LongType()
    amount     = T.DoubleType()
    line_items = T.ArrayType(T.StructType([          # array of structs
        T.StructField("sku",      T.StringType()),
        T.StructField("quantity", T.IntegerType()),
        T.StructField("price",    T.DoubleType()),
    ]))
    zip_code   = Optional[T.StringType()]            # nullable field

class EnrichedSchema(OrderSchema):                   # inherits all parent fields
    revenue = T.DoubleType()

df = spark.createDataFrame(rows, OrderSchema.to_struct())
```

**Polars** — `pl.List`, `pl.Struct`, `pl.Array` are native first-class types:

```python
from dfguard.polars import Optional

class OrderSchema(dfg.PolarsSchema):
    order_id   = pl.Int64
    amount     = pl.Float64
    line_items = pl.List(pl.Struct({                 # list of structs
        "sku":      pl.String,
        "quantity": pl.Int32,
        "price":    pl.Float64,
    }))
    zip_code   = Optional[pl.String]                 # nullable field
```

**pandas** — use `pd.ArrowDtype` (requires `pyarrow`) for nested types:

```python
import pyarrow as pa
from dfguard.pandas import Optional

class OrderSchema(dfg.PandasSchema):
    order_id   = np.dtype("int64")
    amount     = np.dtype("float64")
    line_items = pd.ArrowDtype(pa.list_(pa.struct([  # nested via PyArrow
        pa.field("sku",      pa.string()),
        pa.field("quantity", pa.int32()),
        pa.field("price",    pa.float64()),
    ])))
    zip_code   = Optional[pd.StringDtype()]          # nullable field
```

> **pandas + PyArrow**: `pd.ArrowDtype` gives pandas the same nested-type enforcement as PySpark and Polars — arrays, structs, and maps at arbitrary depth. Without PyArrow, pandas dtype enforcement is limited to flat scalar types (`np.dtype`, `pd.StringDtype`, etc.). Install with `pip install 'dfguard[pandas]' pyarrow`.

---

## Enforcement

### Arm once, protect everything

```python
# my_pipeline/__init__.py
import dfguard.pyspark as dfg

dfg.arm()   # walks the package, wraps every annotated function
```

Functions with schema-annotated arguments are enforced automatically, no decorator needed on each one:

```python
# my_pipeline/transforms.py
def enrich(df: OrderSchema):       # enforced automatically
    return df.withColumn(...)

def aggregate(df: EnrichedSchema): # also enforced
    return df.groupBy(...)
```

### Per-function decoration

Use `@dfg.enforce` in scripts and notebooks, or when you want a function-level `subset` override:

```python
@dfg.enforce                   # subset=True: extra columns fine (default)
def process(df: OrderSchema): ...

@dfg.enforce(subset=False)     # exact match: no extra columns allowed
def write_final(df: OrderSchema): ...
```

### The `subset` flag

`subset=True` (default): all declared columns must be present with the right types; extra columns are fine.
`subset=False`: declared columns must be present and no extras are allowed.

Set it globally via `dfg.arm(subset=False)`. Override per function via `@dfg.enforce(subset=True)`. Function level always wins. `schema_of` types always use exact matching regardless of `subset`.

### Disabling enforcement

`dfg.disarm()` turns off all enforcement globally, whether wrapped by `dfg.arm()` or decorated with `@dfg.enforce`. Useful in tests.

```python
dfg.arm()
enrich(wrong_df)   # raises

dfg.disarm()
enrich(wrong_df)   # passes: enforcement is off
```

---

## Validate at load time

Use `assert_valid` right after reading from storage to catch upstream schema drift before processing starts:

```python
raw = spark.read.parquet("/data/orders/raw.parquet")
OrderSchema.assert_valid(raw)   # raises SchemaValidationError if schema changed

enriched = enrich(raw)          # @dfg.enforce then guards the function call
```

Reports all problems at once, not just the first:

```
SchemaValidationError: Schema validation failed:
  ✗ Column 'revenue': type mismatch: expected double, got string
  ✗ Missing column 'is_high_value' (expected boolean, nullable=False)
```

---

## Schema history

`dfg.dataset(df)` records every schema-changing operation. Call `.schema_history.print()` to see the full evolution:

```python
ds = dfg.dataset(raw_df)
ds = ds.withColumn("revenue",  F.col("amount") * 1.1)
ds = ds.withColumn("discount", F.when(F.col("revenue") > 500, 50.0).otherwise(0.0))
ds = ds.drop("tags")
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
#   [ 3] drop(['tags'])
#        dropped: tags
#   [ 4] withColumnRenamed('customer'→'customer_name')
#        added: customer_name:string | dropped: customer
# ────────────────────────────────────────────────────────────
```

---

## Pipeline integrations

dfguard fits naturally into pipeline frameworks. See the full docs for working examples with runnable code:

- **[Airflow](https://nitrajen.github.io/dfguard/airflow.html)**: `dfg.arm()` globally, `assert_valid` after loading from storage, `@dfg.enforce(subset=False)` on functions that write to fixed-schema sinks
- **[Kedro](https://nitrajen.github.io/dfguard/kedro.html)**: `dfg.arm()` in `settings.py`, node functions need no decorators

---

## Documentation

**[nitrajen.github.io/dfguard](https://nitrajen.github.io/dfguard/)**

- [Quickstart](https://nitrajen.github.io/dfguard/quickstart.html): nested structs, multi-stage pipelines, subset flag, schema history
- [Types](https://nitrajen.github.io/dfguard/types.html): full type coverage per backend, including PyArrow for nested pandas types
- [API reference](https://nitrajen.github.io/dfguard/api/index.html): `arm`, `disarm`, `enforce`, `schema_of`, `SparkSchema`/`PandasSchema`/`PolarsSchema`, `dataset`
- [Airflow integration](https://nitrajen.github.io/dfguard/airflow.html)
- [Kedro integration](https://nitrajen.github.io/dfguard/kedro.html)

---

## License

[Apache 2.0](LICENSE)
