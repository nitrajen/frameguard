<div align="center">

# dfguard

**Catch DataFrame schema mismatches at the function call, not deep in your pipeline.**

[![PyPI](https://img.shields.io/pypi/v/dfguard?color=blue&label=PyPI)](https://pypi.org/project/dfguard/)
[![Python](https://img.shields.io/pypi/pyversions/dfguard)](https://pypi.org/project/dfguard/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![CI](https://github.com/nitrajen/dfguard/actions/workflows/ci.yml/badge.svg)](https://github.com/nitrajen/dfguard/actions)
[![Coverage](https://codecov.io/gh/nitrajen/dfguard/branch/main/graph/badge.svg)](https://codecov.io/gh/nitrajen/dfguard)
[![Docs](https://img.shields.io/badge/docs-dfguard.readthedocs.io-blue)](https://dfguard.readthedocs.io)

**[Documentation](https://dfguard.readthedocs.io)** | [Quickstart](https://dfguard.readthedocs.io/en/latest/quickstart.html) | [API Reference](https://dfguard.readthedocs.io/en/latest/api/index.html)

</div>

---

Data pipelines fail late. A DataFrame with the wrong schema enters a function without complaint, the job runs, and the crash surfaces somewhere downstream with an error that tells you nothing about where the mismatch started.

**dfguard moves that failure to the function call.** The wrong DataFrame is rejected immediately with a precise error: which function, which argument, what schema was expected, what arrived.

Currently supports PySpark. pandas and polars support coming soon.

```python
import dfguard.pyspark as dfg
from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3), (2, 5.0, 7)],
    "order_id LONG, amount DOUBLE, quantity INT",
)

# Declare the input contract upfront -- no live DataFrame needed
class RawSchema(dfg.SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    quantity: T.IntegerType()

@dfg.enforce
def enrich(df: RawSchema):
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

# Capture the output schema from the live result
EnrichedSchema = dfg.schema_of(enrich(raw_df))

@dfg.enforce
def flag_high_value(df: EnrichedSchema):
    return df.withColumn("is_vip", F.col("revenue") > 1000)

flag_high_value(raw_df)
# TypeError: Schema mismatch in flag_high_value() argument 'df':
#   expected: order_id:bigint, amount:double, quantity:int, revenue:double
#   received: order_id:bigint, amount:double, quantity:int
```

No validation logic inside functions. The wrong DataFrame simply cannot enter the wrong function.

For package-wide enforcement without decorating each function, call `dfg.arm()` once from your package entry point.

---

## Install

```bash
pip install dfguard[pyspark]
```

Requires Python >= 3.10, PySpark >= 3.3. No other dependencies.

---

## Two ways to define a schema

### Capture from a live DataFrame

```python
RawSchema      = dfg.schema_of(raw_df)       # exact snapshot of this stage
EnrichedSchema = dfg.schema_of(enriched_df)  # new type after adding columns
```

Exact matching: a DataFrame with extra columns does **not** satisfy `RawSchema`. Capture a new type at each stage boundary.

### Declare upfront

```python
from dfguard.pyspark import Optional
from pyspark.sql import types as T

class OrderSchema(dfg.SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    tags:     T.ArrayType(T.StringType())
    address:  Optional[T.StringType()]   # nullable field

class EnrichedSchema(OrderSchema):       # inherits all parent fields
    revenue: T.DoubleType()
```

No live DataFrame needed. Subclasses inherit parent fields. Works with nested structs, arrays, and maps.

Use the schema when creating a DataFrame:

```python
df = spark.createDataFrame(rows, OrderSchema.to_struct())
```

---

## Enforcement

### Arm once, protect everything

```python
# my_pipeline/__init__.py
import dfguard.pyspark as dfg

dfg.arm()   # walks the package, wraps every annotated function
```

Functions with schema-annotated arguments are enforced automatically -- no decorator needed on each one:

```python
# my_pipeline/transforms.py
def enrich(df: OrderSchema):       # enforced automatically
    return df.withColumn(...)

def aggregate(df: EnrichedSchema): # also enforced
    return df.groupBy(...)
```

### Per-function decoration

Use `@dfg.enforce` in scripts, notebooks, or when you want a function-level `subset` override:

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

`dfg.disarm()` turns off all enforcement globally -- every guarded function passes through without checking, whether wrapped by `dfg.arm()` or decorated with `@dfg.enforce`. Useful in tests where you want to exercise transformation logic without schema-valid fixtures.

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

- **[Airflow](https://dfguard.readthedocs.io/en/latest/airflow.html)**: `dfg.arm()` globally, `assert_valid` after loading from storage, `@dfg.enforce(subset=False)` on functions that write to fixed-schema sinks
- **[Kedro](https://dfguard.readthedocs.io/en/latest/kedro.html)**: `dfg.arm()` in `settings.py`, node functions need no decorators

---

## Documentation

**[dfguard.readthedocs.io](https://dfguard.readthedocs.io)**

- [Quickstart](https://dfguard.readthedocs.io/en/latest/quickstart.html): nested structs, multi-stage pipelines, subset flag, schema history
- [API reference](https://dfguard.readthedocs.io/en/latest/api/index.html): `arm`, `disarm`, `enforce`, `schema_of`, `SparkSchema`, `dataset`
- [Airflow integration](https://dfguard.readthedocs.io/en/latest/airflow.html)
- [Kedro integration](https://dfguard.readthedocs.io/en/latest/kedro.html)

---

## License

[Apache 2.0](LICENSE)
