# frameguard

[![PyPI](https://img.shields.io/pypi/v/frameguard)](https://pypi.org/project/frameguard/)
[![Python](https://img.shields.io/pypi/pyversions/frameguard)](https://pypi.org/project/frameguard/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Tests](https://github.com/nitrajen/frameguard/actions/workflows/ci.yml/badge.svg)](https://github.com/nitrajen/frameguard/actions)
[![Docs](https://img.shields.io/badge/docs-frameguard.readthedocs.io-blue)](https://frameguard.readthedocs.io)

**Schema mismatches in data pipelines fail late.** A cryptic `AnalysisException` deep in
Spark, or worse — silent bad data reaching production. By then you've lost the context of
what went wrong and where.

**frameguard catches it at the source: the function call.**

```python
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
def enrich(df: RawSchema):
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

enrich(raw_df)     # ✓ passes
enrich(users_df)   # ✗ raises immediately — wrong schema
```

No validation logic inside the function. No waiting for Spark to plan the job.
The wrong DataFrame simply cannot enter the wrong function.

> **Zero extra dependencies.** `pip install frameguard[pyspark]` installs only
> PySpark, which you already have. Enforcement is pure Python `isinstance()`.
> Only schema-annotated arguments are touched — `str`, `int`, and all other
> arguments pass through untouched.

---

## Install

```bash
pip install frameguard[pyspark]
```

Requires Python ≥ 3.10, PySpark ≥ 3.3.

---

## Two ways to define a schema

### Capture from a live DataFrame

```python
import frameguard.pyspark as fg
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
)
enriched_df = raw_df.withColumn("revenue", F.col("amount") * F.col("quantity"))

RawSchema      = fg.schema_of(raw_df)       # snapshot — exact match required
EnrichedSchema = fg.schema_of(enriched_df)  # new type after adding revenue
```

The check is **exact**: a DataFrame with extra columns does *not* satisfy
`RawSchema`. Capture a new type at each stage boundary.

### Declare upfront

```python
import frameguard.pyspark as fg
from pyspark.sql import types as T
from typing import Optional

class OrderSchema(fg.SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    tags:     T.ArrayType(T.StringType())
    zip:      Optional[T.StringType()]    # nullable

class EnrichedSchema(OrderSchema):        # inherits all parent fields
    revenue: T.DoubleType()
```

Use `fg.SparkSchema` when you want to declare a contract upfront — Kedro nodes,
shared schemas across a team. By default extra columns are allowed (`subset=True`).

---

## Enforcement

### Packages — arm once, protect everywhere

```python
# my_pipeline/settings.py  (or __init__.py)
import frameguard.pyspark as fg

fg.arm()                # subset=True: extra columns fine (default)
fg.arm(subset=False)    # exact match: no extra columns anywhere
```

`fg.arm()` walks the entire package and wraps every annotated function.
Node files need no imports or decorators.

### Scripts and notebooks — per-function

```python
import frameguard.pyspark as fg
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
)
RawSchema = fg.schema_of(raw_df)

@fg.enforce                     # inherits global subset setting
def enrich(df: RawSchema, label: str):   # only df is checked
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

@fg.enforce(subset=False)       # exact match for this function only
def write(df: RawSchema): ...

@fg.enforce(always=True)        # enforces even after fg.disable()
def critical(df: RawSchema): ...
```

### Subset flag

| Level | How | Default |
|---|---|---|
| Global | `fg.arm(subset=True/False)` | `True` |
| Function | `@fg.enforce(subset=True/False)` | inherits global |

Function-level always wins. `subset=True` means extra columns are fine.
`subset=False` means the DataFrame must match the schema exactly — no extra columns.

---

## Schema history

```python
import frameguard.pyspark as fg
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3, ["vip"], "10001")],
    "order_id LONG, amount DOUBLE, quantity INT, tags ARRAY<STRING>, zip STRING",
)

ds = fg.dataset(raw_df)
ds = ds.withColumn("revenue", F.col("amount") * F.col("quantity"))
ds = ds.drop("tags")

print(ds.schema_history)
# [0] input                  order_id:long, amount:double, quantity:int, tags:array<string>, zip:string
# [1] withColumn('revenue')  + revenue:double
# [2] drop(['tags'])         - tags
```

When `validate()` fails, the error includes the full history — you know exactly
where the schema diverged from what the next stage expected.

---

## Documentation

Full docs at **[frameguard.readthedocs.io](https://frameguard.readthedocs.io)**,
including:

- [Quickstart](https://frameguard.readthedocs.io/en/latest/quickstart.html) — full walkthrough with nested structs and multi-stage pipelines
- [Enforcement reference](https://frameguard.readthedocs.io/en/latest/api/pyspark/enforcement.html) — `arm()`, `enforce()`, `disable()`, `always`
- [Schema utilities](https://frameguard.readthedocs.io/en/latest/api/pyspark/schemas.html) — `schema_of`, `SparkSchema`, `from_struct`, `to_code`
- [Schema history](https://frameguard.readthedocs.io/en/latest/api/pyspark/history.html) — tracking mutations across pipeline stages

---

## License

Apache 2.0
