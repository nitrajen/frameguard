# frameguard

**Schema mismatches in data pipelines fail late.** A cryptic `AnalysisException` inside
Spark, or worse, silent bad data reaching production. By then you've lost the context of
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

enriched_df = enrich(raw_df)    # ok
enrich(users_df)                # raises immediately: wrong schema
enrich(enriched_df)             # raises immediately: extra columns violate the contract
```

No validation logic inside the function. No waiting for Spark. The wrong DataFrame
simply cannot enter the wrong function.

**Zero extra dependencies.** `pip install frameguard[pyspark]` installs only PySpark,
which you already have. The enforcement is pure Python `isinstance()` checks. Only
your DataFrame arguments are checked; `str`, `int`, and other args are left alone.

---

## Install

```bash
pip install frameguard[pyspark]
```

Requires Python >= 3.10, PySpark >= 3.3.

---

## Two ways to define a schema

**Capture from a live DataFrame** — inferred at runtime, exact matching:

```python
# continuing from above
RawSchema      = fg.schema_of(raw_df)
EnrichedSchema = fg.schema_of(enriched_df)   # new type after adding revenue column
```

**Declare upfront** — no DataFrame required:

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

Use `fg.SparkSchema` when you want to declare a contract upfront (Kedro nodes, shared
schemas across a team). Use `fg.schema_of(df)` when you want to snapshot the exact schema
at each pipeline stage.

---

## Enforcement

**In packages** (Kedro, Airflow) — call `fg.arm()` once from your entry point:

```python
# my_pipeline/settings.py
import frameguard.pyspark as fg

fg.arm()   # walks and arms the entire package — no decorators needed in node files
```

**In scripts and notebooks** — per-function decorator:

```python
import frameguard.pyspark as fg
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
)
RawSchema = fg.schema_of(raw_df)

@fg.enforce
def enrich(df: RawSchema, label: str):   # only df is checked; label is not touched
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))
```

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

When `validate()` fails the error includes the full history, so you know exactly where
the schema diverged from what the downstream stage expected.

---

## License

Apache 2.0
