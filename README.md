# frameguard

**Schema mismatches in data pipelines fail late.** A cryptic `AnalysisException` inside
Spark, or worse, silent bad data reaching production. By then you've lost the context of
what went wrong and where.

**frameguard catches it at the source: the function call.**

```python
from pyspark.sql import SparkSession, functions as F
from frameguard.pyspark import schema_of, enforce

spark = SparkSession.builder.getOrCreate()

raw_df = spark.createDataFrame(
    [(1, 10.0, 3), (2, 5.0, 7)],
    "order_id LONG, amount DOUBLE, quantity INT",
)
users_df = spark.createDataFrame(
    [(101, "Alice"), (102, "Bob")],
    "user_id LONG, name STRING",
)

RawSchema = schema_of(raw_df)

@enforce
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

**Capture from a live DataFrame** — exact matching, per-stage snapshot:

```python
# continuing from above
RawSchema      = schema_of(raw_df)
EnrichedSchema = schema_of(enriched_df)   # new type after adding revenue column
```

**Declare upfront** — subset matching, no DataFrame required:

```python
from pyspark.sql import types as T
from typing import Optional
from frameguard.pyspark import SparkSchema

class OrderSchema(SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    tags:     T.ArrayType(T.StringType())
    zip:      Optional[T.StringType()]    # nullable

class EnrichedSchema(OrderSchema):        # inherits all parent fields
    revenue: T.DoubleType()
```

Use `SparkSchema` when you want to declare a contract upfront (Kedro nodes, shared
schemas across a team). Use `schema_of(df)` when you want to snapshot the exact schema
at each pipeline stage.

---

## Enforcement

**In scripts and notebooks** — per-function decorator:

```python
from pyspark.sql import SparkSession, functions as F
from frameguard.pyspark import schema_of, enforce

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3)], "order_id LONG, amount DOUBLE, quantity INT"
)
RawSchema = schema_of(raw_df)

@enforce
def enrich(df: RawSchema, label: str):   # only df is checked; label is not touched
    return df.withColumn("revenue", F.col("amount") * F.col("quantity"))
```

**In packages** (Kedro, Airflow) — call `arm()` after all function definitions:

```python
# nodes.py
from pyspark.sql import types as T
from frameguard.pyspark import SparkSchema, arm

class RawSchema(SparkSchema):
    order_id: T.LongType()
    amount:   T.DoubleType()
    quantity: T.IntegerType()

def enrich(df: RawSchema): ...    # enforced
def clean(df: RawSchema):  ...    # enforced

arm()   # wraps every public function above; call after all definitions
```

---

## Schema history

```python
from pyspark.sql import SparkSession, functions as F
from frameguard.pyspark import dataset

spark = SparkSession.builder.getOrCreate()
raw_df = spark.createDataFrame(
    [(1, 10.0, 3, ["vip"], "10001")],
    "order_id LONG, amount DOUBLE, quantity INT, tags ARRAY<STRING>, zip STRING",
)

ds = dataset(raw_df)
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
