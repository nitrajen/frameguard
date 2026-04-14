"""
Sample orders pipeline using dfguard for schema enforcement.

dfg.arm() in the module covers all annotated functions automatically.
No decorator needed on each function.

Run standalone:
    python examples/benchmarks/pipeline.py
"""
import numpy as np
import pandas as pd
import dfguard.pandas as dfg

dfg.arm()  # one call covers all annotated functions below


class RawOrderSchema(dfg.PandasSchema):
    order_id    = np.dtype("int64")           # numpy int
    customer_id = np.dtype("int64")
    amount      = np.dtype("float64")         # numpy float
    quantity    = np.dtype("int32")           # numpy int32
    sku         = pd.StringDtype()            # pandas extension string
    is_vip      = pd.BooleanDtype()           # pandas extension bool
    created_at  = np.dtype("datetime64[ns]")  # datetime


class EnrichedSchema(RawOrderSchema):         # inherits all parent fields
    revenue  = np.dtype("float64")
    discount = np.dtype("float64")


class FlaggedSchema(EnrichedSchema):
    is_high_value = pd.BooleanDtype()


def make_raw(n: int = 100, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    return pd.DataFrame({
        "order_id":    pd.array(rng.integers(1, 10_000, n),   dtype="int64"),
        "customer_id": pd.array(rng.integers(1, 1_000, n),    dtype="int64"),
        "amount":      pd.array(rng.uniform(1.0, 500.0, n),   dtype="float64"),
        "quantity":    pd.array(rng.integers(1, 20, n),       dtype="int32"),
        "sku":         pd.array([f"SKU-{i:04d}" for i in rng.integers(1, 500, n)], dtype=pd.StringDtype()),
        "is_vip":      pd.array(rng.choice([True, False], n), dtype=pd.BooleanDtype()),
        "created_at":  pd.array(pd.date_range("2024-01-01", periods=n, freq="h"), dtype="datetime64[ns]"),
    })


def enrich(df: RawOrderSchema) -> pd.DataFrame:
    revenue  = df["amount"] * df["quantity"]
    discount = revenue.where(revenue <= 500, 50.0)
    return df.assign(revenue=revenue, discount=discount)


def flag(df: EnrichedSchema) -> pd.DataFrame:
    return df.assign(is_high_value=pd.array(df["revenue"] > 500, dtype=pd.BooleanDtype()))


def aggregate(df: FlaggedSchema) -> pd.DataFrame:
    return (
        df.groupby("customer_id")
        .agg(total_revenue=("revenue", "sum"), order_count=("order_id", "count"))
        .reset_index()
    )


if __name__ == "__main__":
    raw      = make_raw(200)
    enriched = enrich(raw)
    flagged  = flag(enriched)
    summary  = aggregate(flagged)
    print(f"Processed {len(raw)} orders -> {len(summary)} customers")
    print(summary.head())
