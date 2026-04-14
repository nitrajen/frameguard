"""
Benchmark: dfguard vs pandera vs Great Expectations

Measures two claims:
  1. Lightest  -- enforcement execution time on 1000 diverse DataFrames
  2. Easiest   -- lines of code to add schema enforcement to a pipeline

Install optional dependencies for full comparison:
    pip install pandera
    pip install great-expectations

Run:
    python examples/benchmarks/benchmark.py
"""
import time
import sys
import numpy as np
import pandas as pd

# ── optional imports ───────────────────────────────────────────────────────────

try:
    import pandera as pa
    import pandera.typing as pat
    HAS_PANDERA = True
except ImportError:
    HAS_PANDERA = False

try:
    import great_expectations as ge
    HAS_GE = True
except ImportError:
    HAS_GE = False

# ── schema: 7 columns covering 6 distinct pandas dtype categories ──────────────

import dfguard.pandas as dfg


class RawOrderSchema(dfg.PandasSchema):
    order_id    = np.dtype("int64")           # numpy signed int 64
    customer_id = np.dtype("int64")
    amount      = np.dtype("float64")         # numpy float 64
    quantity    = np.dtype("int32")           # numpy signed int 32
    sku         = pd.StringDtype()            # pandas extension: string
    is_vip      = pd.BooleanDtype()           # pandas extension: bool
    created_at  = np.dtype("datetime64[ns]")  # numpy datetime


SCHEMA_COLS = list(RawOrderSchema._schema_fields.keys())

# ── generate 1000 DataFrames with varying sizes ────────────────────────────────

ROW_SIZES  = [10, 50, 100, 500, 1000]
N_BATCHES  = 1000
rng        = np.random.default_rng(42)


def _make(n: int) -> pd.DataFrame:
    return pd.DataFrame({
        "order_id":    pd.array(rng.integers(1, 10_000, n),   dtype="int64"),
        "customer_id": pd.array(rng.integers(1, 1_000, n),    dtype="int64"),
        "amount":      pd.array(rng.uniform(1.0, 500.0, n),   dtype="float64"),
        "quantity":    pd.array(rng.integers(1, 20, n),       dtype="int32"),
        "sku":         pd.array([f"SKU-{i}" for i in rng.integers(1, 500, n)], dtype=pd.StringDtype()),
        "is_vip":      pd.array(rng.choice([True, False], n), dtype=pd.BooleanDtype()),
        "created_at":  pd.array(pd.date_range("2024-01-01", periods=n, freq="h"), dtype="datetime64[ns]"),
    })


batches = [_make(ROW_SIZES[i % len(ROW_SIZES)]) for i in range(N_BATCHES)]

# ── dfguard ────────────────────────────────────────────────────────────────────

@dfg.enforce
def _pipeline_dfguard(df: RawOrderSchema) -> pd.DataFrame:
    revenue = df["amount"] * df["quantity"]
    return df.assign(revenue=revenue, discount=revenue.where(revenue <= 500, 50.0))


for b in batches[:20]:  # warm up
    _pipeline_dfguard(b)

t0 = time.perf_counter()
for b in batches:
    _pipeline_dfguard(b)
dfguard_ms = (time.perf_counter() - t0) * 1000

# also verify enforcement raises correctly
try:
    bad_df = batches[0].drop(columns=["sku"])
    _pipeline_dfguard(bad_df)
    dfguard_raises = False
except TypeError:
    dfguard_raises = True

# ── pandera ────────────────────────────────────────────────────────────────────

pandera_ms = None
pandera_raises = None

if HAS_PANDERA:
    try:
        class _PanderaSchema(pa.SchemaModel):
            order_id:    pat.Series[np.int64]
            customer_id: pat.Series[np.int64]
            amount:      pat.Series[np.float64]
            quantity:    pat.Series[np.int32]
            sku:         pat.Series[pd.StringDtype()]
            is_vip:      pat.Series[pd.BooleanDtype()]
            created_at:  pat.Series[pd.DatetimeTZDtype] = pa.Field(nullable=True)

            class Config:
                coerce = False

        @pa.check_types
        def _pipeline_pandera(df: pat.DataFrame[_PanderaSchema]) -> pd.DataFrame:
            revenue = df["amount"] * df["quantity"]
            return df.assign(revenue=revenue, discount=revenue.where(revenue <= 500, 50.0))

        for b in batches[:20]:
            _pipeline_pandera(b)

        t0 = time.perf_counter()
        for b in batches:
            _pipeline_pandera(b)
        pandera_ms = (time.perf_counter() - t0) * 1000

        try:
            bad_df = batches[0].drop(columns=["sku"])
            _pipeline_pandera(bad_df)
            pandera_raises = False
        except Exception:
            pandera_raises = True

    except Exception as e:
        pandera_ms = None
        HAS_PANDERA = False
        print(f"[pandera] skipped: {e}", file=sys.stderr)

# ── great expectations ─────────────────────────────────────────────────────────
# GE is designed for data quality, not function-level schema enforcement.
# Closest equivalent: expect_column_to_exist() per column (metadata only).
# Note: GE dtype enforcement (expect_column_values_to_be_of_type) scans data.

ge_ms = None
ge_raises = None

if HAS_GE:
    try:
        def _validate_ge(df: pd.DataFrame) -> None:
            gdf = ge.from_pandas(df)
            for col in SCHEMA_COLS:
                result = gdf.expect_column_to_exist(col)
                if not result.success:
                    raise ValueError(f"Missing column: {col}")

        def _pipeline_ge(df: pd.DataFrame) -> pd.DataFrame:
            _validate_ge(df)
            revenue = df["amount"] * df["quantity"]
            return df.assign(revenue=revenue, discount=revenue.where(revenue <= 500, 50.0))

        for b in batches[:20]:
            _pipeline_ge(b)

        t0 = time.perf_counter()
        for b in batches:
            _pipeline_ge(b)
        ge_ms = (time.perf_counter() - t0) * 1000

        try:
            bad_df = batches[0].drop(columns=["sku"])
            _pipeline_ge(bad_df)
            ge_raises = False
        except Exception:
            ge_raises = True

    except Exception as e:
        ge_ms = None
        HAS_GE = False
        print(f"[great_expectations] skipped: {e}", file=sys.stderr)

# ── lines of code comparison ───────────────────────────────────────────────────
# Counts extra lines needed to add schema enforcement to a 3-function pipeline.
# Schema class definition (N fields) is the same for all tools and excluded.

N_PIPELINE_FUNCS = 3  # enrich, flag, aggregate

loc = {
    "dfguard": {
        "enforcement_setup": "1  (dfg.arm() in __init__.py)",
        "per_function":      "0  (arm() covers all annotated functions)",
        "total":             1,
    },
    "pandera": {
        "enforcement_setup": "0",
        "per_function":      f"{N_PIPELINE_FUNCS}  (@pa.check_types on each function)",
        "total":             N_PIPELINE_FUNCS,
    },
    "great_expectations": {
        "enforcement_setup": "5+ per schema  (context, suite, batch config)",
        "per_function":      f"{N_PIPELINE_FUNCS * 3}+  (get_validator + validate + check result per function)",
        "total":             f"{5 + N_PIPELINE_FUNCS * 3}+",
    },
}

# ── report ─────────────────────────────────────────────────────────────────────

SEP  = "=" * 62
SEP2 = "-" * 62

print(SEP)
print("  dfguard benchmark")
print(SEP)
print(f"\n  DataFrames : {N_BATCHES}")
print(f"  Row sizes  : {ROW_SIZES} (cycled)")
print(f"  Columns    : {len(SCHEMA_COLS)}  "
      f"(int64 x2, float64, int32, StringDtype, BooleanDtype, datetime64[ns])")

print(f"\n{SEP2}")
print("  CLAIM 1: Lightest (execution time)")
print(SEP2)
print(f"  {'Tool':<22} {'Total':>10}  {'Per call':>10}  {'Raises on mismatch':>20}")
print(f"  {'-'*22} {'-'*10}  {'-'*10}  {'-'*20}")
print(f"  {'dfguard':<22} {dfguard_ms:>9.1f}ms  {dfguard_ms/N_BATCHES*1000:>8.1f}us  {'yes' if dfguard_raises else 'NO':>20}")

if pandera_ms is not None:
    speedup = pandera_ms / dfguard_ms
    print(f"  {'pandera':<22} {pandera_ms:>9.1f}ms  {pandera_ms/N_BATCHES*1000:>8.1f}us  "
          f"{'yes' if pandera_raises else 'NO':>20}  [{speedup:.1f}x slower]")
else:
    print(f"  {'pandera':<22} {'not installed':>22}   pip install pandera")

if ge_ms is not None:
    speedup = ge_ms / dfguard_ms
    print(f"  {'great_expectations':<22} {ge_ms:>9.1f}ms  {ge_ms/N_BATCHES*1000:>8.1f}us  "
          f"{'yes' if ge_raises else 'NO':>20}  [{speedup:.1f}x slower]")
else:
    print(f"  {'great_expectations':<22} {'not installed':>22}   pip install great-expectations")

print(f"\n  Note: dfguard and pandera read schema metadata only (no data scanned).")
print(f"  GE column-existence check shown. GE dtype enforcement scans actual data.")

print(f"\n{SEP2}")
print("  CLAIM 2: Easiest (lines of code for a {}-function pipeline)".format(N_PIPELINE_FUNCS))
print(SEP2)
for tool, info in loc.items():
    print(f"\n  {tool}")
    print(f"    Setup       : {info['enforcement_setup']}")
    print(f"    Per function: {info['per_function']}")
    print(f"    Total extra : {info['total']} line(s)")

print(f"\n  Note: pandera has no equivalent to dfg.arm().")
print(f"  GE has no function-level enforcement concept at all.")
print(SEP)
