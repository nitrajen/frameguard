"""enforce() decorator: subset, disarm/arm, arm() — Polars backend."""

import inspect
import warnings

import polars as pl
import pytest

import dfguard.polars._enforcement as _e
from dfguard.polars import PolarsSchema, enforce
from dfguard.polars._enforcement import disarm


class RawSchema(PolarsSchema):
    order_id: pl.Int64
    amount:   pl.Float64


class EnrichedSchema(RawSchema):
    revenue: pl.Float64


@pytest.fixture(autouse=True)
def reset_state():
    _e._ENABLED = True
    _e._SUBSET  = True
    yield
    _e._ENABLED = True
    _e._SUBSET  = True


@pytest.fixture()
def raw_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": pl.Series([1], dtype=pl.Int64),
        "amount":   pl.Series([1.0], dtype=pl.Float64),
    })


@pytest.fixture()
def enriched_df(raw_df: pl.DataFrame) -> pl.DataFrame:
    return raw_df.with_columns(revenue=pl.lit(2.0))


# ── subset=True (default) ────────────────────────────────────────────────────

def test_subset_true_passes_exact_schema(raw_df):
    @enforce
    def process(df: RawSchema): return df
    process(raw_df)


def test_subset_true_passes_with_extra_columns(enriched_df):
    @enforce
    def process(df: RawSchema): return df
    process(enriched_df)


def test_subset_true_rejects_missing_columns(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)


# ── subset=False ──────────────────────────────────────────────────────────────

def test_subset_false_passes_exact_schema(raw_df):
    @enforce(subset=False)
    def process(df: RawSchema): return df
    process(raw_df)


def test_subset_false_rejects_extra_columns(enriched_df):
    @enforce(subset=False)
    def process(df: RawSchema): return df
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


# ── global _SUBSET override ───────────────────────────────────────────────────

def test_global_subset_false_rejects_extra_columns(enriched_df):
    _e._SUBSET = False

    @enforce
    def process(df: RawSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


def test_function_level_overrides_global(enriched_df):
    _e._SUBSET = False

    @enforce(subset=True)
    def process(df: RawSchema): return df

    process(enriched_df)


# ── non-schema args pass through ─────────────────────────────────────────────

def test_non_schema_args_pass_through(raw_df):
    @enforce
    def process(df: RawSchema, label: str, limit: int = 10): return df
    process(raw_df, "hello", limit=5)


def test_no_schema_params_returns_original_function():
    def plain(x: int, y: str): return y
    assert enforce(plain) is plain


# ── disarm / re-enable ────────────────────────────────────────────────────────

def test_disarm_silences_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df
    disarm()
    process(raw_df)


def test_arm_restores_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df
    disarm()
    process(raw_df)
    _e._ENABLED = True
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)


# ── arm() warns in __main__ ───────────────────────────────────────────────────

def test_arm_warns_in_main(monkeypatch):
    fake_frame_globals = {"__package__": None, "__name__": "__main__"}

    class FakeInnerFrame:
        f_globals = fake_frame_globals

    class FakeOuterFrame:
        f_back = FakeInnerFrame()

    monkeypatch.setattr(inspect, "currentframe", lambda: FakeOuterFrame())
    _e._ARMED = set()

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _e.arm()
        assert len(w) == 1
        assert "dfguard.polars.arm" in str(w[0].message)
