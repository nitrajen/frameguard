"""enforce() decorator and global disable/enable."""

import inspect
import warnings

import pytest
from pyspark.sql import types as T

import frameguard.pyspark._enforcement as _e
from frameguard.pyspark import SparkSchema, enforce
from frameguard.pyspark._enforcement import disable, enable_enforcement


class RawSchema(SparkSchema):
    id: T.LongType()
    value: T.DoubleType()


class EnrichedSchema(RawSchema):
    label: T.StringType()


@pytest.fixture(autouse=True)
def reset_enforcement():
    """Ensure enforcement is on before and after each test."""
    enable_enforcement()
    yield
    enable_enforcement()


@pytest.fixture()
def raw_df(spark):
    return spark.createDataFrame([(1, 1.0)], RawSchema.to_struct())


@pytest.fixture()
def enriched_df(spark):
    return spark.createDataFrame([(1, 1.0, "a")], EnrichedSchema.to_struct())


# ── @enforce basic ────────────────────────────────────────────────────────────

def test_enforce_passes_correct_schema(raw_df):
    @enforce
    def process(df: RawSchema) -> RawSchema:
        return df

    process(raw_df)  # must not raise


def test_enforce_rejects_wrong_schema(raw_df, enriched_df):
    @enforce
    def process(df: EnrichedSchema) -> EnrichedSchema:
        return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)


def test_enforce_ignores_non_schema_args(raw_df):
    """Strings, ints, etc. must pass through without touching enforcement."""

    @enforce
    def process(df: RawSchema, label: str, limit: int = 10) -> RawSchema:
        return df

    process(raw_df, "hello", limit=5)  # must not raise


def test_enforce_no_schema_params_is_noop():
    """Functions with no schema-typed params return the original function unchanged."""

    def plain(x: int, y: str) -> str:
        return y

    wrapped = enforce(plain)
    assert wrapped is plain


# ── disable() / enable_enforcement() ─────────────────────────────────────────

def test_disable_silences_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema) -> EnrichedSchema:
        return df

    disable()
    process(raw_df)  # would raise without disable()


def test_enable_restores_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema) -> EnrichedSchema:
        return df

    disable()
    process(raw_df)  # no raise
    enable_enforcement()

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)


def test_disable_is_idempotent(raw_df):
    @enforce
    def process(df: EnrichedSchema) -> EnrichedSchema:
        return df

    disable()
    disable()
    process(raw_df)  # still no raise


# ── enforce(always=True) ──────────────────────────────────────────────────────

def test_always_true_enforces_when_globally_disabled(raw_df):
    @enforce(always=True)
    def critical(df: EnrichedSchema) -> EnrichedSchema:
        return df

    disable()

    with pytest.raises(TypeError, match="Schema mismatch"):
        critical(raw_df)


def test_always_true_does_not_affect_other_functions(raw_df):
    @enforce
    def normal(df: EnrichedSchema) -> EnrichedSchema:
        return df

    @enforce(always=True)
    def critical(df: EnrichedSchema) -> EnrichedSchema:
        return df

    disable()
    normal(raw_df)  # no raise — enforcement off

    with pytest.raises(TypeError):
        critical(raw_df)  # raises — always=True


def test_always_true_with_enforcement_enabled_still_raises(raw_df):
    @enforce(always=True)
    def critical(df: EnrichedSchema) -> EnrichedSchema:
        return df

    # enforcement is on (default)
    with pytest.raises(TypeError, match="Schema mismatch"):
        critical(raw_df)


# ── arm() warns in __main__ ───────────────────────────────────────────────────

def test_arm_warns_in_main(monkeypatch):
    fake_frame_globals = {"__package__": None, "__name__": "__main__"}

    class FakeInnerFrame:
        f_globals = fake_frame_globals

    class FakeOuterFrame:
        f_back = FakeInnerFrame()

    monkeypatch.setattr(inspect, "currentframe", lambda: FakeOuterFrame())

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _e.arm()
        assert len(w) == 1
        assert "enforce" in str(w[0].message).lower()
