"""enforce() decorator: subset, disarm/arm, arm()."""

import inspect
import warnings

import pytest
from pyspark.sql import types as T

import dfguard.pyspark._enforcement as _e
from dfguard.pyspark import SparkSchema, enforce
from dfguard.pyspark._enforcement import disarm


class RawSchema(SparkSchema):
    id:    T.LongType()
    value: T.DoubleType()


class EnrichedSchema(RawSchema):
    label: T.StringType()


@pytest.fixture(autouse=True)
def reset_state():
    """Reset global enforcement state before and after each test."""
    _e._ENABLED = True
    _e._SUBSET = True
    yield
    _e._ENABLED = True
    _e._SUBSET = True


@pytest.fixture()
def raw_df(spark):
    return spark.createDataFrame([(1, 1.0)], RawSchema.to_struct())


@pytest.fixture()
def enriched_df(spark):
    return spark.createDataFrame([(1, 1.0, "a")], EnrichedSchema.to_struct())


# ── subset=True (default): extra columns are fine ────────────────────────────

def test_subset_true_passes_exact_schema(raw_df):
    @enforce
    def process(df: RawSchema): return df

    process(raw_df)  # exact match: always passes


def test_subset_true_passes_with_extra_columns(enriched_df):
    @enforce                            # subset=True by default
    def process(df: RawSchema): return df

    process(enriched_df)  # enriched has extra 'label' column: should pass


def test_subset_true_still_rejects_missing_columns(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)  # raw_df is missing 'label'


# ── subset=False: exact match required ───────────────────────────────────────

def test_subset_false_passes_exact_schema(raw_df):
    @enforce(subset=False)
    def process(df: RawSchema): return df

    process(raw_df)  # exact match: passes


def test_subset_false_rejects_extra_columns(enriched_df):
    @enforce(subset=False)
    def process(df: RawSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)  # enriched has extra 'label': rejected


def test_subset_false_rejects_missing_columns(raw_df):
    @enforce(subset=False)
    def process(df: EnrichedSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(raw_df)


# ── global subset via _SUBSET, function-level overrides ──────────────────────

def test_global_subset_false_rejects_extra_columns(enriched_df):
    _e._SUBSET = False  # simulate dfg.arm(subset=False)

    @enforce          # no explicit subset: inherits global
    def process(df: RawSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


def test_function_level_overrides_global(enriched_df):
    _e._SUBSET = False  # global says exact

    @enforce(subset=True)   # function says subset: wins
    def process(df: RawSchema): return df

    process(enriched_df)  # passes despite global subset=False


def test_global_subset_true_with_function_override_false(enriched_df):
    _e._SUBSET = True   # global says subset

    @enforce(subset=False)   # function says exact: wins
    def process(df: RawSchema): return df

    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


# ── non-schema args are never touched ────────────────────────────────────────

def test_non_schema_args_pass_through(raw_df):
    @enforce
    def process(df: RawSchema, label: str, limit: int = 10): return df

    process(raw_df, "hello", limit=5)


def test_no_schema_params_returns_original_function():
    def plain(x: int, y: str): return y

    assert enforce(plain) is plain


# ── disarm() / arm() (re-enable) ─────────────────────────────────────────────

def test_disarm_silences_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df

    disarm()
    process(raw_df)  # would raise without disarm()


def test_arm_restores_enforcement(raw_df):
    @enforce
    def process(df: EnrichedSchema): return df

    disarm()
    process(raw_df)
    _e._ENABLED = True  # re-enable manually (arm() would re-walk, use flag directly)

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
    # Reset _ARMED so arm() attempts to walk
    _e._ARMED = False

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        _e.arm()
        assert len(w) == 1
        assert "dfguard.pyspark.arm" in str(w[0].message)
