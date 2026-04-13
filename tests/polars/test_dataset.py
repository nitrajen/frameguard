"""schema_of, dataset, _PolarsDatasetBase — Polars backend."""

import polars as pl
import pytest

from dfguard.polars import PolarsSchema, dataset, enforce, schema_of
from dfguard.polars.dataset import _PolarsDatasetBase
from dfguard.polars.exceptions import SchemaValidationError


class OrderSchema(PolarsSchema):
    order_id: pl.Int64
    amount:   pl.Float64
    quantity: pl.Int32


# ── schema_of ─────────────────────────────────────────────────────────────────

def test_schema_of_exact_match_and_rejects_extra(order_df, enriched_df):
    T = schema_of(order_df)
    assert isinstance(order_df, T)
    assert not isinstance(enriched_df, T)
    assert not isinstance(order_df.select("order_id"), T)


def test_schema_of_with_enforce(order_df, enriched_df):
    T = schema_of(order_df)

    @enforce
    def process(df: T): return df  # type: ignore[valid-type]

    process(order_df)
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


def test_schema_of_lazyframe(order_df):
    T = schema_of(order_df.lazy())
    assert isinstance(order_df.lazy(), T)


# ── dataset wrapper ───────────────────────────────────────────────────────────

def test_dataset_basic_properties(order_df):
    ds = dataset(order_df)
    assert isinstance(ds, _PolarsDatasetBase)
    assert dict(ds.schema) == dict(order_df.schema)
    assert list(ds.columns) == list(order_df.columns)


def test_with_columns_adds_to_schema(order_df):
    ds = dataset(order_df)
    ds2 = ds.with_columns(revenue=pl.col("amount") * pl.col("quantity"))
    assert "revenue" in ds2.columns
    assert "revenue" not in ds.columns


def test_rename_updates_columns(order_df):
    ds = dataset(order_df)
    ds2 = ds.rename({"order_id": "id"})
    assert "id" in ds2.columns and "order_id" not in ds2.columns


def test_drop_removes_column(order_df):
    ds = dataset(order_df)
    ds2 = ds.drop("amount")
    assert "amount" not in ds2.columns and "order_id" in ds2.columns


def test_select_keeps_only_requested(order_df):
    assert list(dataset(order_df).select("order_id").columns) == ["order_id"]


def test_join_adds_columns(order_df):
    extra = pl.DataFrame({
        "order_id": pl.Series([1, 2, 3], dtype=pl.Int64),
        "region":   ["US", "EU", "US"],
    })
    ds2 = dataset(order_df).join(extra, on="order_id")
    assert "region" in ds2.columns


def test_group_by_returns_dataset(order_df):
    ds2 = dataset(order_df).group_by("order_id").agg(pl.col("amount").sum())
    assert isinstance(ds2, _PolarsDatasetBase)


# ── schema history ────────────────────────────────────────────────────────────

def test_history_records_operations(order_df):
    ds = dataset(order_df)
    assert ds.schema_history.changes[0].operation == "input"

    ds2 = ds.with_columns(revenue=pl.col("amount") * pl.col("quantity"))
    assert len(ds2.schema_history) == 2
    assert "with_columns" in ds2.schema_history.changes[-1].operation

    ds3 = ds2.drop("amount")
    assert "amount" in ds3.schema_history.changes[-1].dropped


# ── validate (chainable) ─────────────────────────────────────────────────────

def test_dataset_validate_passes_and_raises(order_df):
    ds = dataset(order_df)
    ds.validate(OrderSchema)

    class BigSchema(PolarsSchema):
        order_id: pl.Int64
        amount:   pl.Float64
        quantity: pl.Int32
        revenue:  pl.Float64

    with pytest.raises(SchemaValidationError):
        ds.validate(BigSchema)


def test_assert_columns_and_type(order_df):
    ds = dataset(order_df)
    ds.assert_columns("order_id", "amount")
    ds.assert_column_type("order_id", pl.Int64)

    with pytest.raises(SchemaValidationError, match="revenue"):
        ds.assert_columns("revenue")
    with pytest.raises(SchemaValidationError):
        ds.assert_column_type("order_id", pl.Float64)
