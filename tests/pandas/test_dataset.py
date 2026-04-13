"""schema_of, dataset, _PandasDatasetBase mutation tracking."""

import numpy as np
import pandas as pd
import pytest

from dfguard.pandas import PandasSchema, dataset, enforce, schema_of
from dfguard.pandas.dataset import _PandasDatasetBase
from dfguard.pandas.exceptions import SchemaValidationError


class OrderSchema(PandasSchema):
    order_id: np.dtype("int64")
    amount:   np.dtype("float64")


# ── schema_of ─────────────────────────────────────────────────────────────────

def test_schema_of_exact_match_and_rejects_extras(order_df, enriched_df):
    T = schema_of(order_df)
    assert isinstance(order_df, T)
    assert not isinstance(enriched_df, T)
    assert not isinstance(order_df[["order_id"]], T)


def test_schema_of_with_enforce(order_df, enriched_df):
    T = schema_of(order_df)

    @enforce
    def process(df: T): return df  # type: ignore[valid-type]

    process(order_df)
    with pytest.raises(TypeError, match="Schema mismatch"):
        process(enriched_df)


# ── dataset wrapper ───────────────────────────────────────────────────────────

def test_dataset_basic_properties(order_df):
    ds = dataset(order_df)
    assert isinstance(ds, _PandasDatasetBase)
    assert dict(ds.dtypes) == dict(order_df.dtypes)
    assert list(ds.columns) == list(order_df.columns)


def test_assign_adds_column(order_df):
    ds  = dataset(order_df)
    ds2 = ds.assign(revenue=ds._df["amount"] * 2)
    assert "revenue" in ds2.columns and "revenue" not in ds.columns


def test_rename_updates_columns(order_df):
    ds2 = dataset(order_df).rename({"order_id": "id"})
    assert "id" in ds2.columns and "order_id" not in ds2.columns


def test_drop_removes_column(order_df):
    ds2 = dataset(order_df).drop("amount")
    assert "amount" not in ds2.columns and "order_id" in ds2.columns


def test_select_keeps_only_requested(order_df):
    assert list(dataset(order_df).select("order_id").columns) == ["order_id"]


def test_merge_adds_columns(order_df):
    extra = pd.DataFrame({
        "order_id": pd.array([1, 2, 3], dtype="int64"),
        "region": ["US", "EU", "US"],
    })
    ds2 = dataset(order_df).merge(extra, on="order_id")
    assert "region" in ds2.columns


def test_groupby_agg_returns_dataset(order_df):
    ds2 = dataset(order_df).groupby("order_id").agg({"amount": "sum"})
    assert isinstance(ds2, _PandasDatasetBase)


# ── schema history ────────────────────────────────────────────────────────────

def test_history_records_operations(order_df):
    ds = dataset(order_df)
    assert ds.schema_history.changes[0].operation == "input"

    ds2 = ds.assign(revenue=ds._df["amount"] * 2)
    assert len(ds2.schema_history) == 2
    assert "assign" in ds2.schema_history.changes[-1].operation

    ds3 = ds2.drop("amount")
    assert "amount" in ds3.schema_history.changes[-1].dropped


# ── validate + assert helpers ─────────────────────────────────────────────────

def test_dataset_validate_passes_and_raises(order_df):
    ds = dataset(order_df)
    ds.validate(OrderSchema)

    class BigSchema(PandasSchema):
        order_id: np.dtype("int64")
        amount:   np.dtype("float64")
        revenue:  np.dtype("float64")

    with pytest.raises(SchemaValidationError):
        ds.validate(BigSchema)


def test_assert_columns_and_type(order_df):
    ds = dataset(order_df)
    ds.assert_columns("order_id", "amount")
    ds.assert_column_type("order_id", np.dtype("int64"))

    with pytest.raises(SchemaValidationError, match="revenue"):
        ds.assert_columns("revenue")
    with pytest.raises(SchemaValidationError):
        ds.assert_column_type("order_id", np.dtype("float64"))
