"""Shared fixtures for dfguard.pandas tests."""

import numpy as np
import pandas as pd
import pytest


@pytest.fixture()
def order_df() -> pd.DataFrame:
    """A simple orders DataFrame with numpy-backed dtypes."""
    return pd.DataFrame({
        "order_id": np.array([1, 2, 3], dtype="int64"),
        "amount":   np.array([10.0, 5.0, 8.5], dtype="float64"),
        "quantity": np.array([2, 7, 3], dtype="int64"),
    })


@pytest.fixture()
def enriched_df(order_df: pd.DataFrame) -> pd.DataFrame:
    """order_df plus a computed 'revenue' column (numpy float64)."""
    df = order_df.copy()
    df["revenue"] = (df["amount"] * df["quantity"]).astype("float64")
    return df


@pytest.fixture()
def string_df() -> pd.DataFrame:
    """DataFrame with object-dtype string column and StringDtype column."""
    return pd.DataFrame({
        "name": pd.Series(["alice", "bob"], dtype="object"),
        "city": pd.array(["NY", "LA"], dtype=pd.StringDtype()),
    })


@pytest.fixture()
def mixed_df() -> pd.DataFrame:
    """DataFrame covering many dtype kinds."""
    return pd.DataFrame({
        "id":       np.array([1, 2], dtype="int64"),
        "score":    np.array([1.5, 2.5], dtype="float64"),
        "flag":     np.array([True, False], dtype="bool"),
        "label":    pd.Series(["a", "b"], dtype="object"),
        "nullable": pd.array([1, None], dtype=pd.Int64Dtype()),
    })
