"""Shared fixtures for dfguard.polars tests."""

import polars as pl
import pytest


@pytest.fixture()
def order_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": pl.Series([1, 2, 3], dtype=pl.Int64),
        "amount":   pl.Series([10.0, 5.0, 8.5], dtype=pl.Float64),
        "quantity": pl.Series([2, 7, 3], dtype=pl.Int32),
    })


@pytest.fixture()
def enriched_df(order_df: pl.DataFrame) -> pl.DataFrame:
    return order_df.with_columns(
        (pl.col("amount") * pl.col("quantity")).alias("revenue")
    )


@pytest.fixture()
def nested_df() -> pl.DataFrame:
    return pl.DataFrame({
        "order_id": pl.Series([1, 2], dtype=pl.Int64),
        "tags":     pl.Series([["a", "b"], ["c"]], dtype=pl.List(pl.String)),
        "scores":   pl.Series([[1, 2], [3]], dtype=pl.List(pl.Int64)),
    })
