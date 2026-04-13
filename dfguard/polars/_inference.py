"""dfguard.polars._inference — infer_schema and print_schema for Polars DataFrames."""

from __future__ import annotations

from typing import Any


def infer_schema(df: Any, name: str = "InferredSchema") -> type:
    """
    Inspect ``df`` and return a ``PolarsSchema`` subclass matching its schema.

    Also prints the Python code for copy-pasting into a codebase.
    Column names that are not valid Python identifiers are sanitized and wrapped
    with ``alias()`` automatically.
    """
    import polars as pl

    from dfguard.polars.dataset import _PolarsDatasetBase
    from dfguard.polars.schema import PolarsSchema

    if type.__instancecheck__(_PolarsDatasetBase, df):
        raw = df._df
    elif isinstance(df, pl.DataFrame):
        raw = df
    elif isinstance(df, pl.LazyFrame):
        raw = df.collect()
    else:
        raise TypeError(f"Expected a Polars DataFrame or LazyFrame, got {type(df).__name__}")

    schema_cls = PolarsSchema.from_struct(dict(raw.schema), name=name)
    print(schema_cls.to_code())
    return schema_cls


def print_schema(df: Any, name: str = "GeneratedSchema") -> None:
    """Print a ``PolarsSchema`` class definition ready to paste into your codebase.

    Column names that are not valid Python identifiers are sanitized and wrapped
    with ``alias()`` automatically.
    """
    infer_schema(df, name=name)
