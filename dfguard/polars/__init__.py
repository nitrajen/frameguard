"""
dfguard.polars
~~~~~~~~~~~~~~
Runtime schema enforcement for **Polars** DataFrames.

Requires **Polars ≥ 1.0** and **Python ≥ 3.11**.

Quick start::

    import polars as pl
    from dfguard.polars import PolarsSchema, Optional, enforce

    class OrderSchema(PolarsSchema):
        order_id: pl.Int64
        amount:   pl.Float64
        tags:     pl.List(pl.String)    # first-class nested type
        name:     Optional[pl.String]   # explicitly nullable

    @enforce
    def process(df: OrderSchema) -> pl.DataFrame:
        ...

``schema_of`` captures an exact schema from a live DataFrame::

    T = schema_of(df)

    @enforce
    def write_final(df: T): ...   # rejects DataFrames with extra or missing columns

``dataset`` wraps a DataFrame and tracks schema evolution::

    ds = dataset(df)
    ds2 = ds.with_columns(revenue=pl.col("amount") * pl.col("quantity"))
    ds2.schema_history.print()

.. note::
    Requires **Polars ≥ 1.0** and **Python ≥ 3.11**.
"""

from dfguard._base._alias import alias
from dfguard._nullable import Optional
from dfguard.polars._enforcement import arm, disarm, enforce
from dfguard.polars._inference import infer_schema, print_schema
from dfguard.polars.dataset import _PolarsDatasetBase, _PolarsGroupBy, dataset, schema_of
from dfguard.polars.exceptions import (
    ColumnNotFoundError,
    DfTypesError,
    SchemaValidationError,
    TypeAnnotationError,
)
from dfguard.polars.history import PolarsSchemaChange, PolarsSchemaHistory
from dfguard.polars.schema import PolarsSchema

__all__ = [
    # Core
    "PolarsSchema",
    "Optional",
    "alias",
    "enforce",
    "arm",
    "disarm",
    # Dataset
    "schema_of",
    "dataset",
    "infer_schema",
    "print_schema",
    # History
    "PolarsSchemaChange",
    "PolarsSchemaHistory",
    # Exceptions
    "DfTypesError",
    "SchemaValidationError",
    "TypeAnnotationError",
    "ColumnNotFoundError",
    # Internal (for tests / advanced use)
    "_PolarsDatasetBase",
    "_PolarsGroupBy",
]
