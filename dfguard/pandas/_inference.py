"""
dfguard.pandas._inference
~~~~~~~~~~~~~~~~~~~~~~~~~
``infer_schema(df)``: inspect a live DataFrame and generate a PandasSchema
subclass with the correct types.

The generated class can immediately be used for validation::

    schema = infer_schema(df, name="OrderSchema")
    print(schema.to_code())    # copy-paste into your codebase
    schema.assert_valid(other_df)
"""

from __future__ import annotations

from typing import Any


def infer_schema(df: Any, name: str = "InferredSchema") -> type:
    """
    Inspect ``df`` (a pandas DataFrame or dataset wrapper) and return a
    ``PandasSchema`` subclass that exactly matches its current schema.

    Also prints the Python code so developers can copy it into their codebase.
    Column names that are not valid Python identifiers are sanitized and wrapped
    with ``alias()`` automatically.

    Parameters
    ----------
    df:
        A live ``pandas.DataFrame`` or ``dataset`` wrapper.
    name:
        Name to give the generated class.

    Returns
    -------
    type[PandasSchema]
        A fully usable PandasSchema subclass.
    """
    import pandas as pd

    from dfguard.pandas.dataset import _PandasDatasetBase
    from dfguard.pandas.schema import PandasSchema

    if type.__instancecheck__(_PandasDatasetBase, df):
        raw = df._df
    elif isinstance(df, pd.DataFrame):
        raw = df
    else:
        raise TypeError(f"Expected a pandas DataFrame, got {type(df).__name__}")

    dtypes: dict[str, Any] = dict(raw.dtypes)
    schema_class = PandasSchema.from_dtype_dict(dtypes, name=name)

    print(schema_class.to_code())
    return schema_class


def print_schema(df: Any, name: str = "GeneratedSchema") -> None:
    """Print a ``PandasSchema`` class definition ready to paste into your codebase.

    Column names that are not valid Python identifiers are sanitized and wrapped
    with ``alias()`` automatically.

    Parameters
    ----------
    df:
        A live ``pandas.DataFrame`` or ``dataset`` wrapper.
    name:
        Name to give the generated class.
    """
    infer_schema(df, name=name)
