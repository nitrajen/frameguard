"""
dfguard.pandas: runtime schema enforcement for pandas DataFrames.

.. note::
    Requires **pandas ≥ 2.0** and **Python ≥ 3.11**.

Two-line setup for packages
----------------------------
::

    from dfguard.pandas import schema_of, arm

    RawSchema = schema_of(raw_df)

    def enrich(df: RawSchema): ...
    def clean(df: RawSchema): ...

    arm()                              # enforces every annotated function above

For scripts and notebooks use ``@enforce`` per function::

    from dfguard.pandas import schema_of, enforce

    RawSchema = schema_of(raw_df)

    @enforce
    def enrich(df: RawSchema): ...

Declaring schemas upfront (no live DataFrame required)
------------------------------------------------------
::

    import numpy as np
    import pandas as pd
    from dfguard.pandas import PandasSchema, Optional, enforce

    class OrderSchema(PandasSchema):
        order_id:  np.dtype("int64")
        amount:    np.dtype("float64")
        name:      pd.StringDtype()
        active:    pd.BooleanDtype()
        tags:      list[str]                    # object dtype, holds lists of str
        zip_code:  Optional[pd.StringDtype()]   # nullable

    @enforce
    def process(df: OrderSchema): ...           # subset matching: df must have these columns

Public API
----------
``schema_of(df)``
    Capture ``df``'s schema as a type. Exact match required.

``dataset(df)``
    Wrap ``df`` in a tracked instance. Every ``assign``, ``drop``,
    ``rename``, etc. is recorded in ``schema_history``.

``arm()``
    Apply schema enforcement to every annotated function in the calling module.

``disarm()``
    Turn off all enforcement globally. Call ``arm()`` to re-enable.

``enforce``
    Per-function decorator. Only checks schema-annotated args.

``PandasSchema``
    Declare a schema as a class using numpy/pandas dtypes.
    Subset matching: df must have at least the declared columns.

``Optional``
    Nullable field marker for dtype instance annotations
    (e.g. ``Optional[pd.StringDtype()]``). For numpy scalar types,
    the native ``np.int64 | None`` syntax also works.
"""

try:
    import pandas  # noqa: F401
except ImportError as _e:
    raise ImportError(
        "dfguard's pandas integration requires pandas. "
        "Install it with: pip install 'dfguard[pandas]'"
    ) from _e

from dfguard._base._alias import alias
from dfguard._nullable import Optional
from dfguard.pandas._enforcement import arm, disarm, enforce
from dfguard.pandas._inference import infer_schema, print_schema
from dfguard.pandas.dataset import TypedGroupBy, _PandasDatasetBase, schema_of
from dfguard.pandas.dataset import _make_dataset as dataset
from dfguard.pandas.exceptions import (
    ColumnNotFoundError,
    DfTypesError,
    SchemaValidationError,
    TypeAnnotationError,
)
from dfguard.pandas.history import PandasSchemaChange, PandasSchemaHistory
from dfguard.pandas.schema import PandasSchema

__all__ = [
    "schema_of",
    "dataset",
    "enforce",
    "arm",
    "disarm",
    "_PandasDatasetBase",
    "TypedGroupBy",
    "PandasSchema",
    "Optional",
    "alias",
    "PandasSchemaChange",
    "PandasSchemaHistory",
    "DfTypesError",
    "SchemaValidationError",
    "TypeAnnotationError",
    "ColumnNotFoundError",
    "infer_schema",
    "print_schema",
]
