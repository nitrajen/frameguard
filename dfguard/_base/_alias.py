"""Per-variable column-name alias for schema fields."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class alias:
    """Map a Python identifier to a DataFrame column with a different name.

    Use when the column name is not a valid Python identifier or differs from
    the attribute name you want to declare::

        from dfguard.pandas import PandasSchema, alias
        import numpy as np

        class OrderSchema(PandasSchema):
            first_name    = alias("First Name",   np.dtype('object'))
            order_id      = alias("order-id",     np.dtype('int64'))
            _2023_revenue = alias("2023_revenue",  np.dtype('float64'))
            revenue       = np.dtype('float64')   # no alias needed

    Parameters
    ----------
    src:
        The actual column name in the DataFrame.
    dtype:
        The dtype annotation (same as you would use in annotation form).
    """

    src: str
    dtype: Any
