"""PyArrow-backed pandas schema tests. Skipped if pyarrow is not available."""

import numpy as np
import pandas as pd
import pytest

pa = pytest.importorskip("pyarrow", exc_type=ImportError)

from dfguard.pandas import PandasSchema  # noqa: E402
from dfguard.pandas.exceptions import SchemaValidationError  # noqa: E402
from dfguard.pandas.types import annotation_to_pandas_dtype  # noqa: E402

# ── Type conversion ───────────────────────────────────────────────────────────

def test_arrow_dtype_passthrough():
    dtype = pd.ArrowDtype(pa.int64())
    result, nullable = annotation_to_pandas_dtype(dtype)
    assert result == dtype
    assert nullable is True


def test_arrow_dtype_list_of_struct_preserved():
    inner = pa.list_(pa.struct([pa.field("sku", pa.string()), pa.field("qty", pa.int32())]))
    dtype = pd.ArrowDtype(inner)
    result, _ = annotation_to_pandas_dtype(dtype)
    assert result == dtype


def test_arrow_dtype_three_level_nested_preserved():
    inner = pa.list_(pa.struct([
        pa.field("sku",   pa.string()),
        pa.field("qty",   pa.int32()),
        pa.field("attrs", pa.list_(pa.struct([
            pa.field("key",   pa.string()),
            pa.field("value", pa.string()),
        ]))),
    ]))
    result, _ = annotation_to_pandas_dtype(pd.ArrowDtype(inner))
    assert result == pd.ArrowDtype(inner)


# ── Schema definition ─────────────────────────────────────────────────────────

_ITEM_TYPE = pa.list_(pa.struct([
    pa.field("sku",   pa.string()),
    pa.field("qty",   pa.int32()),
    pa.field("attrs", pa.list_(pa.struct([
        pa.field("key",   pa.string()),
        pa.field("value", pa.string()),
    ]))),
]))


class ArrowSchema(PandasSchema):
    order_id:   np.dtype("int64")
    label:      pd.StringDtype()
    line_items: pd.ArrowDtype(_ITEM_TYPE)


def _make_df(item_type=_ITEM_TYPE):
    return pd.DataFrame({
        "order_id":   np.array([1], dtype="int64"),
        "label":      pd.array(["x"], dtype=pd.StringDtype()),
        "line_items": pd.Series(
            [[{"sku": "A", "qty": 1, "attrs": [{"key": "color", "value": "red"}]}]],
            dtype=pd.ArrowDtype(item_type),
        ),
    })


# ── Validation: passes ────────────────────────────────────────────────────────

def test_isinstance_nested_arrow_passes():
    assert isinstance(_make_df(), ArrowSchema)


def test_validate_nested_arrow_passes():
    assert ArrowSchema.validate(_make_df()) == []


# ── Validation: failures ──────────────────────────────────────────────────────

def test_isinstance_wrong_inner_type_fails():
    wrong = pa.list_(pa.struct([
        pa.field("sku", pa.string()),
        pa.field("qty", pa.string()),   # int32 expected, string given
        pa.field("attrs", pa.list_(pa.struct([
            pa.field("key",   pa.string()),
            pa.field("value", pa.string()),
        ]))),
    ]))
    assert not isinstance(_make_df(wrong), ArrowSchema)


def test_validate_wrong_inner_type_reports_column():
    wrong = pa.list_(pa.struct([pa.field("sku", pa.string())]))
    errors = ArrowSchema.validate(_make_df(wrong))
    assert any("line_items" in str(e) for e in errors)


def test_assert_valid_raises_with_column_name():
    wrong = pa.list_(pa.struct([pa.field("sku", pa.string())]))
    with pytest.raises(SchemaValidationError) as exc_info:
        ArrowSchema.assert_valid(_make_df(wrong))
    assert "line_items" in str(exc_info.value)
