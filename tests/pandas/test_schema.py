"""PandasSchema: definition, validation, isinstance, inheritance, to_code."""

import numpy as np
import pandas as pd
import pytest

from dfguard.pandas import Optional, PandasSchema
from dfguard.pandas.exceptions import SchemaValidationError, TypeAnnotationError

# ── Schema definitions used across tests ─────────────────────────────────────

class OrderSchema(PandasSchema):
    order_id: np.dtype("int64")
    amount:   np.dtype("float64")
    quantity: np.dtype("int64")


class EnrichedSchema(OrderSchema):
    revenue: np.dtype("float64")


class NullableSchema(PandasSchema):
    name:   pd.StringDtype()            # inherently nullable extension dtype
    score:  Optional[pd.Int64Dtype()]   # nullable int via dfguard Optional
    active: np.bool_ | None             # nullable bool via native union syntax


class ObjectSchema(PandasSchema):
    tags:     list[str]         # object dtype, holds lists
    metadata: dict[str, int]    # object dtype, holds dicts
    label:    str               # object dtype, holds strings


# ── Inheritance ───────────────────────────────────────────────────────────────

def test_child_inherits_parent_fields():
    assert "order_id" in EnrichedSchema._schema_fields
    assert "revenue"  in EnrichedSchema._schema_fields


def test_parent_unchanged_by_child():
    assert "revenue" not in OrderSchema._schema_fields


# ── to_dtype_dict ─────────────────────────────────────────────────────────────

def test_to_dtype_dict_returns_correct_dtypes():
    d = OrderSchema.to_dtype_dict()
    assert d["order_id"] == np.dtype("int64")
    assert d["amount"]   == np.dtype("float64")
    assert d["quantity"] == np.dtype("int64")


def test_to_dtype_dict_is_cached():
    a = OrderSchema.to_dtype_dict()
    b = OrderSchema.to_dtype_dict()
    assert a is b


def test_nullable_extension_dtype_in_dict():
    d = NullableSchema.to_dtype_dict()
    assert d["name"] == pd.StringDtype()
    assert d["score"] == pd.Int64Dtype()


def test_optional_assignment_form_collected():
    """Optional[dtype] in assignment form must appear in _schema_fields (not silently dropped)."""
    class AssignOpt(PandasSchema):
        x = np.dtype("int64")
        y = Optional[np.dtype("float64")]   # assignment form, not annotation form

    assert "y" in AssignOpt._schema_fields

    df_ok = pd.DataFrame({"x": pd.array([1], dtype="int64"), "y": pd.array([1.0], dtype="float64")})
    df_missing = pd.DataFrame({"x": pd.array([1], dtype="int64")})
    assert isinstance(df_ok, AssignOpt)
    assert not isinstance(df_missing, AssignOpt)


# ── isinstance (subset matching) ─────────────────────────────────────────────

def test_isinstance_exact_match(order_df):
    assert isinstance(order_df, OrderSchema)


def test_isinstance_subset_passes_extra_columns(enriched_df):
    """EnrichedSchema df still satisfies OrderSchema (subset)."""
    assert isinstance(enriched_df, OrderSchema)


def test_isinstance_fails_missing_column(order_df):
    assert not isinstance(order_df, EnrichedSchema)


def test_isinstance_fails_wrong_dtype():
    df = pd.DataFrame({"order_id": ["a", "b"], "amount": [1.0, 2.0], "quantity": [1, 2]})
    assert not isinstance(df, OrderSchema)


def test_isinstance_object_dtype_columns():
    # pandas 3.0 defaults string lists to StringDtype; use explicit dtype="object"
    df = pd.DataFrame({
        "tags":     pd.Series([["a", "b"], ["c"]], dtype="object"),
        "metadata": pd.Series([{"k": 1}, {}], dtype="object"),
        "label":    pd.Series(["x", "y"], dtype="object"),
    })
    assert isinstance(df, ObjectSchema)


# ── validate ─────────────────────────────────────────────────────────────────

def test_validate_returns_empty_list_on_success(order_df):
    assert OrderSchema.validate(order_df) == []


def test_validate_reports_missing_column(order_df):
    errors = EnrichedSchema.validate(order_df)
    assert any("revenue" in str(e) for e in errors)


def test_validate_reports_type_mismatch():
    df = pd.DataFrame({"order_id": [1.0, 2.0], "amount": [1.0, 2.0], "quantity": [1, 2]})
    errors = OrderSchema.validate(df)
    assert any("order_id" in str(e) for e in errors)


def test_validate_strict_rejects_extra_columns(enriched_df):
    errors = OrderSchema.validate(enriched_df, subset=False)
    assert any("revenue" in str(e) for e in errors)


def test_validate_non_strict_accepts_extra_columns(enriched_df):
    assert OrderSchema.validate(enriched_df, subset=True) == []


# ── assert_valid ─────────────────────────────────────────────────────────────

def test_assert_valid_passes(order_df):
    OrderSchema.assert_valid(order_df)  # no exception


def test_assert_valid_raises_on_mismatch(order_df):
    with pytest.raises(SchemaValidationError) as exc_info:
        EnrichedSchema.assert_valid(order_df)
    assert "revenue" in str(exc_info.value)



# ── empty and astype construction ─────────────────────────────────────────────

def test_empty_creates_zero_row_dataframe():
    df = OrderSchema.empty()
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 0
    assert list(df.columns) == ["order_id", "amount", "quantity"]
    assert df["order_id"].dtype == np.dtype("int64")



# ── to_code ───────────────────────────────────────────────────────────────────

def test_to_code_contains_class_name():
    code = OrderSchema.to_code()
    assert "class OrderSchema(PandasSchema):" in code


def test_to_code_contains_field_annotations():
    code = OrderSchema.to_code()
    assert "order_id" in code
    assert "amount" in code


# ── from_dtype_dict ───────────────────────────────────────────────────────────

def test_from_dtype_dict_creates_schema():
    dtypes = {"x": np.dtype("int64"), "y": np.dtype("float64")}
    cls = PandasSchema.from_dtype_dict(dtypes, name="TestSchema")
    df = pd.DataFrame({"x": [1], "y": [1.0]})
    assert isinstance(df, cls)


# ── diff ──────────────────────────────────────────────────────────────────────

def test_diff_identical_schemas():
    result = OrderSchema.diff(OrderSchema)
    assert "identical" in result


def test_diff_different_schemas():
    result = OrderSchema.diff(EnrichedSchema)
    assert "revenue" in result


# ── to_dtype_dict and empty: comprehensive dtype coverage ────────────────────
# One schema that exercises every supported annotation category.

import datetime  # noqa: E402


class AllDtypesSchema(PandasSchema):
    col_int64:     np.dtype("int64")         # numpy dtype instance
    col_float32:   np.dtype("float32")
    col_datetime:  np.dtype("datetime64[ns]")
    col_np_int32:  np.int32                  # numpy scalar type
    col_string:    pd.StringDtype()          # pandas extension dtype
    col_int_ext:   pd.Int64Dtype()
    col_bool_ext:  pd.BooleanDtype()
    col_int_py:    int                       # Python builtins
    col_str_py:    str
    col_list:      list[str]                 # collection types → object
    col_dt:        datetime.datetime         # datetime builtins
    col_opt:       Optional[pd.StringDtype()]  # Optional


def test_dtype_coverage():
    """to_dtype_dict and empty() cover every supported annotation category."""
    d = AllDtypesSchema.to_dtype_dict()
    assert d["col_int64"]   == np.dtype("int64")
    assert d["col_float32"] == np.dtype("float32")
    assert d["col_datetime"] == np.dtype("datetime64[ns]")
    assert d["col_np_int32"] == np.dtype("int32")
    assert d["col_string"]  == pd.StringDtype()
    assert d["col_int_ext"] == pd.Int64Dtype()
    assert d["col_bool_ext"] == pd.BooleanDtype()
    assert d["col_int_py"]  == np.dtype("int64")
    assert d["col_str_py"]  == np.dtype("object")
    assert d["col_list"]    == np.dtype("object")
    assert d["col_dt"]      == np.dtype("datetime64[ns]")
    assert d["col_opt"]     == pd.StringDtype()

    df = AllDtypesSchema.empty()
    assert len(df) == 0
    assert df["col_int64"].dtype   == np.dtype("int64")
    assert df["col_string"].dtype  == pd.StringDtype()
    assert df["col_int_ext"].dtype == pd.Int64Dtype()
    assert df["col_list"].dtype    == np.dtype("object")
    assert isinstance(df, AllDtypesSchema)


# ── PyArrow nested types: to_dtype_dict, empty, isinstance ───────────────────

pytest.importorskip("pyarrow")

import pyarrow as pa  # noqa: E402


class ArrowNestedSchema(PandasSchema):
    order_id   = np.dtype("int64")
    line_items = pd.ArrowDtype(pa.list_(pa.struct([   # list of structs
        pa.field("sku",   pa.string()),
        pa.field("price", pa.float64()),
    ])))
    address    = pd.ArrowDtype(pa.struct([             # flat struct
        pa.field("street", pa.string()),
        pa.field("city",   pa.string()),
    ]))


def test_arrow_nested_dtype_coverage():
    """to_dtype_dict, empty, and isinstance all work for PyArrow nested columns."""
    d = ArrowNestedSchema.to_dtype_dict()
    assert d["line_items"] == pd.ArrowDtype(pa.list_(pa.struct([
        pa.field("sku",   pa.string()),
        pa.field("price", pa.float64()),
    ])))
    assert d["address"] == pd.ArrowDtype(pa.struct([
        pa.field("street", pa.string()),
        pa.field("city",   pa.string()),
    ]))

    df = ArrowNestedSchema.empty()
    assert len(df) == 0
    assert isinstance(df["line_items"].dtype, pd.ArrowDtype)
    assert isinstance(df["address"].dtype, pd.ArrowDtype)
    assert isinstance(df, ArrowNestedSchema)


# ── bad annotation ────────────────────────────────────────────────────────────

def test_bad_annotation_raises_type_annotation_error():
    class BadSchema(PandasSchema):
        col: object()  # type: ignore[assignment]

    with pytest.raises(TypeAnnotationError):
        BadSchema.to_dtype_dict()
