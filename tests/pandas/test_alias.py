"""Tests for alias(), assignment form, auto-docstring, and print_schema (pandas)."""

import numpy as np
import pandas as pd
import pytest

from dfguard._base._col_names import sanitize, sanitize_all
from dfguard.pandas import PandasSchema, alias, print_schema
from dfguard.pandas.exceptions import SchemaValidationError

# ── alias helpers ─────────────────────────────────────────────────────────────

def test_sanitize_basics():
    assert sanitize("First Name") == "first_name"
    assert sanitize("order-id") == "order_id"
    assert sanitize("2023_revenue") == "_2023_revenue"
    assert sanitize("class") == "class_"
    assert sanitize("HTMLParser") == "html_parser"
    assert sanitize("") == "col"


def test_sanitize_all_deduplication():
    result = sanitize_all(["First Name", "first_name", "FIRST NAME"])
    # first_name appears twice → second gets _2, third gets _3
    assert len(result) == 3
    assert result["first_name"] == "First Name"
    assert result["first_name_2"] == "first_name"
    assert result["first_name_3"] == "FIRST NAME"


# ── Schema with alias() fields ─────────────────────────────────────────────────

class AliasedSchema(PandasSchema):
    first_name = alias("First Name",   np.dtype("object"))
    order_id   = alias("order-id",     np.dtype("int64"))
    revenue    = np.dtype("float64")   # no alias needed


def test_alias_schema_fields_present():
    assert "first_name" in AliasedSchema._schema_fields
    assert "order_id"   in AliasedSchema._schema_fields
    assert "revenue"    in AliasedSchema._schema_fields


def test_alias_to_dtype_dict_uses_actual_col_names():
    dtypes = AliasedSchema.to_dtype_dict()
    assert "First Name" in dtypes
    assert "order-id"   in dtypes
    assert "revenue"    in dtypes
    assert "first_name" not in dtypes
    assert "order_id"   not in dtypes


def test_alias_isinstance_passes_with_actual_col_names():
    # Use pd.Series with explicit object dtype — pd.array(..., dtype="object")
    # creates StringDtype in pandas 3.0.
    df = pd.DataFrame({
        "First Name": pd.Series(["Alice"], dtype="object"),
        "order-id":   pd.Series([1], dtype="int64"),
        "revenue":    pd.Series([9.99], dtype="float64"),
    })
    assert isinstance(df, AliasedSchema)


def test_alias_validate_missing_actual_col():
    df = pd.DataFrame({
        "first_name": pd.Series(["Alice"], dtype="object"),  # wrong: Python name not col name
        "order-id":   pd.Series([1], dtype="int64"),
        "revenue":    pd.Series([9.99], dtype="float64"),
    })
    errors = AliasedSchema.validate(df)
    assert any("First Name" in str(e) for e in errors)


def test_alias_assert_valid_raises():
    df = pd.DataFrame({"revenue": pd.array([1.0], dtype="float64")})
    with pytest.raises(SchemaValidationError):
        AliasedSchema.assert_valid(df)


def test_alias_to_code_emits_alias():
    code = AliasedSchema.to_code()
    assert "alias" in code
    assert "First Name" in code
    assert "order-id" in code
    assert "revenue:" in code   # non-alias fields use annotation form


# ── Assignment form (without alias) ───────────────────────────────────────────

class AssignmentSchema(PandasSchema):
    order_id = np.dtype("int64")
    label    = pd.StringDtype()
    score    = np.dtype("float64")


def test_assignment_form_fields_collected():
    assert "order_id" in AssignmentSchema._schema_fields
    assert "label"    in AssignmentSchema._schema_fields
    assert "score"    in AssignmentSchema._schema_fields


def test_assignment_form_validates():
    df = pd.DataFrame({
        "order_id": pd.array([1], dtype="int64"),
        "label":    pd.array(["x"], dtype=pd.StringDtype()),
        "score":    pd.array([1.0], dtype="float64"),
    })
    assert isinstance(df, AssignmentSchema)
    assert AssignmentSchema.validate(df) == []


# ── Auto-generated docstring ───────────────────────────────────────────────────

class NoDocSchema(PandasSchema):
    order_id: np.dtype("int64")
    amount:   np.dtype("float64")


def test_auto_doc_generated():
    assert NoDocSchema.__doc__ is not None
    assert "order_id" in NoDocSchema.__doc__
    assert "amount"   in NoDocSchema.__doc__


def test_explicit_doc_not_overridden():
    class WithDoc(PandasSchema):
        """My custom docstring."""
        order_id: np.dtype("int64")

    assert WithDoc.__doc__ == "My custom docstring."


# ── from_dtype_dict with non-identifier column names ──────────────────────────

def test_from_dtype_dict_sanitizes_column_names():
    dtypes = {
        "First Name":  np.dtype("object"),
        "order-id":    np.dtype("int64"),
        "2023_revenue": np.dtype("float64"),
    }
    cls = PandasSchema.from_dtype_dict(dtypes, name="TestSchema")
    # Python names in _schema_fields
    assert "first_name"    in cls._schema_fields
    assert "order_id"      in cls._schema_fields
    assert "_2023_revenue" in cls._schema_fields
    # Actual col names in dtype dict
    d = cls.to_dtype_dict()
    assert "First Name"    in d
    assert "order-id"      in d
    assert "2023_revenue"  in d


# ── print_schema ──────────────────────────────────────────────────────────────

def test_print_schema_runs(capsys):
    df = pd.DataFrame({
        "First Name": pd.Series(["Alice"], dtype="object"),
        "order-id":   pd.Series([1], dtype="int64"),
    })
    print_schema(df, name="MySchema")
    captured = capsys.readouterr()
    assert "MySchema" in captured.out
    assert "alias" in captured.out
    assert "First Name" in captured.out
