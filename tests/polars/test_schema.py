"""PolarsSchema: definition, validation, isinstance, inheritance, to_code."""

import polars as pl
import pytest

from dfguard.polars import Optional, PolarsSchema
from dfguard.polars.exceptions import SchemaValidationError, TypeAnnotationError


class OrderSchema(PolarsSchema):
    order_id: pl.Int64
    amount:   pl.Float64
    quantity: pl.Int32


class EnrichedSchema(OrderSchema):
    revenue: pl.Float64


class NullableSchema(PolarsSchema):
    name:   pl.String
    score:  Optional[pl.Float64]
    active: pl.Boolean | None


class NestedSchema(PolarsSchema):
    order_id: pl.Int64
    tags:     pl.List(pl.String)
    scores:   pl.List(pl.Int64)


# ── Inheritance ───────────────────────────────────────────────────────────────

def test_child_inherits_parent_fields():
    assert "order_id" in EnrichedSchema._schema_fields
    assert "revenue"  in EnrichedSchema._schema_fields


def test_parent_unchanged_by_child():
    assert "revenue" not in OrderSchema._schema_fields


# ── to_struct ─────────────────────────────────────────────────────────────────

def test_to_struct_returns_correct_types():
    s = OrderSchema.to_struct()
    from dfguard.polars.types import dtypes_compatible
    assert dtypes_compatible(pl.Int64,   s["order_id"])
    assert dtypes_compatible(pl.Float64, s["amount"])
    assert dtypes_compatible(pl.Int32,   s["quantity"])


def test_to_struct_is_cached():
    assert OrderSchema.to_struct() is OrderSchema.to_struct()


def test_nullable_fields_in_struct():
    s = NullableSchema.to_struct()
    from dfguard.polars.types import dtypes_compatible
    assert dtypes_compatible(pl.String, s["name"])
    assert dtypes_compatible(pl.Float64, s["score"])


def test_optional_assignment_form_collected():
    """Optional[dtype] in assignment form must appear in _schema_fields (not silently dropped)."""
    class AssignOpt(PolarsSchema):
        x = pl.Int64
        y = Optional[pl.Float64]   # assignment form, not annotation form

    assert "y" in AssignOpt._schema_fields

    df_ok = pl.DataFrame({
        "x": pl.Series([1], dtype=pl.Int64),
        "y": pl.Series([1.0], dtype=pl.Float64),
    })
    df_missing = pl.DataFrame({"x": pl.Series([1], dtype=pl.Int64)})
    assert isinstance(df_ok, AssignOpt)
    assert not isinstance(df_missing, AssignOpt)


def test_nested_list_in_struct():
    s = NestedSchema.to_struct()
    assert s["tags"] == pl.List(pl.String)


# ── isinstance (subset matching) ──────────────────────────────────────────────

def test_isinstance_exact_match(order_df):
    assert isinstance(order_df, OrderSchema)


def test_isinstance_subset_passes_extra_columns(enriched_df):
    assert isinstance(enriched_df, OrderSchema)


def test_isinstance_fails_missing_column(order_df):
    assert not isinstance(order_df, EnrichedSchema)


def test_isinstance_fails_wrong_dtype(order_df):
    bad = order_df.with_columns(pl.col("order_id").cast(pl.String))
    assert not isinstance(bad, OrderSchema)


def test_isinstance_nested(nested_df):
    assert isinstance(nested_df, NestedSchema)


# ── validate ──────────────────────────────────────────────────────────────────

def test_validate_passes(order_df):
    assert OrderSchema.validate(order_df) == []


def test_validate_reports_missing_column(order_df):
    errors = EnrichedSchema.validate(order_df)
    assert any("revenue" in str(e) for e in errors)


def test_validate_reports_type_mismatch(order_df):
    bad = order_df.with_columns(pl.col("order_id").cast(pl.String))
    errors = OrderSchema.validate(bad)
    assert any("order_id" in str(e) for e in errors)


def test_validate_strict_rejects_extra_columns(enriched_df):
    errors = OrderSchema.validate(enriched_df, subset=False)
    assert any("revenue" in str(e) for e in errors)


def test_validate_non_strict_accepts_extra_columns(enriched_df):
    assert OrderSchema.validate(enriched_df, subset=True) == []


# ── assert_valid ──────────────────────────────────────────────────────────────

def test_assert_valid_passes(order_df):
    OrderSchema.assert_valid(order_df)


def test_assert_valid_raises_on_mismatch(order_df):
    with pytest.raises(SchemaValidationError) as exc_info:
        EnrichedSchema.assert_valid(order_df)
    assert "revenue" in str(exc_info.value)


# ── LazyFrame support ─────────────────────────────────────────────────────────

def test_isinstance_lazyframe(order_df):
    assert isinstance(order_df.lazy(), OrderSchema)


def test_validate_lazyframe(order_df):
    assert OrderSchema.validate(order_df.lazy()) == []


# ── empty ─────────────────────────────────────────────────────────────────────

def test_empty_creates_zero_row_dataframe():
    df = OrderSchema.empty()
    assert isinstance(df, pl.DataFrame)
    assert len(df) == 0
    assert set(df.columns) == {"order_id", "amount", "quantity"}


# ── to_code ───────────────────────────────────────────────────────────────────

def test_to_code_contains_class_name():
    code = OrderSchema.to_code()
    assert "class OrderSchema(PolarsSchema):" in code


def test_to_code_contains_fields():
    code = OrderSchema.to_code()
    assert "order_id" in code
    assert "pl.Int64" in code


def test_to_code_contains_nested_type():
    code = NestedSchema.to_code()
    assert "pl.List" in code


# ── from_struct ───────────────────────────────────────────────────────────────

def test_from_struct_creates_schema(order_df):
    cls = PolarsSchema.from_struct(dict(order_df.schema), name="TestSchema")
    assert isinstance(order_df, cls)


# ── diff ──────────────────────────────────────────────────────────────────────

def test_diff_identical():
    assert "identical" in OrderSchema.diff(OrderSchema)


def test_diff_different():
    result = OrderSchema.diff(EnrichedSchema)
    assert "revenue" in result


# ── bad annotation ────────────────────────────────────────────────────────────

def test_bad_annotation_raises():
    class BadSchema(PolarsSchema):
        col: object()  # type: ignore[assignment]

    with pytest.raises(TypeAnnotationError):
        BadSchema.to_struct()
