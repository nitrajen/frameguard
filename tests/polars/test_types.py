"""annotation_to_polars_dtype, dtypes_compatible, polars_dtype_to_str."""

import datetime

import numpy as np
import polars as pl
import pytest

from dfguard._nullable import _NullableAnnotation
from dfguard.polars.types import (
    annotation_to_polars_dtype,
    dtypes_compatible,
    polars_dtype_to_str,
)


class TestAnnotationToPolars:

    # ── Polars native types ────────────────────────────────────────────────

    def test_polars_int_class(self):
        dtype, nullable = annotation_to_polars_dtype(pl.Int64)
        assert dtypes_compatible(pl.Int64, dtype)
        assert nullable is False

    def test_polars_float_class(self):
        dtype, _ = annotation_to_polars_dtype(pl.Float64)
        assert dtypes_compatible(pl.Float64, dtype)

    def test_polars_string_class(self):
        dtype, _ = annotation_to_polars_dtype(pl.String)
        assert dtypes_compatible(pl.String, dtype)

    def test_polars_boolean_class(self):
        dtype, _ = annotation_to_polars_dtype(pl.Boolean)
        assert dtypes_compatible(pl.Boolean, dtype)

    def test_polars_binary_class(self):
        dtype, _ = annotation_to_polars_dtype(pl.Binary)
        assert dtypes_compatible(pl.Binary, dtype)

    def test_polars_all_int_sizes(self):
        for t in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64):
            dtype, _ = annotation_to_polars_dtype(t)
            assert dtypes_compatible(t, dtype)

    def test_polars_float_sizes(self):
        for t in (pl.Float32, pl.Float64):
            dtype, _ = annotation_to_polars_dtype(t)
            assert dtypes_compatible(t, dtype)

    def test_polars_temporal(self):
        for t in (pl.Date, pl.Datetime, pl.Duration, pl.Time):
            dtype, _ = annotation_to_polars_dtype(t)
            assert dtypes_compatible(t, dtype)

    def test_polars_list_instance(self):
        ann = pl.List(pl.String)
        dtype, _ = annotation_to_polars_dtype(ann)
        assert dtype == pl.List(pl.String)

    def test_polars_struct_instance(self):
        ann = pl.Struct({"a": pl.Int64, "b": pl.String})
        dtype, _ = annotation_to_polars_dtype(ann)
        assert dtype == pl.Struct({"a": pl.Int64, "b": pl.String})

    def test_polars_nested_list(self):
        ann = pl.List(pl.List(pl.Int64))
        dtype, _ = annotation_to_polars_dtype(ann)
        assert dtype == pl.List(pl.List(pl.Int64))

    # ── Python builtins ────────────────────────────────────────────────────

    def test_builtin_int(self):
        dtype, _ = annotation_to_polars_dtype(int)
        assert dtypes_compatible(pl.Int64, dtype)

    def test_builtin_float(self):
        dtype, _ = annotation_to_polars_dtype(float)
        assert dtypes_compatible(pl.Float64, dtype)

    def test_builtin_str(self):
        dtype, _ = annotation_to_polars_dtype(str)
        assert dtypes_compatible(pl.String, dtype)

    def test_builtin_bool(self):
        dtype, _ = annotation_to_polars_dtype(bool)
        assert dtypes_compatible(pl.Boolean, dtype)

    def test_builtin_bytes(self):
        dtype, _ = annotation_to_polars_dtype(bytes)
        assert dtypes_compatible(pl.Binary, dtype)

    # ── Python generics ────────────────────────────────────────────────────

    def test_list_str(self):
        dtype, _ = annotation_to_polars_dtype(list[str])
        assert dtype == pl.List(pl.String)

    def test_list_int(self):
        dtype, _ = annotation_to_polars_dtype(list[int])
        assert dtype == pl.List(pl.Int64)

    def test_list_nested(self):
        dtype, _ = annotation_to_polars_dtype(list[list[int]])
        assert dtype == pl.List(pl.List(pl.Int64))

    def test_dict_becomes_object(self):
        dtype, _ = annotation_to_polars_dtype(dict)
        assert dtypes_compatible(pl.Object, dtype)

    # ── numpy dtype mapping ───────────────────────────────────────────────

    def test_numpy_int64(self):
        dtype, _ = annotation_to_polars_dtype(np.dtype("int64"))
        assert dtypes_compatible(pl.Int64, dtype)

    def test_numpy_float32(self):
        dtype, _ = annotation_to_polars_dtype(np.dtype("float32"))
        assert dtypes_compatible(pl.Float32, dtype)

    def test_numpy_bool(self):
        dtype, _ = annotation_to_polars_dtype(np.dtype("bool"))
        assert dtypes_compatible(pl.Boolean, dtype)

    def test_numpy_uint32(self):
        dtype, _ = annotation_to_polars_dtype(np.dtype("uint32"))
        assert dtypes_compatible(pl.UInt32, dtype)

    def test_numpy_scalar_type(self):
        dtype, _ = annotation_to_polars_dtype(np.int32)
        assert dtypes_compatible(pl.Int32, dtype)

    # ── datetime types ────────────────────────────────────────────────────

    def test_datetime_datetime(self):
        dtype, _ = annotation_to_polars_dtype(datetime.datetime)
        assert dtypes_compatible(pl.Datetime, dtype)

    def test_datetime_date(self):
        dtype, _ = annotation_to_polars_dtype(datetime.date)
        assert dtypes_compatible(pl.Date, dtype)

    def test_datetime_timedelta(self):
        dtype, _ = annotation_to_polars_dtype(datetime.timedelta)
        assert dtypes_compatible(pl.Duration, dtype)

    # ── Optional / nullable ───────────────────────────────────────────────

    def test_dfguard_optional(self):
        ann = _NullableAnnotation(pl.String)
        dtype, nullable = annotation_to_polars_dtype(ann)
        assert dtypes_compatible(pl.String, dtype)
        assert nullable is True

    def test_typing_optional(self):
        from typing import Optional
        dtype, nullable = annotation_to_polars_dtype(Optional[pl.Int64])
        assert dtypes_compatible(pl.Int64, dtype)
        assert nullable is True

    def test_native_union_syntax(self):
        dtype, nullable = annotation_to_polars_dtype(pl.Float64 | None)
        assert dtypes_compatible(pl.Float64, dtype)
        assert nullable is True

    def test_unsupported_raises(self):
        class Weird:
            pass
        with pytest.raises(TypeError):
            annotation_to_polars_dtype(Weird)


class TestDtypesCompatible:

    def test_class_vs_class(self):
        assert dtypes_compatible(pl.Int64, pl.Int64)
        assert not dtypes_compatible(pl.Int64, pl.Float64)

    def test_class_vs_instance(self):
        df = pl.DataFrame({"a": pl.Series([1], dtype=pl.Int64)})
        assert dtypes_compatible(pl.Int64, df.schema["a"])

    def test_instance_vs_instance(self):
        assert dtypes_compatible(pl.List(pl.String), pl.List(pl.String))
        assert not dtypes_compatible(pl.List(pl.String), pl.List(pl.Int64))

    def test_struct_equality(self):
        s1 = pl.Struct({"a": pl.Int64})
        s2 = pl.Struct({"a": pl.Int64})
        assert dtypes_compatible(s1, s2)

    def test_different_types(self):
        assert not dtypes_compatible(pl.Int64, pl.String)


class TestAllRegisteredDtypes:
    """
    Runtime-discovered test: every Polars DataType registered in the installed
    version must be accepted by annotation_to_polars_dtype and renderable by
    polars_dtype_to_str.  New types in future Polars releases are covered
    automatically without any code change.
    """

    @staticmethod
    def _concrete_dtypes() -> list:
        """Return all no-arg-instantiable, non-abstract Polars DataType subclasses."""
        import polars.datatypes as _pld
        # Collect abstract base classes that exist in the installed version
        _abstract = {
            getattr(_pld, n)
            for n in ("NumericType", "TemporalType", "NestedType", "ObjectType",
                      "FloatType", "IntegerType", "SignedIntegerType", "UnsignedIntegerType")
            if hasattr(_pld, n)
        }
        seen: set = set()
        def walk(cls):
            for sub in cls.__subclasses__():
                if sub not in seen:
                    seen.add(sub)
                    walk(sub)
        walk(pl.DataType)

        result = []
        for cls in seen:
            if cls in _abstract:
                continue
            try:
                inst = cls()
                if isinstance(inst, pl.DataType):
                    result.append(cls)
            except Exception:
                pass
        return result

    @pytest.mark.parametrize("dtype_cls", _concrete_dtypes.__func__())
    def test_dtype_accepted_as_annotation(self, dtype_cls):
        """Every concrete DataType class is accepted as an annotation."""
        dtype, _ = annotation_to_polars_dtype(dtype_cls)
        assert dtypes_compatible(dtype_cls, dtype)

    @pytest.mark.parametrize("dtype_cls", _concrete_dtypes.__func__())
    def test_dtype_instance_accepted_as_annotation(self, dtype_cls):
        """Every concrete DataType instance is accepted as an annotation."""
        inst = dtype_cls()
        dtype, _ = annotation_to_polars_dtype(inst)
        assert dtypes_compatible(inst, dtype)

    @pytest.mark.parametrize("dtype_cls", _concrete_dtypes.__func__())
    def test_dtype_renders_to_str(self, dtype_cls):
        """Every concrete DataType can be rendered to a source string."""
        s = polars_dtype_to_str(dtype_cls())
        assert s.startswith("pl.")


class TestPolarsDtypeToStr:

    def test_int64(self):
        assert polars_dtype_to_str(pl.Int64) == "pl.Int64"

    def test_float32(self):
        assert polars_dtype_to_str(pl.Float32) == "pl.Float32"

    def test_string(self):
        assert polars_dtype_to_str(pl.String) == "pl.String"

    def test_list(self):
        assert polars_dtype_to_str(pl.List(pl.String)) == "pl.List(pl.String)"

    def test_nested_list(self):
        assert polars_dtype_to_str(pl.List(pl.List(pl.Int64))) == "pl.List(pl.List(pl.Int64))"

    def test_struct(self):
        s = polars_dtype_to_str(pl.Struct({"a": pl.Int64, "b": pl.String}))
        assert "pl.Struct" in s
        assert '"a"' in s

    def test_datetime_default(self):
        assert "Datetime" in polars_dtype_to_str(pl.Datetime)

    def test_datetime_with_unit(self):
        s = polars_dtype_to_str(pl.Datetime("ms"))
        assert "ms" in s


# ── Deep nested complex types ─────────────────────────────────────────────────

def test_nested_list_of_struct():
    ann = pl.List(pl.Struct({"x": pl.Int64, "y": pl.String}))
    dtype, _ = annotation_to_polars_dtype(ann)
    assert isinstance(dtype, pl.List)
    assert isinstance(dtype.inner, pl.Struct)


def test_struct_with_nested_list():
    ann = pl.Struct({"a": pl.Int64, "b": pl.List(pl.String)})
    dtype, _ = annotation_to_polars_dtype(ann)
    assert isinstance(dtype, pl.Struct)


def test_three_level_nested_list():
    ann = pl.List(pl.List(pl.List(pl.Int64)))
    dtype, _ = annotation_to_polars_dtype(ann)
    assert dtype == pl.List(pl.List(pl.List(pl.Int64)))


def test_struct_with_nested_struct():
    ann = pl.Struct({"a": pl.Struct({"b": pl.List(pl.Int64)})})
    dtype, _ = annotation_to_polars_dtype(ann)
    assert isinstance(dtype, pl.Struct)
