"""annotation_to_pandas_dtype and dtypes_compatible."""

import datetime

import numpy as np
import pandas as pd
import pytest

from dfguard._nullable import _NullableAnnotation
from dfguard.pandas.types import annotation_to_pandas_dtype, dtypes_compatible, pandas_dtype_to_str

# ── annotation_to_pandas_dtype ────────────────────────────────────────────────

class TestAnnotationToPandasDtype:

    def test_numpy_dtype_instance(self):
        dtype, nullable = annotation_to_pandas_dtype(np.dtype("int64"))
        assert dtype == np.dtype("int64")
        assert nullable is False

    def test_numpy_scalar_type(self):
        dtype, nullable = annotation_to_pandas_dtype(np.int64)
        assert dtype == np.dtype("int64")
        assert nullable is False

    def test_numpy_float(self):
        dtype, nullable = annotation_to_pandas_dtype(np.float64)
        assert dtype == np.dtype("float64")
        assert nullable is False

    def test_numpy_bool(self):
        dtype, nullable = annotation_to_pandas_dtype(np.bool_)
        assert dtype == np.dtype("bool")
        assert nullable is False

    def test_pandas_string_dtype(self):
        dtype, nullable = annotation_to_pandas_dtype(pd.StringDtype())
        assert dtype == pd.StringDtype()
        assert nullable is True  # extension dtypes are inherently nullable

    def test_pandas_boolean_dtype(self):
        dtype, nullable = annotation_to_pandas_dtype(pd.BooleanDtype())
        assert dtype == pd.BooleanDtype()
        assert nullable is True

    def test_pandas_nullable_int(self):
        dtype, nullable = annotation_to_pandas_dtype(pd.Int64Dtype())
        assert dtype == pd.Int64Dtype()
        assert nullable is True

    def test_python_int(self):
        dtype, nullable = annotation_to_pandas_dtype(int)
        assert dtype == np.dtype("int64")
        assert nullable is False

    def test_python_float(self):
        dtype, nullable = annotation_to_pandas_dtype(float)
        assert dtype == np.dtype("float64")
        assert nullable is False

    def test_python_bool(self):
        dtype, nullable = annotation_to_pandas_dtype(bool)
        assert dtype == np.dtype("bool")
        assert nullable is False

    def test_python_str_returns_object_dtype(self):
        dtype, nullable = annotation_to_pandas_dtype(str)
        assert dtype == np.dtype("object")
        assert nullable is False

    def test_python_bytes(self):
        dtype, nullable = annotation_to_pandas_dtype(bytes)
        assert dtype == np.dtype("object")

    def test_list_annotation(self):
        dtype, nullable = annotation_to_pandas_dtype(list)
        assert dtype == np.dtype("object")

    def test_parameterised_list(self):
        dtype, _ = annotation_to_pandas_dtype(list[str])  # type: ignore[valid-type]
        assert dtype == np.dtype("object")

    def test_parameterised_dict(self):
        from typing import Any
        dtype, _ = annotation_to_pandas_dtype(dict[str, Any])  # type: ignore[valid-type]
        assert dtype == np.dtype("object")

    def test_datetime_datetime(self):
        dtype, _ = annotation_to_pandas_dtype(datetime.datetime)
        assert dtype == np.dtype("datetime64[ns]")

    def test_pd_timestamp(self):
        dtype, _ = annotation_to_pandas_dtype(pd.Timestamp)
        assert dtype == np.dtype("datetime64[ns]")

    def test_timedelta(self):
        dtype, _ = annotation_to_pandas_dtype(datetime.timedelta)
        assert dtype == np.dtype("timedelta64[ns]")

    def test_optional_nullable_annotation(self):
        """dfguard's Optional wrapper for dtype instances."""
        wrapped = _NullableAnnotation(pd.StringDtype())
        dtype, nullable = annotation_to_pandas_dtype(wrapped)
        assert dtype == pd.StringDtype()
        assert nullable is True

    def test_optional_union_syntax(self):
        """typing.Optional[np.int64] → Union[np.int64, None]."""
        from typing import Optional
        dtype, nullable = annotation_to_pandas_dtype(Optional[np.int64])
        assert dtype == np.dtype("int64")
        assert nullable is True

    def test_native_union_syntax(self):
        """np.int64 | None → Python 3.11 native union."""
        dtype, nullable = annotation_to_pandas_dtype(np.int64 | None)
        assert dtype == np.dtype("int64")
        assert nullable is True

    def test_unsupported_raises(self):
        class Weird:
            pass
        with pytest.raises(TypeError):
            annotation_to_pandas_dtype(Weird)


# ── dtypes_compatible ─────────────────────────────────────────────────────────

class TestDtypesCompatible:

    def test_identical_numpy_dtypes(self):
        assert dtypes_compatible(np.dtype("int64"), np.dtype("int64"))

    def test_different_numpy_dtypes(self):
        assert not dtypes_compatible(np.dtype("int64"), np.dtype("float64"))

    def test_object_dtype(self):
        assert dtypes_compatible(np.dtype("object"), np.dtype("object"))

    def test_identical_extension_dtypes(self):
        assert dtypes_compatible(pd.StringDtype(), pd.StringDtype())

    def test_different_extension_dtypes(self):
        assert not dtypes_compatible(pd.StringDtype(), pd.BooleanDtype())

    def test_numpy_vs_extension_not_compatible(self):
        # np.int64 and pd.Int64Dtype() are different
        assert not dtypes_compatible(np.dtype("int64"), pd.Int64Dtype())

    def test_object_vs_string_dtype_not_compatible(self):
        assert not dtypes_compatible(np.dtype("object"), pd.StringDtype())


# ── pandas_dtype_to_str ───────────────────────────────────────────────────────

class TestPandasDtypeToStr:

    def test_int64(self):
        assert pandas_dtype_to_str(np.dtype("int64")) == "np.dtype('int64')"

    def test_float64(self):
        assert pandas_dtype_to_str(np.dtype("float64")) == "np.dtype('float64')"

    def test_object(self):
        assert pandas_dtype_to_str(np.dtype("object")) == "object"

    def test_datetime(self):
        assert "datetime64" in pandas_dtype_to_str(np.dtype("datetime64[ns]"))

    def test_string_dtype(self):
        assert pandas_dtype_to_str(pd.StringDtype()) == "pd.StringDtype()"

    def test_boolean_dtype(self):
        assert pandas_dtype_to_str(pd.BooleanDtype()) == "pd.BooleanDtype()"

    def test_int64_nullable_dtype(self):
        assert pandas_dtype_to_str(pd.Int64Dtype()) == "pd.Int64Dtype()"
