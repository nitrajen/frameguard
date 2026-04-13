"""Annotation-to-pandas-dtype conversion utilities.

Design
------
We avoid hard-coding a closed list of dtypes. Instead, ``annotation_to_pandas_dtype``
dispatches on *kind* of annotation:

1. numpy dtype instances and scalar types  → passed through or converted via ``np.dtype()``
2. pandas extension dtype instances        → passed through (StringDtype, BooleanDtype, …)
3. Python builtins (int, float, bool, str, bytes) → mapped to canonical numpy dtypes
4. Python generic types (list, dict, tuple, set)  → ``object`` dtype with a type-hint note
5. datetime.datetime / pd.Timestamp        → ``datetime64[ns]``
6. Optional[X] / X | None                 → convert X, set nullable=True
7. ``dfguard._nullable._NullableAnnotation`` → same as Optional

No explicit mapping table of dtype names; conversion is structural and functional.
This means any future numpy or pandas dtype that follows the same protocol works
automatically without code changes.
"""

from __future__ import annotations

import datetime
import types as _builtin_types
from typing import Any, Union, get_args, get_origin

from dfguard._nullable import _NullableAnnotation

# cache: annotation → (dtype, nullable)
_ANNOTATION_CACHE: dict[Any, tuple[Any, bool]] = {}


def annotation_to_pandas_dtype(annotation: Any) -> tuple[Any, bool]:
    """
    Convert a type annotation to ``(dtype, nullable)``.

    Accepts
    -------
    - numpy dtype instance:     ``np.dtype("int64")``
    - numpy scalar type:        ``np.int64``, ``np.float32``
    - pandas extension dtype:   ``pd.StringDtype()``, ``pd.Int64Dtype()``, ``pd.BooleanDtype()``
    - Python builtins:          ``int``, ``float``, ``bool``, ``str``, ``bytes``
    - Python generic types:     ``list[str]``, ``dict[str, Any]``, ``list``, ``dict``
    - datetime types:           ``datetime.datetime``, ``pd.Timestamp``
    - ``Optional[X]`` / ``X | None`` → convert X, nullable=True
    - ``dfguard.pandas.Optional[X]`` (same as above, for dtype instances)

    Returns
    -------
    (dtype, nullable)
        ``dtype``   is a numpy ``dtype`` or pandas ``ExtensionDtype``.
        ``nullable`` is True when the annotation was wrapped in Optional or
        is an inherently-nullable pandas extension dtype.
    """
    try:
        if annotation in _ANNOTATION_CACHE:
            return _ANNOTATION_CACHE[annotation]
    except TypeError:
        pass  # unhashable annotations (parameterised generics) fall through

    import numpy as np
    import pandas as pd

    # ── dfguard Optional[X] wrapper (for dtype instances that aren't types) ──
    if isinstance(annotation, _NullableAnnotation):
        dtype, _ = annotation_to_pandas_dtype(annotation.inner)
        return dtype, True

    # ── Optional[X]  ≡  Union[X, None] ──────────────────────────────────────
    origin = get_origin(annotation)
    args   = get_args(annotation)

    if origin is Union or (
        hasattr(_builtin_types, "UnionType")
        and isinstance(annotation, _builtin_types.UnionType)
    ):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            dtype, _ = annotation_to_pandas_dtype(non_none[0])
            return dtype, True
        raise TypeError(
            f"Only Optional[T] (Union with None) is supported for nullable columns, "
            f"got: {annotation}"
        )

    # ── Python generic types: list[T], dict[K, V], tuple, set, … ────────────
    # These are stored as object dtype in pandas (can't reflect inner types).
    if origin in (list, dict, tuple, set, frozenset):
        return np.dtype("object"), False

    # ── numpy dtype instance ─────────────────────────────────────────────────
    if isinstance(annotation, np.dtype):
        result: tuple[Any, bool] = (annotation, False)
        try:
            _ANNOTATION_CACHE[annotation] = result
        except TypeError:
            pass
        return result

    # ── pandas extension dtype instance ─────────────────────────────────────
    # All pandas ExtensionDtype subclasses (StringDtype, BooleanDtype, Int8Dtype,
    # Float32Dtype, ArrowDtype, etc.) are inherently nullable.
    if isinstance(annotation, pd.api.extensions.ExtensionDtype):
        result = (annotation, True)
        try:
            _ANNOTATION_CACHE[annotation] = result
        except TypeError:
            pass
        return result

    # ── numpy scalar type (np.int64, np.float32, np.bool_, …) ───────────────
    if isinstance(annotation, type) and issubclass(annotation, np.generic):
        dtype = np.dtype(annotation)
        result = (dtype, False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # ── Python builtins ──────────────────────────────────────────────────────
    _BUILTIN_MAP: dict[type, str] = {
        int:   "int64",
        float: "float64",
        bool:  "bool",
        bytes: "object",
    }
    if annotation in _BUILTIN_MAP:
        result = (np.dtype(_BUILTIN_MAP[annotation]), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # str → object dtype (Python strings are stored as numpy object arrays
    # unless the user explicitly uses pd.StringDtype())
    if annotation is str:
        result = (np.dtype("object"), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # ── Unparameterised collection types ────────────────────────────────────
    if annotation in (list, dict, tuple, set, frozenset):
        result = (np.dtype("object"), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # ── Datetime types ───────────────────────────────────────────────────────
    if annotation in (datetime.datetime, pd.Timestamp):
        result = (np.dtype("datetime64[ns]"), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    if annotation is datetime.timedelta:
        result = (np.dtype("timedelta64[ns]"), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # datetime.date is stored as object dtype by pandas
    if annotation is datetime.date:
        result = (np.dtype("object"), False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    raise TypeError(
        f"Cannot convert {annotation!r} to a pandas dtype.\n"
        "Use:\n"
        "  • numpy dtype instances:      np.dtype('int64'), np.dtype('float32')\n"
        "  • numpy scalar types:         np.int64, np.float32, np.bool_\n"
        "  • pandas extension dtypes:    pd.StringDtype(), pd.Int64Dtype(), pd.BooleanDtype()\n"
        "  • Python builtins:            int, float, bool, str\n"
        "  • Python collection types:    list[str], dict[str, Any]  (→ object dtype)\n"
        "  • Datetime:                   datetime.datetime, pd.Timestamp\n"
        "  • Nullable:                   Optional[pd.StringDtype()]  (dfguard.pandas.Optional)\n"
        "                                np.int64 | None  (Python 3.11+ native union syntax)"
    )


def pandas_dtype_to_str(dtype: Any) -> str:
    """
    Return a Python source string for a dtype instance (used by ``infer_schema`` / ``to_code``).

    Functional: dispatches on duck-typed attributes rather than hard-coded names.
    """
    import numpy as np
    import pandas as pd

    if isinstance(dtype, np.dtype):
        kind = dtype.kind
        name = dtype.name  # e.g. "int64", "float64", "datetime64[ns]"
        if kind in ("M", "m"):          # datetime64 / timedelta64
            return f"np.dtype('{name}')"
        if kind == "O":                  # object
            return "object"
        return f"np.dtype('{name}')"

    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
        cls_name = type(dtype).__name__
        if isinstance(dtype, pd.CategoricalDtype):
            if dtype.categories is None:
                return "pd.CategoricalDtype()"
            return f"pd.CategoricalDtype({list(dtype.categories)!r}, ordered={dtype.ordered!r})"
        return f"pd.{cls_name}()"

    return repr(dtype)


def dtypes_compatible(expected: Any, actual: Any) -> bool:
    """
    Return True when ``actual`` dtype satisfies ``expected``.

    Both numpy and pandas extension dtypes implement ``__eq__`` correctly,
    so we just delegate to that. Handles object dtype, all extension types,
    and parameterised types like CategoricalDtype.
    """
    try:
        return bool(expected == actual)
    except Exception:
        return type(expected) is type(actual)
