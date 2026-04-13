"""Annotation-to-Polars-dtype conversion utilities.

Design
------
Dispatches on the *kind* of annotation without a closed mapping table:

1. Polars DataType class (``pl.Int64``) or instance (``pl.List(pl.String)``) → pass through
2. Python builtins (``int``, ``float``, ``str``, ``bool``, ``bytes``)       → canonical Polars types
3. Python generic ``list[T]``                                                → ``pl.List(inner)``
4. Python generic ``dict`` / ``dict[K, V]``                                 → ``pl.Object``
5. numpy dtype instance          → mapped via dtype.kind/itemsize
6. ``Optional[X]`` / ``X | None`` → convert X, nullable=True
7. ``dfguard._nullable._NullableAnnotation`` → same as Optional
8. Datetime types                → pl.Date / pl.Datetime / pl.Duration
"""

from __future__ import annotations

import datetime
import types as _builtin_types
from typing import Any, Union, get_args, get_origin

from dfguard._nullable import _NullableAnnotation

_ANNOTATION_CACHE: dict[Any, tuple[Any, bool]] = {}

# numpy integer/uint/float kind → (bits → Polars type)
# Built lazily on first use to avoid importing polars at module load.
_NUMPY_KIND_MAP: dict[str, Any] | None = None


def _numpy_kind_map() -> dict[str, Any]:
    global _NUMPY_KIND_MAP
    if _NUMPY_KIND_MAP is None:
        import polars as pl
        _NUMPY_KIND_MAP = {
            "b": {1: pl.Boolean},
            "i": {1: pl.Int8, 2: pl.Int16, 4: pl.Int32, 8: pl.Int64},
            "u": {1: pl.UInt8, 2: pl.UInt16, 4: pl.UInt32, 8: pl.UInt64},
            "f": {4: pl.Float32, 8: pl.Float64},
            "M": pl.Datetime,
            "m": pl.Duration,
            "O": pl.Object,
            "U": pl.String,
            "S": pl.Binary,
        }
    return _NUMPY_KIND_MAP


def annotation_to_polars_dtype(annotation: Any) -> tuple[Any, bool]:
    """
    Convert a type annotation to ``(polars_dtype, nullable)``.

    Accepted annotations
    --------------------
    - Polars DataType class:   ``pl.Int64``, ``pl.Boolean``
    - Polars DataType instance: ``pl.List(pl.String)``, ``pl.Struct({...})``
    - Python builtins:         ``int``, ``float``, ``str``, ``bool``, ``bytes``
    - Python generic:          ``list[T]`` → ``pl.List(inner)``; ``dict`` → ``pl.Object``
    - numpy dtype:             ``np.dtype("int64")``, ``np.dtype("float32")``, …
    - numpy scalar type:       ``np.int64``, ``np.float32``, …
    - Datetime:                ``datetime.datetime`` / ``pd.Timestamp`` → ``pl.Datetime``
                               ``datetime.date``                        → ``pl.Date``
                               ``datetime.timedelta``                   → ``pl.Duration``
                               ``datetime.time``                        → ``pl.Time``
    - ``Optional[X]`` / ``X | None`` → convert X, nullable=True
    - ``dfguard.polars.Optional[X]`` (same, for DataType instances)

    Returns
    -------
    ``(dtype, nullable)`` where ``dtype`` is a Polars DataType class or instance.
    All Polars columns are nullable at the physical level; ``nullable=True`` means
    the annotation *explicitly* declared nullability.
    """
    try:
        if annotation in _ANNOTATION_CACHE:
            return _ANNOTATION_CACHE[annotation]
    except TypeError:
        pass

    import polars as pl

    # ── dfguard Optional[X] wrapper ──────────────────────────────────────────
    if isinstance(annotation, _NullableAnnotation):
        dtype, _ = annotation_to_polars_dtype(annotation.inner)
        return dtype, True

    # ── Optional[X] ≡ Union[X, None] ─────────────────────────────────────────
    origin = get_origin(annotation)
    args   = get_args(annotation)

    if origin is Union or (
        hasattr(_builtin_types, "UnionType")
        and isinstance(annotation, _builtin_types.UnionType)
    ):
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            dtype, _ = annotation_to_polars_dtype(non_none[0])
            return dtype, True
        raise TypeError(
            f"Only Optional[T] (Union with None) is supported; got: {annotation}"
        )

    # ── list[T] → pl.List(inner) ─────────────────────────────────────────────
    if origin is list or annotation is list:
        if origin is list and args:
            inner, _ = annotation_to_polars_dtype(args[0])
            return pl.List(inner), False
        return pl.List(pl.Unknown), False

    # ── dict / dict[K, V] → pl.Object (Polars has no MapType) ────────────────
    if origin is dict or annotation in (dict, tuple, set, frozenset):
        return pl.Object, False

    # ── Polars DataType instance (pl.List(pl.String), pl.Struct({…}), …) ─────
    if isinstance(annotation, pl.DataType):
        return annotation, False

    # ── Polars DataType class (pl.Int64, pl.Boolean, …) ──────────────────────
    if isinstance(annotation, type) and issubclass(annotation, pl.DataType):
        result: tuple[Any, bool] = (annotation, False)
        try:
            _ANNOTATION_CACHE[annotation] = result
        except TypeError:
            pass
        return result

    # ── numpy dtype instance ─────────────────────────────────────────────────
    try:
        import numpy as np
        if isinstance(annotation, np.dtype):
            dtype = _numpy_dtype_to_polars(annotation)
            result = (dtype, False)
            try:
                _ANNOTATION_CACHE[annotation] = result
            except TypeError:
                pass
            return result

        # numpy scalar type (np.int64, np.float32, …)
        if isinstance(annotation, type) and issubclass(annotation, np.generic):
            dtype = _numpy_dtype_to_polars(np.dtype(annotation))
            result = (dtype, False)
            _ANNOTATION_CACHE[annotation] = result
            return result
    except ImportError:
        pass

    # ── Python builtins ──────────────────────────────────────────────────────
    _BUILTIN: dict[type, Any] = {
        int:   pl.Int64,
        float: pl.Float64,
        bool:  pl.Boolean,
        str:   pl.String,
        bytes: pl.Binary,
    }
    if annotation in _BUILTIN:
        result = (_BUILTIN[annotation], False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # ── Datetime types ───────────────────────────────────────────────────────
    if annotation in (datetime.datetime,):
        result = (pl.Datetime, False)
        _ANNOTATION_CACHE[annotation] = result
        return result
    if annotation is datetime.date:
        result = (pl.Date, False)
        _ANNOTATION_CACHE[annotation] = result
        return result
    if annotation is datetime.timedelta:
        result = (pl.Duration, False)
        _ANNOTATION_CACHE[annotation] = result
        return result
    if annotation is datetime.time:
        result = (pl.Time, False)
        _ANNOTATION_CACHE[annotation] = result
        return result

    # ── pd.Timestamp ─────────────────────────────────────────────────────────
    try:
        import pandas as pd
        if annotation is pd.Timestamp:
            result = (pl.Datetime, False)
            _ANNOTATION_CACHE[annotation] = result
            return result
    except ImportError:
        pass

    raise TypeError(
        f"Cannot convert {annotation!r} to a Polars dtype.\n"
        "Use:\n"
        "  • Polars DataType classes:   pl.Int64, pl.String, pl.Boolean\n"
        "  • Polars parametrised types: pl.List(pl.String), pl.Struct({'a': pl.Int64})\n"
        "  • Python builtins:           int, float, str, bool, bytes\n"
        "  • Python list generic:       list[str], list[int]  (→ pl.List)\n"
        "  • numpy dtype instances:     np.dtype('int64'), np.dtype('float32')\n"
        "  • Datetime:                  datetime.datetime, datetime.date\n"
        "  • Nullable:                  Optional[pl.String]  (dfguard.polars.Optional)\n"
        "                               pl.Int64 | None  (Python 3.11+ native union)"
    )


def _numpy_dtype_to_polars(dtype: Any) -> Any:
    """Map a numpy dtype to the corresponding Polars DataType."""
    km = _numpy_kind_map()
    entry = km.get(dtype.kind)
    if entry is None:
        import polars as pl
        return pl.Object
    if isinstance(entry, dict):
        pl_type = entry.get(dtype.itemsize)
        if pl_type is None:
            import polars as pl
            return pl.Object
        return pl_type
    return entry  # datetime / duration / object / string singletons


def dtypes_compatible(expected: Any, actual: Any) -> bool:
    """
    Return True when *actual* satisfies *expected*.

    Polars dtypes exist as both classes (``pl.Int64``) and instances (``Int64()``).
    DataFrame schemas store instances; annotations may use either form.
    """
    import polars as pl
    try:
        # class vs class
        if isinstance(expected, type) and isinstance(actual, type):
            return expected is actual
        # class vs instance  (e.g. annotation=pl.Int64, schema=Int64())
        if isinstance(expected, type) and issubclass(expected, pl.DataType):
            return isinstance(actual, expected)
        # instance vs class
        if isinstance(actual, type) and issubclass(actual, pl.DataType):
            return isinstance(expected, actual)
        # instance vs instance
        return bool(expected == actual)
    except Exception:
        return type(expected) is type(actual)


def polars_dtype_to_str(dtype: Any) -> str:
    """Return a Python source string for a Polars dtype (used by ``to_code``)."""
    import polars as pl

    name = type(dtype).__name__

    if isinstance(dtype, pl.List):
        return f"pl.List({polars_dtype_to_str(dtype.inner)})"
    if isinstance(dtype, pl.Array):
        return f"pl.Array({polars_dtype_to_str(dtype.inner)}, {dtype.width})"
    if isinstance(dtype, pl.Struct):
        fields_str = "{" + ", ".join(
            f'"{f.name}": {polars_dtype_to_str(f.dtype)}' for f in dtype.fields
        ) + "}"
        return f"pl.Struct({fields_str})"
    if isinstance(dtype, pl.Datetime):
        tu = dtype.time_unit or "us"
        tz = dtype.time_zone
        if tz:
            return f'pl.Datetime("{tu}", "{tz}")'
        return f'pl.Datetime("{tu}")' if tu != "us" else "pl.Datetime"
    if isinstance(dtype, pl.Duration):
        tu = dtype.time_unit or "us"
        return f'pl.Duration("{tu}")' if tu != "us" else "pl.Duration"
    if isinstance(dtype, pl.Categorical):
        return "pl.Categorical"
    if isinstance(dtype, pl.Enum):
        return f"pl.Enum({list(dtype.categories)!r})"
    # Simple types: Int64, String, Boolean, …
    if isinstance(dtype, type) and issubclass(dtype, pl.DataType):
        return f"pl.{dtype.__name__}"
    return f"pl.{name}"
