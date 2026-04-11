"""
Type coercion rules for PySpark, implemented in pure Python.

This mirrors Spark's Catalyst TypeCoercion and DecimalPrecision rules so that
dfguard can resolve derived column types without a running Spark session.

Rules source: org.apache.spark.sql.catalyst.analysis.TypeCoercion
              org.apache.spark.sql.catalyst.analysis.DecimalPrecision
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Integer types expressed as Decimal equivalents (precision, scale=0)
# ---------------------------------------------------------------------------

_INT_AS_DECIMAL = {
    "ByteType":    (3,  0),
    "ShortType":   (5,  0),
    "IntegerType": (10, 0),
    "LongType":    (20, 0),
}

# Numeric widening rank (higher = wider). Float/Double are above Long because
# Spark promotes integer+float → double (lossy but matches Spark behaviour).
_NUMERIC_RANK: dict[str, int] = {
    "ByteType":    1,
    "ShortType":   2,
    "IntegerType": 3,
    "LongType":    4,
    "FloatType":   5,
    "DoubleType":  6,
}


def _type_name(dt: Any) -> str:
    return type(dt).__name__


def _is_integral(dt: Any) -> bool:
    return _type_name(dt) in _INT_AS_DECIMAL


def _is_fractional(dt: Any) -> bool:
    return _type_name(dt) in ("FloatType", "DoubleType")


def _is_numeric(dt: Any) -> bool:
    return _is_integral(dt) or _is_fractional(dt) or _type_name(dt) == "DecimalType"


# ---------------------------------------------------------------------------
# Decimal precision arithmetic  (mirrors DecimalPrecision.scala)
# ---------------------------------------------------------------------------

def _decimal_for_integral(dt: Any) -> tuple[int, int]:
    """Return (precision, scale) for an integer type treated as Decimal."""
    return _INT_AS_DECIMAL[_type_name(dt)]


def _decimal_add(p1: int, s1: int, p2: int, s2: int) -> tuple[int, int]:
    """Decimal addition/subtraction result precision and scale."""
    scale = max(s1, s2)
    precision = max(p1 - s1, p2 - s2) + scale + 1
    return precision, scale


def _decimal_mul(p1: int, s1: int, p2: int, s2: int) -> tuple[int, int]:
    """Decimal multiplication result precision and scale."""
    scale = s1 + s2
    precision = p1 + p2 + 1
    return precision, scale


def _decimal_div(p1: int, s1: int, p2: int, s2: int) -> tuple[int, int]:
    """Decimal division result precision and scale."""
    scale = max(6, s1 + p2 + 1)
    precision = p1 - s1 + s2 + scale
    return precision, scale


def _make_decimal(p: int, s: int) -> Any:
    from pyspark.sql import types as T
    # Spark caps precision at 38
    p = min(p, 38)
    return T.DecimalType(p, s)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def coerce_add(left: Any, right: Any) -> Any:
    """Return the result type of ``left + right`` (or ``left - right``)."""
    return _coerce_binary(left, right, "add")


def coerce_mul(left: Any, right: Any) -> Any:
    """Return the result type of ``left * right``."""
    return _coerce_binary(left, right, "mul")


def coerce_div(left: Any, right: Any) -> Any:
    """Return the result type of ``left / right``. Always Double in Spark."""
    from pyspark.sql import types as T
    if not (_is_numeric(left) and _is_numeric(right)):
        raise TypeError(f"Cannot divide {_type_name(left)} by {_type_name(right)}")
    return T.DoubleType()


def coerce_mod(left: Any, right: Any) -> Any:
    """Return the result type of ``left % right`` (modulo)."""
    return _coerce_binary(left, right, "add")   # same widening rules as add


def _coerce_binary(left: Any, right: Any, op: str) -> Any:
    from pyspark.sql import types as T

    ln, rn = _type_name(left), _type_name(right)

    # Both are simple numeric (no Decimal involved)
    if ln in _NUMERIC_RANK and rn in _NUMERIC_RANK:
        # Float/Double always promotes to Double (Spark rule)
        if _is_fractional(left) or _is_fractional(right):
            return T.DoubleType()
        # Pure integer widening
        winner = left if _NUMERIC_RANK[ln] >= _NUMERIC_RANK[rn] else right
        return type(winner)()

    # At least one side is Decimal
    if ln == "DecimalType" or rn == "DecimalType":
        # Float or Double wins over Decimal
        if _is_fractional(left) or _is_fractional(right):
            return T.DoubleType()

        # Normalise both sides to (precision, scale)
        if ln == "DecimalType":
            p1, s1 = left.precision, left.scale
        else:
            p1, s1 = _decimal_for_integral(left)

        if rn == "DecimalType":
            p2, s2 = right.precision, right.scale
        else:
            p2, s2 = _decimal_for_integral(right)

        if op == "mul":
            p, s = _decimal_mul(p1, s1, p2, s2)
        else:
            p, s = _decimal_add(p1, s1, p2, s2)

        return _make_decimal(p, s)

    raise TypeError(f"Cannot coerce {ln} and {rn} for operation '{op}'")


def coerce_comparison(left: Any, right: Any) -> Any:
    """Comparison (==, <, >, <=, >=) always returns BooleanType."""
    from pyspark.sql import types as T
    return T.BooleanType()


def coerce_cast(target: Any) -> Any:
    """Explicit cast: the target type is the result."""
    return target


def result_type(left: Any, right: Any, op: str) -> Any:
    """
    Resolve the output DataType for a binary operation between two typed columns.

    Parameters
    ----------
    left, right : DataType instances
    op : one of '+', '-', '*', '/', '%', '==', '!=', '<', '<=', '>', '>='

    Returns
    -------
    DataType instance representing the result type.

    Examples
    --------
    >>> from pyspark.sql import types as T
    >>> result_type(T.IntegerType(), T.DecimalType(10, 2), '+')
    DecimalType(13,2)
    >>> result_type(T.LongType(), T.DoubleType(), '*')
    DoubleType()
    >>> result_type(T.IntegerType(), T.IntegerType(), '/')
    DoubleType()
    """
    if op in ("+", "-"):
        return coerce_add(left, right)
    if op == "*":
        return coerce_mul(left, right)
    if op == "/":
        return coerce_div(left, right)
    if op == "%":
        return coerce_mod(left, right)
    if op in ("==", "!=", "<", "<=", ">", ">="):
        return coerce_comparison(left, right)
    raise ValueError(f"Unknown operator: {op!r}")
