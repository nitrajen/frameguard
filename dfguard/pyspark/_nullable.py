"""
Nullable field marker for SparkSchema.

``typing.Optional[T.StringType()]`` raises TypeError on Python 3.10 because
PySpark DataType instances are not callable classes, and Python's typing module
validates this. Python 3.11+ relaxed the check.

This module provides a drop-in replacement that works on Python 3.10+.
"""

from __future__ import annotations

from typing import Any


class _NullableAnnotation:
    """Wraps a DataType annotation to mark the field as nullable."""

    __slots__ = ("inner",)

    def __init__(self, inner: Any) -> None:
        self.inner = inner

    def __class_getitem__(cls, item: Any) -> _NullableAnnotation:
        return cls(item)

    def __repr__(self) -> str:
        return f"Optional[{self.inner!r}]"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _NullableAnnotation) and self.inner == other.inner

    def __hash__(self) -> int:
        try:
            return hash(("_NullableAnnotation", self.inner))
        except TypeError:
            return id(self)


#: Drop-in for ``typing.Optional`` that works on Python 3.10+ with PySpark DataType fields.
#:
#: Usage::
#:
#:     from dfguard.pyspark import Optional
#:
#:     class OrderSchema(fg.SparkSchema):
#:         order_id: T.LongType()
#:         zip:      Optional[T.StringType()]  # nullable field
Optional = _NullableAnnotation
