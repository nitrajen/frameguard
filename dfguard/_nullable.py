"""
Nullable field marker, shared across all dfguard backends.

``typing.Optional[some_instance]`` raises ``TypeError`` because Python's typing
module validates that ``Optional`` args are types, not instances. Both PySpark
DataType instances and pandas extension dtype instances (``pd.StringDtype()``,
etc.) trigger this.

This module provides a drop-in replacement that works on Python 3.11+ with any
annotation that is a value rather than a class.
"""

from __future__ import annotations

from typing import Any


class _NullableAnnotation:
    """Wraps a dtype/type annotation to mark the field as nullable."""

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


#: Drop-in for ``typing.Optional`` that works with dtype-instance annotations.
#:
#: Imported by each backend's public ``__init__.py`` so users write::
#:
#:     from dfguard.pyspark import Optional   # or dfguard.pandas
#:     class MySchema(...):
#:         col: Optional[SomeDtypeInstance()]
Optional = _NullableAnnotation
