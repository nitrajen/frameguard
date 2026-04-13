"""PolarsSchema: declare a DataFrame's expected shape as a Python class."""

from __future__ import annotations

import typing
from typing import Any

from dfguard.polars.exceptions import SchemaValidationError, TypeAnnotationError
from dfguard.polars.types import annotation_to_polars_dtype, dtypes_compatible, polars_dtype_to_str


class _SchemaError:
    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return self.message


def _collect_assignment_fields(cls: type) -> dict[str, Any]:
    """Scan MRO for assignment-form fields (alias, dtype, or Optional wrapper)."""
    import polars as pl

    from dfguard._base._alias import alias as _AliasType
    from dfguard._nullable import _NullableAnnotation

    collected: dict[str, Any] = {}
    for base in reversed(cls.__mro__):
        if base is object:
            continue
        own_annotations = set(getattr(base, "__annotations__", {}))
        for k, v in vars(base).items():
            if k.startswith("__") or k in own_annotations:
                continue
            if isinstance(v, (_AliasType, _NullableAnnotation)):  # noqa: UP038
                collected[k] = v
            elif isinstance(v, pl.DataType):
                collected[k] = v
            elif isinstance(v, type) and issubclass(v, pl.DataType) and v is not pl.DataType:
                collected[k] = v
    return collected


class _PolarsSchemaMeta(type):
    """Collects annotations at class-definition time; caches the dtype dict."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> _PolarsSchemaMeta:
        cls = super().__new__(mcs, name, bases, namespace)

        try:
            all_hints = typing.get_type_hints(cls)
        except Exception:
            all_hints = {}
            for base in reversed(cls.__mro__):
                if base is object:
                    continue
                all_hints.update(getattr(base, "__annotations__", {}))

        # Assignment form: alias() instances and Polars dtype instances/classes.
        assignment_fields = _collect_assignment_fields(cls)
        all_hints.update(assignment_fields)

        cls._schema_fields: dict[str, Any] = {  # type: ignore[misc,attr-defined]
            k: v for k, v in all_hints.items()
            if not k.startswith("__") and (not k.startswith("_") or k in assignment_fields)
        }
        cls._cached_dtype_dict: Any = None  # type: ignore[misc,attr-defined]

        # Auto-generate __doc__ when the user did not write one.
        if not cls.__dict__.get("__doc__") and cls._schema_fields:  # type: ignore[attr-defined]
            cls.__doc__ = _make_schema_doc(name, cls._schema_fields)  # type: ignore[attr-defined]

        return cls

    def __instancecheck__(cls, instance: Any) -> bool:
        """``isinstance(df, MySchema)`` subset schema check (extra cols fine)."""
        import polars as pl

        from dfguard.polars.dataset import _PolarsDatasetBase

        if isinstance(instance, pl.LazyFrame):
            actual = dict(instance.collect_schema())
        elif isinstance(instance, pl.DataFrame):
            actual = dict(instance.schema)
        elif type.__instancecheck__(_PolarsDatasetBase, instance):
            actual = dict(instance.schema)
        else:
            return False

        try:
            required = cls.to_struct()  # type: ignore[attr-defined]
            return all(
                col in actual and dtypes_compatible(expected, actual[col])
                for col, expected in required.items()
            )
        except Exception:
            return False


class PolarsSchema(metaclass=_PolarsSchemaMeta):
    """
    Declare a Polars DataFrame's expected shape as a Python class.

    Uses **subset matching**: the DataFrame must have every declared column with
    matching dtype, but extra columns are fine. Use ``schema_of(df)`` for exact
    matching (no extra columns allowed).

    Annotation form (standard)::

        import polars as pl
        from dfguard.polars import PolarsSchema, Optional, enforce

        class OrderSchema(PolarsSchema):
            order_id: pl.Int64
            amount:   pl.Float64
            tags:     pl.List(pl.String)   # first-class nested type
            name:     Optional[pl.String]  # explicitly nullable

        @enforce
        def process(df: OrderSchema): ...

    Assignment form (avoids Pylance ``reportInvalidTypeForm`` on instance-form dtypes)::

        class OrderSchema(PolarsSchema):
            order_id = pl.Int64
            tags     = pl.List(pl.String)

    Column-name aliasing::

        from dfguard.polars import PolarsSchema, alias

        class OrderSchema(PolarsSchema):
            first_name = alias("First Name", pl.String)
            order_id   = alias("order-id",   pl.Int64)

    Child classes inherit all parent fields::

        class EnrichedSchema(OrderSchema):
            revenue: pl.Float64

    Python builtins and generics
    ----------------------------
    ``int`` -> ``pl.Int64``, ``float`` -> ``pl.Float64``, ``str`` -> ``pl.String``,
    ``bool`` -> ``pl.Boolean``, ``bytes`` -> ``pl.Binary``.
    ``list[T]`` -> ``pl.List(T)``.  ``dict`` -> ``pl.Object``.

    Nullability
    -----------
    All Polars columns are physically nullable. Use ``Optional[T]`` to *declare*
    that nulls are expected. ``X | None`` (Python 3.11 native union) also works.

    .. note::
        Requires **Polars >= 1.0**.
    """

    @classmethod
    def _fg_check(cls, value: Any, subset: bool) -> bool:
        if subset:
            return isinstance(value, cls)
        if not isinstance(value, cls):
            return False
        import polars as pl

        from dfguard.polars.dataset import _PolarsDatasetBase
        if isinstance(value, pl.LazyFrame):
            actual_names = set(value.collect_schema())
        elif isinstance(value, pl.DataFrame):
            actual_names = set(value.schema)
        elif type.__instancecheck__(_PolarsDatasetBase, value):
            actual_names = set(value.schema)
        else:
            return False
        declared_names = _actual_col_names(cls._schema_fields)  # type: ignore[attr-defined]
        return actual_names == declared_names

    @classmethod
    def to_struct(cls) -> dict[str, Any]:
        """Return ``{col: polars_dtype}`` for this schema. Cached after first call."""
        if cls._cached_dtype_dict is not None:  # type: ignore[attr-defined]
            return cls._cached_dtype_dict  # type: ignore[attr-defined]

        result: dict[str, Any] = {}
        for python_name, annotation in cls._schema_fields.items():  # type: ignore[attr-defined]
            col_name, actual_ann = _resolve_field(python_name, annotation)
            try:
                dtype, _nullable = annotation_to_polars_dtype(actual_ann)
            except TypeError as exc:
                raise TypeAnnotationError(
                    f"Field '{python_name}' on {cls.__name__}: {exc}"
                ) from exc
            result[col_name] = dtype

        cls._cached_dtype_dict = result  # type: ignore[attr-defined]
        return result  # type: ignore[attr-defined]

    @classmethod
    def from_struct(
        cls,
        struct: dict[str, Any],
        name: str = "GeneratedSchema",
    ) -> type[PolarsSchema]:
        """Create a ``PolarsSchema`` subclass from a ``{col: polars_dtype}`` dict.

        Column names that are not valid Python identifiers are automatically
        sanitized and wrapped with ``alias()``.
        """
        from dfguard._base._alias import alias as _AliasType
        from dfguard._base._col_names import sanitize

        annotations: dict[str, Any] = {}
        assignments: dict[str, Any] = {}

        for col_name, dtype in struct.items():
            py_name = sanitize(col_name)
            if py_name != col_name:
                assignments[py_name] = _AliasType(col_name, dtype)
            else:
                annotations[py_name] = dtype

        namespace: dict[str, Any] = {"__annotations__": annotations, **assignments}
        new_cls = _PolarsSchemaMeta(name, (PolarsSchema,), namespace)
        return new_cls  # type: ignore[return-value]

    @classmethod
    def validate(cls, df: Any, subset: bool = True) -> list[_SchemaError]:
        """
        Compare a DataFrame or LazyFrame against this schema.

        Returns a list of errors; empty means valid.
        ``subset=True`` (default): extra columns are fine.
        ``subset=False``: extra columns are also errors.
        """
        import polars as pl

        from dfguard.polars.dataset import _PolarsDatasetBase

        if type.__instancecheck__(_PolarsDatasetBase, df):
            actual = dict(df.schema)
        elif isinstance(df, pl.LazyFrame):
            actual = dict(df.collect_schema())
        elif isinstance(df, pl.DataFrame):
            actual = dict(df.schema)
        else:
            actual = dict(df)

        return _compare_schemas(cls._schema_fields, actual, strict=not subset)  # type: ignore[attr-defined]

    @classmethod
    def assert_valid(cls, df: Any, subset: bool = True, history: Any = None) -> None:
        """Like ``validate`` but raises ``SchemaValidationError`` on failure."""
        from dfguard.polars.history import PolarsSchemaHistory

        errors = cls.validate(df, subset=subset)
        if errors:
            import polars as pl

            from dfguard.polars.dataset import _PolarsDatasetBase
            if type.__instancecheck__(_PolarsDatasetBase, df):
                schema = dict(df.schema)
            elif isinstance(df, pl.LazyFrame):
                schema = dict(df.collect_schema())
            elif isinstance(df, pl.DataFrame):
                schema = dict(df.schema)
            else:
                schema = {}
            h = history or PolarsSchemaHistory.initial(schema)
            raise SchemaValidationError([str(e) for e in errors], h)

    @classmethod
    def empty(cls) -> Any:
        """Return an empty Polars DataFrame with this schema and zero rows."""
        import polars as pl
        return pl.DataFrame(schema=cls.to_struct())

    @classmethod
    def to_code(cls) -> str:
        """Generate valid Python source code for this schema class."""
        from dfguard._base._alias import alias as _AliasType

        has_alias = any(
            isinstance(v, _AliasType) for v in cls._schema_fields.values()  # type: ignore[attr-defined]
        )
        imports = ["import polars as pl"]
        if has_alias:
            imports.append("from dfguard.polars import PolarsSchema, Optional, alias")
        else:
            imports.append("from dfguard.polars import PolarsSchema, Optional")
        lines = imports + ["", f"class {cls.__name__}(PolarsSchema):"]

        if not cls._schema_fields:  # type: ignore[attr-defined]
            lines.append("    pass")
        else:
            for python_name, annotation in cls._schema_fields.items():  # type: ignore[attr-defined]
                if isinstance(annotation, _AliasType):
                    dtype_str = _annotation_to_str(annotation.dtype)
                    lines.append(
                        f"    {python_name} = alias({annotation.src!r}, {dtype_str})"
                    )
                else:
                    lines.append(f"    {python_name}: {_annotation_to_str(annotation)}")
        return "\n".join(lines)

    @classmethod
    def diff(cls, other: type[PolarsSchema]) -> str:
        """Return a human-readable diff between two PolarsSchema classes."""
        errors = _compare_schemas(other._schema_fields, cls.to_struct(), strict=True)  # type: ignore[attr-defined]
        if not errors:
            return f"{cls.__name__} and {other.__name__} are identical"
        lines = [f"Diff {cls.__name__} -> {other.__name__}:"]
        for e in errors:
            lines.append(f"  {e}")
        return "\n".join(lines)

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls._cached_dtype_dict = None  # type: ignore[attr-defined]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _resolve_field(python_name: str, annotation: Any) -> tuple[str, Any]:
    """Return (actual_col_name, actual_annotation) resolving any alias wrapper."""
    from dfguard._base._alias import alias as _AliasType
    if isinstance(annotation, _AliasType):
        return annotation.src, annotation.dtype
    return python_name, annotation


def _actual_col_names(schema_fields: dict[str, Any]) -> set[str]:
    """Return the set of actual DataFrame column names from a _schema_fields dict."""
    return {_resolve_field(k, v)[0] for k, v in schema_fields.items()}


def _make_schema_doc(name: str, schema_fields: dict[str, Any]) -> str:
    """Build a docstring listing schema columns for IDE hover."""
    lines = [f"{name} columns:", ""]
    for python_name, annotation in schema_fields.items():
        col_name, actual_ann = _resolve_field(python_name, annotation)
        alias_note = f" (column '{col_name}')" if col_name != python_name else ""
        lines.append(f"  {python_name}{alias_note}: {actual_ann}")
    return "\n".join(lines)


def _compare_schemas(
    expected_annotations: dict[str, Any],
    actual_dtypes: dict[str, Any],
    strict: bool,
) -> list[_SchemaError]:
    errors: list[_SchemaError] = []

    for python_name, annotation in expected_annotations.items():
        col_name, actual_ann = _resolve_field(python_name, annotation)
        try:
            expected_dtype, _nullable = annotation_to_polars_dtype(actual_ann)
        except TypeError as exc:
            errors.append(_SchemaError(f"Column '{col_name}': bad annotation: {exc}"))
            continue

        if col_name not in actual_dtypes:
            errors.append(_SchemaError(
                f"Missing column '{col_name}' (expected {expected_dtype})"
            ))
            continue

        if not dtypes_compatible(expected_dtype, actual_dtypes[col_name]):
            errors.append(_SchemaError(
                f"Column '{col_name}': type mismatch: "
                f"expected {expected_dtype}, got {actual_dtypes[col_name]}"
            ))

    if strict:
        expected_cols = _actual_col_names(expected_annotations)
        for col_name in actual_dtypes:
            if col_name not in expected_cols:
                errors.append(_SchemaError(f"Unexpected column '{col_name}' (strict mode)"))

    return errors


def _annotation_to_str(annotation: Any) -> str:
    """Render a field annotation as Python source for ``to_code()``."""
    import typing as _typing

    from dfguard._nullable import _NullableAnnotation

    if isinstance(annotation, _NullableAnnotation):
        return f"Optional[{_annotation_to_str(annotation.inner)}]"

    origin = getattr(annotation, "__origin__", None)
    args   = getattr(annotation, "__args__", ())

    if origin is _typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return f"Optional[{_annotation_to_str(non_none[0])}]"

    import types as _bt
    if hasattr(_bt, "UnionType") and isinstance(annotation, _bt.UnionType):
        non_none = [a for a in getattr(annotation, "__args__", ()) if a is not type(None)]
        if len(non_none) == 1:
            return f"{_annotation_to_str(non_none[0])} | None"

    import polars as pl

    if isinstance(annotation, pl.DataType):
        return polars_dtype_to_str(annotation)
    if isinstance(annotation, type) and issubclass(annotation, pl.DataType):
        return f"pl.{annotation.__name__}"

    # list[T]
    if origin is list and args:
        return f"list[{_annotation_to_str(args[0])}]"

    if hasattr(annotation, "__name__"):
        return annotation.__name__

    return repr(annotation)
