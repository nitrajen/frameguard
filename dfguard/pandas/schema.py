"""PandasSchema: declare a DataFrame's expected shape as a Python class."""

from __future__ import annotations

import typing
from typing import Any

from dfguard.pandas.exceptions import SchemaValidationError, TypeAnnotationError
from dfguard.pandas.types import annotation_to_pandas_dtype, dtypes_compatible, pandas_dtype_to_str


class _SchemaError:
    def __init__(self, message: str) -> None:
        self.message = message

    def __str__(self) -> str:
        return self.message


def _collect_assignment_fields(cls: type) -> dict[str, Any]:
    """Scan MRO for assignment-form fields (alias, dtype, or Optional wrapper)."""
    import numpy as np
    import pandas as pd

    from dfguard._base._alias import alias as _AliasType
    from dfguard._nullable import _NullableAnnotation

    collected: dict[str, Any] = {}
    for base in reversed(cls.__mro__):
        if base is object:
            continue
        own_annotations = set(getattr(base, "__annotations__", {}))
        for k, v in vars(base).items():
            if k.startswith("__") or callable(v) or k in own_annotations:
                continue
            if isinstance(  # noqa: UP038
                v, (_AliasType, np.dtype, pd.api.extensions.ExtensionDtype, _NullableAnnotation)
            ):
                collected[k] = v
    return collected


class _PandasSchemaMeta(type):
    """Collects annotations at class-definition time and caches the dtype dict."""

    def __new__(
        mcs,
        name: str,
        bases: tuple[type, ...],
        namespace: dict[str, Any],
    ) -> _PandasSchemaMeta:
        cls = super().__new__(mcs, name, bases, namespace)

        # Annotation form: get_type_hints handles PEP 563 + base-class merging.
        try:
            all_hints = typing.get_type_hints(cls)
        except Exception:
            all_hints = {}
            for base in reversed(cls.__mro__):
                if base is object:
                    continue
                all_hints.update(getattr(base, "__annotations__", {}))

        # Assignment form: alias() instances and dtype instances without annotation.
        assignment_fields = _collect_assignment_fields(cls)
        all_hints.update(assignment_fields)

        # Exclude private annotation-form fields (single _) but keep assignment-form
        # fields (which may start with _ due to sanitized column names like _2023_revenue).
        cls._schema_fields: dict[str, Any] = {  # type: ignore[misc, attr-defined]
            k: v for k, v in all_hints.items()
            if not k.startswith("__") and (not k.startswith("_") or k in assignment_fields)
        }
        cls._cached_dtype_dict: Any = None  # type: ignore[misc, attr-defined]

        # Auto-generate __doc__ when the user did not write one.
        if not cls.__dict__.get("__doc__") and cls._schema_fields:  # type: ignore[attr-defined]
            cls.__doc__ = _make_schema_doc(name, cls._schema_fields)  # type: ignore[attr-defined]

        return cls

    def __instancecheck__(cls, instance: Any) -> bool:
        """
        Enable ``isinstance(df, MySchema)`` as a subset schema check.

        Returns True when ``instance`` (a DataFrame or dataset wrapper) has
        at least every field declared in ``cls``, with matching dtypes.
        Extra columns are allowed.
        """
        import pandas as pd

        from dfguard.pandas.dataset import _PandasDatasetBase

        if isinstance(instance, pd.DataFrame):
            actual = dict(instance.dtypes)
        elif type.__instancecheck__(_PandasDatasetBase, instance):
            actual = dict(instance.dtypes)
        else:
            return False

        try:
            required = cls.to_dtype_dict()  # type: ignore[attr-defined]
            return all(
                col in actual and dtypes_compatible(expected, actual[col])
                for col, expected in required.items()
            )
        except Exception:
            return False


class PandasSchema(metaclass=_PandasSchemaMeta):
    """
    Declare a pandas DataFrame's expected shape as a Python class.

    Use this when you want to write down a schema without a live DataFrame.
    ``PandasSchema`` uses **subset matching**: the DataFrame must have every
    declared column, but extra columns are fine.  This mirrors ``SparkSchema``'s
    contract: declare what matters, ignore what doesn't.

    Annotation form (standard)::

        import numpy as np
        import pandas as pd
        from dfguard.pandas import PandasSchema, Optional, enforce

        class OrderSchema(PandasSchema):
            order_id:  np.dtype("int64")
            amount:    np.dtype("float64")
            name:      pd.StringDtype()
            active:    pd.BooleanDtype()
            tags:      list[str]                      # object dtype, holds str lists
            zip_code:  Optional[pd.StringDtype()]     # nullable

        @enforce
        def process(df: OrderSchema): ...

    Assignment form (avoids Pylance ``reportInvalidTypeForm`` warnings)::

        class OrderSchema(PandasSchema):
            order_id = np.dtype("int64")
            amount   = np.dtype("float64")
            name     = pd.StringDtype()

    Column-name aliasing (when the column name is not a valid identifier)::

        from dfguard.pandas import PandasSchema, alias

        class OrderSchema(PandasSchema):
            first_name    = alias("First Name",   np.dtype("object"))
            order_id      = alias("order-id",     np.dtype("int64"))
            revenue       = np.dtype("float64")   # no alias needed

    Child classes inherit all parent fields::

        class EnrichedSchema(OrderSchema):
            revenue: np.dtype("float64")   # adds revenue, keeps all parent fields

    Nullable columns
    ----------------
    Use ``Optional[dtype]`` from ``dfguard.pandas`` for dtype *instances*
    (e.g. ``pd.StringDtype()``) since ``typing.Optional`` rejects non-type args.
    For numpy scalar *types* (e.g. ``np.int64``), both work:
    ``Optional[np.int64]`` and the native ``np.int64 | None`` syntax.

    Pandas extension dtypes (``pd.Int64Dtype()``, ``pd.StringDtype()``, etc.)
    are inherently nullable and do not require wrapping in ``Optional``.

    Supported annotations
    ---------------------
    - numpy dtype instances:    ``np.dtype("int64")``, ``np.dtype("float64")``
    - numpy scalar types:       ``np.int64``, ``np.float32``, ``np.bool_``
    - pandas extension dtypes:  ``pd.StringDtype()``, ``pd.Int64Dtype()``, ``pd.BooleanDtype()``
    - Python builtins:          ``int``, ``float``, ``bool``, ``str``
    - Python generic types:     ``list[str]``, ``dict[str, Any]``  (object dtype)
    - Datetime:                 ``datetime.datetime``, ``pd.Timestamp``

    .. note::
        Requires **pandas >= 2.0**.
    """

    @classmethod
    def _fg_check(cls, value: Any, subset: bool) -> bool:
        """
        Enforcement protocol hook called by ``@enforce`` at runtime.

        Any class that exposes this method participates in dfguard enforcement.
        New DataFrame backends just add this classmethod; no enforcement changes needed.

        - ``subset=True``: extra columns in *value* are fine (default).
        - ``subset=False``: *value* must have exactly the declared columns.
        """
        if subset:
            return isinstance(value, cls)
        if not isinstance(value, cls):
            return False
        # Exact: declared column set must match actual
        import pandas as pd

        from dfguard.pandas.dataset import _PandasDatasetBase
        if isinstance(value, pd.DataFrame):
            actual_names = set(value.columns)
        elif type.__instancecheck__(_PandasDatasetBase, value):
            actual_names = set(value.columns)
        else:
            return False
        declared_names = _actual_col_names(cls._schema_fields)  # type: ignore[attr-defined]
        return actual_names == declared_names

    @classmethod
    def to_dtype_dict(cls) -> dict[str, Any]:
        """Return ``{column_name: dtype}`` for this schema. Cached after first call."""
        if cls._cached_dtype_dict is not None:  # type: ignore[attr-defined]
            return cls._cached_dtype_dict  # type: ignore[attr-defined]

        result: dict[str, Any] = {}
        for python_name, annotation in cls._schema_fields.items():  # type: ignore[attr-defined]
            col_name, actual_ann = _resolve_field(python_name, annotation)
            try:
                dtype, _nullable = annotation_to_pandas_dtype(actual_ann)
            except TypeError as exc:
                raise TypeAnnotationError(
                    f"Field '{python_name}' on {cls.__name__}: {exc}"
                ) from exc
            result[col_name] = dtype

        cls._cached_dtype_dict = result  # type: ignore[attr-defined]
        return result  # type: ignore[attr-defined]

    @classmethod
    def from_dtype_dict(
        cls,
        dtypes: dict[str, Any],
        name: str = "GeneratedSchema",
    ) -> type[PandasSchema]:
        """Create a ``PandasSchema`` subclass from a ``{col: dtype}`` dict.

        Column names that are not valid Python identifiers are automatically
        sanitized and wrapped with ``alias()``.
        """
        from dfguard._base._alias import alias as _AliasType
        from dfguard._base._col_names import sanitize

        annotations: dict[str, Any] = {}
        assignments: dict[str, Any] = {}

        for col_name, dtype in dtypes.items():
            py_name = sanitize(col_name)
            if py_name != col_name:
                assignments[py_name] = _AliasType(col_name, dtype)
            else:
                annotations[py_name] = dtype

        namespace: dict[str, Any] = {"__annotations__": annotations, **assignments}
        new_cls = _PandasSchemaMeta(name, (PandasSchema,), namespace)
        return new_cls  # type: ignore[return-value]

    # Aliases for API parity with SparkSchema / PolarsSchema
    to_struct = to_dtype_dict
    from_struct = from_dtype_dict

    @classmethod
    def validate(
        cls,
        df: Any,
        subset: bool = True,
    ) -> list[_SchemaError]:
        """
        Compare a DataFrame against this schema.

        Returns a list of errors; an empty list means the schema is valid.
        When ``subset=True`` (default), extra columns are fine.
        When ``subset=False``, extra columns are also reported as errors.
        """
        import pandas as pd

        from dfguard.pandas.dataset import _PandasDatasetBase

        if type.__instancecheck__(_PandasDatasetBase, df):
            actual = dict(df.dtypes)
        elif isinstance(df, pd.DataFrame):
            actual = dict(df.dtypes)
        else:
            actual = dict(df)  # accept a plain dtype dict for testing

        return _compare_schemas(cls._schema_fields, actual, strict=not subset)  # type: ignore[attr-defined]

    @classmethod
    def assert_valid(
        cls,
        df: Any,
        subset: bool = True,
        history: Any = None,
    ) -> None:
        """Like ``validate`` but raises ``SchemaValidationError`` on failure."""
        from dfguard.pandas.history import PandasSchemaHistory

        errors = cls.validate(df, subset=subset)
        if errors:
            import pandas as pd

            from dfguard.pandas.dataset import _PandasDatasetBase
            if type.__instancecheck__(_PandasDatasetBase, df):
                schema = dict(df.dtypes)
            elif isinstance(df, pd.DataFrame):
                schema = dict(df.dtypes)
            else:
                schema = {}
            h = history or PandasSchemaHistory.initial(schema)
            raise SchemaValidationError([str(e) for e in errors], h)

    @classmethod
    def empty(cls) -> Any:
        """Return an empty DataFrame with this schema and zero rows."""
        import pandas as pd
        return pd.DataFrame({
            col: pd.Series(dtype=dtype)
            for col, dtype in cls.to_dtype_dict().items()
        })

    @classmethod
    def to_code(cls) -> str:
        """Generate valid Python source code for this schema class."""
        from dfguard._base._alias import alias as _AliasType

        has_alias = any(
            isinstance(v, _AliasType) for v in cls._schema_fields.values()  # type: ignore[attr-defined]
        )
        imports = [
            "import numpy as np",
            "import pandas as pd",
        ]
        if has_alias:
            imports.append("from dfguard.pandas import PandasSchema, Optional, alias")
        else:
            imports.append("from dfguard.pandas import PandasSchema, Optional")
        lines = imports + ["", f"class {cls.__name__}(PandasSchema):"]

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
    def diff(cls, other: type[PandasSchema]) -> str:
        """Return a human-readable diff between two PandasSchema classes."""
        errors = _compare_schemas(other._schema_fields, cls.to_dtype_dict(), strict=True)  # type: ignore[attr-defined]
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
    """
    Compare expected annotations against actual DataFrame dtypes.

    ``expected_annotations`` maps Python names to raw annotations (or alias
    instances) as stored in ``_schema_fields``. ``actual_dtypes`` maps actual
    column names to dtype objects (from ``dict(df.dtypes)``).
    """
    errors: list[_SchemaError] = []

    for python_name, annotation in expected_annotations.items():
        col_name, actual_ann = _resolve_field(python_name, annotation)
        try:
            expected_dtype, _nullable = annotation_to_pandas_dtype(actual_ann)
        except TypeError as exc:
            errors.append(_SchemaError(f"Column '{col_name}': bad annotation: {exc}"))
            continue

        if col_name not in actual_dtypes:
            errors.append(_SchemaError(
                f"Missing column '{col_name}' (expected {expected_dtype})"
            ))
            continue

        actual_dtype = actual_dtypes[col_name]
        if not dtypes_compatible(expected_dtype, actual_dtype):
            errors.append(_SchemaError(
                f"Column '{col_name}': type mismatch: "
                f"expected {expected_dtype}, got {actual_dtype}"
            ))

    if strict:
        expected_cols = _actual_col_names(expected_annotations)
        for col_name in actual_dtypes:
            if col_name not in expected_cols:
                errors.append(_SchemaError(f"Unexpected column '{col_name}' (strict mode)"))

    return errors


def _annotation_to_str(annotation: Any) -> str:
    """Render a field annotation as its Python source string for ``to_code()``."""
    import typing

    from dfguard._nullable import _NullableAnnotation

    # dfguard Optional[X] wrapper
    if isinstance(annotation, _NullableAnnotation):
        return f"Optional[{_annotation_to_str(annotation.inner)}]"

    origin = getattr(annotation, "__origin__", None)
    args   = getattr(annotation, "__args__", ())

    # typing.Optional[X] == Union[X, None]
    if origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return f"Optional[{_annotation_to_str(non_none[0])}]"

    # X | None (Python 3.11 native union)
    import types as _builtin_types
    if hasattr(_builtin_types, "UnionType") and isinstance(annotation, _builtin_types.UnionType):
        non_none = [a for a in getattr(annotation, "__args__", ()) if a is not type(None)]
        if len(non_none) == 1:
            inner_str = _annotation_to_str(non_none[0])
            return f"{inner_str} | None"

    import numpy as np
    import pandas as pd

    # numpy dtype instance
    if isinstance(annotation, np.dtype):
        return pandas_dtype_to_str(annotation)

    # pandas extension dtype instance
    if isinstance(annotation, pd.api.extensions.ExtensionDtype):
        return pandas_dtype_to_str(annotation)

    # numpy scalar type (e.g. np.int64)
    if isinstance(annotation, type) and issubclass(annotation, np.generic):
        return f"np.{annotation.__name__}"

    # Python builtins and collections
    if hasattr(annotation, "__name__"):
        return annotation.__name__

    # parameterised generics (list[str], dict[str, Any])
    return repr(annotation)
