"""Schema enforcement without touching non-schema arguments."""

from __future__ import annotations

import functools
import importlib
import inspect
import pkgutil
import types
import warnings
from collections.abc import Callable
from typing import Any, TypeVar, overload

F = TypeVar("F", bound=Callable[..., Any])

_ENABLED = True   # fg.disable() / fg.enable_enforcement()
_SUBSET  = True   # fg.arm(subset=...) — global default; function-level overrides this

_UNSET = object()  # sentinel: "no function-level override, use global"


def _is_schema_type(annotation: Any) -> bool:
    """
    Return True when *annotation* participates in frameguard enforcement.

    Any class that exposes a ``_fg_check(value, subset) -> bool`` classmethod
    is treated as a schema type. This is the extension point: new DataFrame
    backends (pandas, polars, …) just need to add ``_fg_check`` to their
    schema class — no changes to enforcement code required.
    """
    return isinstance(annotation, type) and callable(getattr(annotation, "_fg_check", None))


def _schema_matches(value: Any, annotation: type, subset: bool) -> bool:
    """
    Check whether *value* satisfies *annotation*.

    Delegates to ``annotation._fg_check(value, subset)`` when available.
    The meaning of *subset* is left to each schema type:

    - ``schema_of`` types (``_TypedDatasetBase``) ignore *subset* — always exact.
    - ``SparkSchema`` types respect *subset*:
        - ``True``  — extra columns in *value* are fine.
        - ``False`` — *value* must have exactly the declared columns, nothing extra.
    """
    checker = getattr(annotation, "_fg_check", None)
    if callable(checker):
        return checker(value, subset)
    return isinstance(value, annotation)


def _arm_module_dict(module_dict: dict[str, Any], *, subset: Any) -> None:
    """Patch all public functions in a module's __dict__ with enforce()."""
    for name, obj in list(module_dict.items()):
        if name.startswith("_"):
            continue
        if isinstance(obj, types.FunctionType):
            # Pass _UNSET so each wrapped function reads _SUBSET at call-time,
            # unless overridden at decoration time by the caller.
            wrapped = enforce(obj, subset=subset)
            if wrapped is not obj:
                module_dict[name] = wrapped


def arm(
    module: Any = None,
    *,
    package: str | None = None,
    subset: bool = True,
) -> None:
    """
    Arm the entire calling package and set the global subset default.

    Call once — typically in your entry point, ``settings.py``, or ``__init__.py``::

        import frameguard.pyspark as fg

        fg.arm()                # subset=True (default): extra columns are fine
        fg.arm(subset=False)    # exact match: no extra columns allowed anywhere

    The ``subset`` value becomes the global default. Individual functions decorated
    with ``@fg.enforce(subset=...)`` override it for that function only.

    **Specific module object**::

        fg.arm(my_module)

    **Explicit package name**::

        fg.arm(package="my_pipeline.nodes")
    """
    global _SUBSET
    _SUBSET = subset

    if isinstance(module, types.ModuleType):
        _arm_module_dict(vars(module), subset=_UNSET)
        return

    if package is None:
        frame = inspect.currentframe()
        if frame is None or frame.f_back is None:
            return
        caller_globals = frame.f_back.f_globals
        package = caller_globals.get("__package__") or caller_globals.get("__name__", "")

    if not package or package == "__main__":
        warnings.warn(
            "frameguard.pyspark.arm() called from __main__. "
            "Use @frameguard.pyspark.enforce on individual functions instead.",
            stacklevel=2,
        )
        return

    pkg = importlib.import_module(package)
    _arm_module_dict(vars(pkg), subset=_UNSET)
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is not None:
        for _, mod_name, _ in pkgutil.walk_packages(pkg_path, prefix=package + "."):
            try:
                mod = importlib.import_module(mod_name)
                _arm_module_dict(vars(mod), subset=_UNSET)
            except Exception as exc:
                warnings.warn(
                    f"frameguard: skipped arming module '{mod_name}': {exc}",
                    stacklevel=2,
                )


def disable() -> None:
    """
    Disable all schema enforcement globally.

    ``@fg.enforce(always=True)`` functions are not affected.
    """
    global _ENABLED
    _ENABLED = False


def enable_enforcement() -> None:
    """Re-enable enforcement after a ``fg.disable()`` call."""
    global _ENABLED
    _ENABLED = True


@overload
def enforce(func: F) -> F: ...

@overload
def enforce(func: None = None, *, always: bool = ..., subset: bool = ...) -> Callable[[F], F]: ...


def enforce(
    func: F | None = None,
    *,
    always: bool = False,
    subset: Any = _UNSET,
) -> F | Callable[[F], F]:
    """
    Validate schema annotations on DataFrame arguments.

    Only intercepts parameters annotated with a ``fg.schema_of`` type or a
    ``fg.SparkSchema`` subclass. All other arguments are left completely alone.

    **Default** — inherits the global ``subset`` set by ``fg.arm()``::

        @fg.enforce
        def process(df: OrderSchema, label: str): ...

    **subset=True** — extra columns in the DataFrame are fine (overrides global)::

        @fg.enforce(subset=True)
        def process(df: OrderSchema): ...

    **subset=False** — DataFrame must match the schema exactly (overrides global)::

        @fg.enforce(subset=False)
        def process(df: OrderSchema): ...

    **always=True** — enforces even after ``fg.disable()`` is called::

        @fg.enforce(always=True)
        def write_to_prod(df: FinalSchema, table: str): ...
    """
    # Capture the function-level subset at decoration time.
    # If _UNSET, the wrapper reads _SUBSET at call-time (respects fg.arm changes).
    subset_override = subset

    def decorator(f: F) -> F:
        params = inspect.signature(f).parameters
        schema_params = [
            (name, param.annotation)
            for name, param in params.items()
            if param.annotation is not inspect.Parameter.empty
            and _is_schema_type(param.annotation)
        ]

        if not schema_params:
            return f  # nothing schema-typed, zero overhead

        sig = inspect.signature(f)

        @functools.wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            if not _ENABLED and not always:
                return f(*args, **kwargs)

            # Function-level subset wins; fall back to global if not set.
            effective_subset = _SUBSET if subset_override is _UNSET else subset_override

            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            for param_name, annotation in schema_params:
                if param_name not in bound.arguments:
                    continue
                value = bound.arguments[param_name]
                if not _schema_matches(value, annotation, subset=effective_subset):
                    _raise_schema_mismatch(f.__name__, param_name, annotation, value)

            return f(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    if func is not None:
        return decorator(func)
    return decorator


def _raise_schema_mismatch(
    func_name: str,
    param_name: str,
    annotation: type,
    value: Any,
) -> None:
    actual_schema = getattr(value, "schema", None)
    if actual_schema is not None:
        actual_str = ", ".join(
            f"{f.name}:{f.dataType.simpleString()}" for f in actual_schema.fields
        )
    else:
        actual_str = type(value).__name__

    expected_schema = getattr(annotation, "_expected_schema", None)
    if expected_schema is not None:
        expected_str = ", ".join(
            f"{f.name}:{f.dataType.simpleString()}" for f in expected_schema.fields
        )
    else:
        expected_str = getattr(annotation, "__name__", repr(annotation))

    raise TypeError(
        f"Schema mismatch in {func_name}() argument '{param_name}':\n"
        f"  expected: {expected_str}\n"
        f"  received: {actual_str}"
    )
