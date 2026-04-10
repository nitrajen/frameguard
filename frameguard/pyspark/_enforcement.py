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

_ENABLED = True  # global flag; disable() sets this to False


def _is_schema_type(annotation: Any) -> bool:
    """Return True only for frameguard schema types."""
    if not isinstance(annotation, type):
        return False
    try:
        from frameguard.pyspark.dataset import _TypedDatasetBase
        from frameguard.pyspark.schema import SparkSchema
        return issubclass(annotation, _TypedDatasetBase | SparkSchema)
    except Exception:
        return False


def _arm_module_dict(module_dict: dict[str, Any]) -> None:
    """Patch all public functions in a module's __dict__ with enforce()."""
    for name, obj in list(module_dict.items()):
        if name.startswith("_"):
            continue
        if isinstance(obj, types.FunctionType):
            wrapped = enforce(obj)
            if wrapped is not obj:
                module_dict[name] = wrapped


def arm(
    module: Any = None,
    *,
    package: str | None = None,
) -> None:
    """
    Enforce schema annotations for all public functions in the calling package.

    Call once — typically in your entry point, ``settings.py``, or ``__init__.py``.
    Every public function in every module of the package is enforced automatically::

        import frameguard.pyspark as fg
        fg.arm()

    **Specific module object** — arm one module only::

        import my_pipeline.nodes as nodes_mod
        fg.arm(nodes_mod)

    **Explicit package name** — arm a package other than the calling one::

        fg.arm(package="my_pipeline.nodes")

    Functions wrapped by ``arm()`` respect the global ``disable()`` flag but
    not the ``always=True`` override (use ``@fg.enforce(always=True)`` for that).
    """
    if isinstance(module, types.ModuleType):
        _arm_module_dict(vars(module))
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
    _arm_module_dict(vars(pkg))
    pkg_path = getattr(pkg, "__path__", None)
    if pkg_path is not None:
        for _, mod_name, _ in pkgutil.walk_packages(pkg_path, prefix=package + "."):
            try:
                mod = importlib.import_module(mod_name)
                _arm_module_dict(vars(mod))
            except Exception as exc:
                warnings.warn(
                    f"frameguard: skipped arming module '{mod_name}': {exc}",
                    stacklevel=2,
                )


def disable() -> None:
    """
    Disable all frameguard schema enforcement globally.

    Every ``@enforce`` wrapper and every function wrapped by ``arm()`` becomes
    a pass-through. Functions decorated with ``@enforce(always=True)`` are
    **not** affected — they always enforce, regardless of this flag::

        from frameguard.pyspark import disable, enable_enforcement

        disable()
        process(wrong_df)    # no error — enforcement is off
        critical(wrong_df)   # still raises — @enforce(always=True) ignores disable()

        enable_enforcement()
        process(wrong_df)    # raises again
    """
    global _ENABLED
    _ENABLED = False


def enable_enforcement() -> None:
    """Re-enable enforcement after a ``disable()`` call."""
    global _ENABLED
    _ENABLED = True


@overload
def enforce(func: F) -> F: ...

@overload
def enforce(func: None = None, *, always: bool = ...) -> Callable[[F], F]: ...


def enforce(
    func: F | None = None,
    *,
    always: bool = False,
) -> F | Callable[[F], F]:
    """
    Validate schema annotations on DataFrame arguments only.

    Only intercepts parameters annotated with a ``schema_of`` type or a
    ``SparkSchema`` subclass. All other arguments (``str``, ``int``, ``list``,
    custom classes) are left completely alone.

    **Basic usage** — respects ``disable()``::

        @enforce
        def enrich(df: RawSchema, label: str):
            return df.withColumn("revenue", F.col("amount") * F.col("quantity"))

    **always=True** — enforces even after ``disable()`` is called. Use this
    for functions where a schema mismatch would be genuinely dangerous, like
    a function that writes to production or deletes data::

        @enforce(always=True)
        def write_to_prod(df: FinalSchema, table: str):
            df.write.saveAsTable(table)

        disable()            # turns off enforcement globally
        enrich(wrong_df)     # passes silently
        write_to_prod(wrong_df)  # still raises — always=True ignores disable()
    """
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

            bound = sig.bind(*args, **kwargs)
            bound.apply_defaults()

            for param_name, annotation in schema_params:
                if param_name not in bound.arguments:
                    continue
                value = bound.arguments[param_name]
                if not isinstance(value, annotation):
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
