"""Shared enforcement building blocks used by every backend.

Each backend's ``_enforcement.py`` keeps its own module-level globals
(``_ENABLED``, ``_ARMED``, ``_SUBSET``) for test-isolation, and calls
``make_enforce`` / ``make_arm`` here to build its public functions.
"""

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

# Single sentinel shared across all backends so subset override works correctly.
_UNSET: Any = object()


def _is_schema_type(annotation: Any) -> bool:
    """True when *annotation* has ``_fg_check`` — the dfguard enforcement protocol."""
    return isinstance(annotation, type) and callable(getattr(annotation, "_fg_check", None))


def _schema_matches(value: Any, annotation: type, subset: bool) -> bool:
    checker = getattr(annotation, "_fg_check", None)
    return bool(checker(value, subset)) if callable(checker) else isinstance(value, annotation)


def _arm_module_dict(module_dict: dict[str, Any], enforce_fn: Callable[..., Any]) -> None:
    for name, obj in list(module_dict.items()):
        if name.startswith("_"):
            continue
        if isinstance(obj, types.FunctionType):
            wrapped = enforce_fn(subset=_UNSET)(obj)
            if wrapped is not obj:
                module_dict[name] = wrapped


def make_enforce(
    get_enabled: Callable[[], bool],
    get_subset:  Callable[[], bool],
    raise_mismatch: Callable[[str, str, type, Any], None],
) -> Any:
    """
    Build an ``enforce`` decorator bound to a backend's runtime state.

    Parameters
    ----------
    get_enabled:    ``() -> bool`` — reads backend's ``_ENABLED``
    get_subset:     ``() -> bool`` — reads backend's ``_SUBSET``
    raise_mismatch: called when a check fails; must raise ``TypeError``
    """
    @overload
    def enforce(func: F) -> F: ...
    @overload
    def enforce(func: None = None, *, subset: bool = ...) -> Callable[[F], F]: ...

    def enforce(func: F | None = None, *, subset: Any = _UNSET) -> F | Callable[[F], F]:
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
                return f

            sig = inspect.signature(f)

            @functools.wraps(f)
            def wrapper(*args: Any, **kwargs: Any) -> Any:
                if not get_enabled():
                    return f(*args, **kwargs)
                effective = get_subset() if subset_override is _UNSET else subset_override
                bound = sig.bind(*args, **kwargs)
                bound.apply_defaults()
                for param_name, annotation in schema_params:
                    if param_name not in bound.arguments:
                        continue
                    value = bound.arguments[param_name]
                    if not _schema_matches(value, annotation, subset=effective):
                        raise_mismatch(f.__name__, param_name, annotation, value)
                return f(*args, **kwargs)

            return wrapper  # type: ignore[return-value]

        return decorator(func) if func is not None else decorator

    return enforce


def make_arm(
    backend_name:   str,
    module_globals: dict[str, Any],
    get_enforce_fn: Callable[[], Callable[..., Any]],
) -> Callable[..., Any]:
    """
    Build an ``arm`` function bound to a backend's module globals dict.

    ``module_globals`` must be the backend's ``globals()`` — mutations to
    ``_ENABLED``, ``_ARMED``, ``_SUBSET`` propagate to the module namespace,
    preserving test-level direct assignment (``_e._ENABLED = True``).
    """
    def arm(
        module: Any = None,
        *,
        package: str | None = None,
        subset: bool = True,
    ) -> None:
        module_globals["_SUBSET"]  = subset
        module_globals["_ENABLED"] = True
        enforce_fn = get_enforce_fn()

        if isinstance(module, types.ModuleType):
            _arm_module_dict(vars(module), enforce_fn)
            return

        if package is None:
            frame = inspect.currentframe()
            if frame is None or frame.f_back is None:
                return
            caller_globals = frame.f_back.f_globals
            package = caller_globals.get("__package__") or caller_globals.get("__name__", "")

        if not package or package == "__main__":
            warnings.warn(
                f"{backend_name}.arm() called from __main__. "
                f"Use @{backend_name}.enforce on individual functions instead.",
                stacklevel=2,
            )
            return

        armed: set[str] = module_globals["_ARMED"]
        if package in armed:
            return
        armed.add(package)

        pkg = importlib.import_module(package)
        _arm_module_dict(vars(pkg), enforce_fn)
        pkg_path = getattr(pkg, "__path__", None)
        if pkg_path is not None:
            for _, mod_name, _ in pkgutil.walk_packages(pkg_path, prefix=package + "."):
                try:
                    mod = importlib.import_module(mod_name)
                    _arm_module_dict(vars(mod), enforce_fn)
                except Exception as exc:
                    warnings.warn(
                        f"dfguard: skipped arming module '{mod_name}': {exc}",
                        stacklevel=2,
                    )

    return arm
