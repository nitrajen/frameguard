"""Schema enforcement for the pandas backend."""

from __future__ import annotations

from typing import Any

from dfguard._base.enforcement import make_arm, make_enforce

_ENABLED = True
_ARMED: set[str] = set()
_SUBSET  = True


def _raise_schema_mismatch(
    func_name: str,
    param_name: str,
    annotation: type,
    value: Any,
) -> None:
    import pandas as pd

    from dfguard.pandas.dataset import _PandasDatasetBase

    if isinstance(value, pd.DataFrame):
        actual_str = ", ".join(f"{c}:{d}" for c, d in value.dtypes.items())
    elif type.__instancecheck__(_PandasDatasetBase, value):
        actual_str = ", ".join(f"{c}:{d}" for c, d in value.dtypes.items())
    else:
        actual_str = type(value).__name__

    expected_schema: dict[str, Any] | None = annotation.__dict__.get("_expected_schema")
    if expected_schema is not None:
        expected_str = ", ".join(f"{c}:{d}" for c, d in expected_schema.items())
    else:
        expected_str = getattr(annotation, "__name__", repr(annotation))

    raise TypeError(
        f"Schema mismatch in {func_name}() argument '{param_name}':\n"
        f"  expected: {expected_str}\n"
        f"  received: {actual_str}"
    )


enforce = make_enforce(
    get_enabled    = lambda: _ENABLED,
    get_subset     = lambda: _SUBSET,
    raise_mismatch = _raise_schema_mismatch,
)

arm = make_arm(
    backend_name   = "dfguard.pandas",
    module_globals = globals(),
    get_enforce_fn = lambda: enforce,
)


def disarm() -> None:
    """Turn off all enforcement globally. Call arm() to re-enable."""
    global _ENABLED
    _ENABLED = False
