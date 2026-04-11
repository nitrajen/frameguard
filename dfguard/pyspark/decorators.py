"""Optional decorators for schema validation at function boundaries."""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from dfguard.pyspark.schema import SparkSchema

F = TypeVar("F", bound=Callable[..., Any])


def typed_transform(
    input_schema: type[SparkSchema] | None = None,
    output_schema: type[SparkSchema] | None = None,
    strict: bool = False,
) -> Callable[[F], F]:
    """
    Decorator that validates schema at function boundaries.

    Parameters
    ----------
    input_schema:
        SparkSchema to validate all DataFrame arguments against.
    output_schema:
        SparkSchema to validate the return value against.
    strict:
        When True, extra columns beyond the declared schema are errors.
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            from pyspark.sql import DataFrame as SparkDF

            from dfguard.pyspark.dataset import _make_dataset, _TypedDatasetBase

            # _is_instance: True only for real _TypedDatasetBase objects (not plain SparkDFs)
            def _is_instance(x: Any) -> bool:
                return type.__instancecheck__(_TypedDatasetBase, x)

            # Normalise arguments: wrap raw DataFrames in _TypedDatasetBase instances
            new_args = []
            for arg in args:
                if isinstance(arg, SparkDF) and not _is_instance(arg):
                    arg = _make_dataset(arg, strict=strict)
                if _is_instance(arg) and input_schema is not None:
                    arg.validate(input_schema, strict=strict)
                new_args.append(arg)

            new_kwargs = {}
            for key, val in kwargs.items():
                if isinstance(val, SparkDF) and not _is_instance(val):
                    val = _make_dataset(val, strict=strict)
                if _is_instance(val) and input_schema is not None:
                    val.validate(input_schema, strict=strict)
                new_kwargs[key] = val

            result = func(*new_args, **new_kwargs)

            # Validate and normalise output
            if isinstance(result, SparkDF) and not _is_instance(result):
                result = _make_dataset(result, strict=strict)

            if _is_instance(result) and output_schema is not None:
                result.validate(output_schema, strict=strict)

            return result

        return wrapper  # type: ignore[return-value]

    return decorator


def check_schema(
    schema: type[SparkSchema],
    strict: bool = False,
) -> Callable[[F], F]:
    """
    Lightweight decorator that only validates the return value.

    Useful when the input does not need validation but you want to
    guarantee the output shape.
    """
    return typed_transform(output_schema=schema, strict=strict)
