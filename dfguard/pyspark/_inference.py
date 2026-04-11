"""
dfguard.pyspark._inference
~~~~~~~~~~~~~~~~~~~~~~~~~~
``infer_schema(df)``: inspect a live DataFrame and generate a SparkSchema
subclass with the correct types, including deeply nested structs.

The generated class can immediately be used for validation::

    schema = infer_schema(df, name="OrderSchema")
    print(schema.to_code())    # copy-paste into your codebase
    ds.validate(schema)

Nested StructTypes are emitted as separate named inner classes.
"""

from __future__ import annotations

from typing import Any


def infer_schema(df: Any, name: str = "InferredSchema") -> type:
    """
    Inspect ``df`` (a pyspark.sql.DataFrame or dataset wrapper) and return
    a SparkSchema subclass that exactly matches its current schema.

    Also prints the Python code so developers can copy it into their codebase.

    Parameters
    ----------
    df:
        A live ``pyspark.sql.DataFrame`` or ``dataset`` wrapper.
    name:
        Name to give the generated class.

    Returns
    -------
    type[SparkSchema]
        A fully usable SparkSchema subclass.
    """
    from dfguard.pyspark.schema import SparkSchema

    struct = df.schema
    schema_class = SparkSchema.from_struct(struct, name=name)

    print(_render_code(schema_class, name))
    return schema_class


def _render_code(schema_class: Any, name: str) -> str:
    """Recursively render a SparkSchema (and any nested schemas) as Python source."""
    lines: list[str] = []
    nested_lines: list[str] = []

    # Collect any nested SparkSchema classes stored as class attributes
    from dfguard.pyspark.schema import SparkSchema
    for attr_name, attr_val in vars(schema_class).items():
        if (
            isinstance(attr_val, type)
            and issubclass(attr_val, SparkSchema)
            and attr_val is not SparkSchema
        ):
            nested_lines.append(_render_code(attr_val, attr_val.__name__))
            nested_lines.append("")

    if nested_lines:
        lines.extend(nested_lines)

    lines.append(f"class {name}(SparkSchema):")
    if not schema_class._schema_fields:
        lines.append("    pass")
    else:
        from dfguard.pyspark.schema import _annotation_to_str
        for col_name, annotation in schema_class._schema_fields.items():
            ann_str = _annotation_to_str(annotation)
            lines.append(f"    {col_name}: {ann_str}")

    return "\n".join(lines)
