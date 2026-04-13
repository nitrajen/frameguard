"""Column name sanitization: convert arbitrary column names to Python identifiers."""
from __future__ import annotations

import keyword
import re


def sanitize(col: str) -> str:
    """Convert a DataFrame column name to a valid Python identifier.

    Transformations applied in order:

    1. camelCase to snake_case (handles ``HTMLParser`` style runs of capitals)
    2. Non-identifier characters (spaces, hyphens, dots, etc.) to underscores
    3. Collapse consecutive underscores to one
    4. Strip leading and trailing underscores
    5. Prepend ``_`` when the first character is a digit
    6. Append ``_`` when the result is a Python keyword

    An input that produces an empty string becomes ``"col"``.
    """
    # camelCase → snake_case
    s = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', col)
    s = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s)
    s = s.lower()
    # Non-identifier chars → underscore
    s = re.sub(r'[^a-z0-9_]', '_', s)
    # Collapse multiple underscores
    s = re.sub(r'_+', '_', s)
    # Strip outer underscores (fall back to "col" if nothing remains)
    s = s.strip('_') or 'col'
    # Leading digit
    if s[0].isdigit():
        s = '_' + s
    # Python keyword
    if keyword.iskeyword(s):
        s = s + '_'
    return s


def sanitize_all(cols: list[str]) -> dict[str, str]:
    """Convert a list of column names to valid, deduplicated Python identifiers.

    Returns ``{python_name: original_col_name}``.  When two columns sanitize to
    the same identifier the second and subsequent ones get ``_2``, ``_3``, etc.
    appended.
    """
    result: dict[str, str] = {}
    seen: dict[str, int] = {}

    for col in cols:
        py = sanitize(col)
        count = seen.get(py, 0) + 1
        seen[py] = count
        deduped = py if count == 1 else f"{py}_{count}"
        result[deduped] = col

    return result
