# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

import datetime
import os
import sys

import tomllib

sys.path.insert(0, os.path.abspath(".."))

with open(os.path.join(os.path.dirname(__file__), "..", "pyproject.toml"), "rb") as _f:
    _meta = tomllib.load(_f)["project"]

project   = _meta["name"]
_default_authors = [{"name": "dfguard contributors"}]
author    = ", ".join(a["name"] for a in _meta.get("authors", _default_authors))
release   = _meta["version"]   # e.g. "0.1.0"
version   = ".".join(release.split(".")[:2])  # e.g. "0.1"
copyright = f"{datetime.date.today().year}, {author}"

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",          # Google/NumPy docstring style
    "sphinx.ext.viewcode",          # [source] links
    "sphinx.ext.intersphinx",       # cross-links to Python / PySpark docs
    "sphinx_autodoc_typehints",     # renders type hints from annotations
    "sphinx_design",                # tab-set / tab-item directives
]

templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']

language = 'en'

# -- Napoleon (docstring style) -----------------------------------------------
napoleon_google_docstring  = True
napoleon_numpy_docstring   = False
napoleon_include_init_with_doc = True

# -- autodoc ------------------------------------------------------------------
autodoc_member_order       = "bysource"
autodoc_typehints          = "description"   # show types in description, not signature
autodoc_typehints_format   = "short"

# -- intersphinx --------------------------------------------------------------
intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "pyspark": ("https://spark.apache.org/docs/latest/api/python", None),
}

# -- Options for HTML output -------------------------------------------------
html_theme = 'furo'           # modern, clean, popular (used by pip, black, attrs)

html_title = f"dfguard {release}"
