# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import commonmark
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join("..", "src")))

from impira import version

# https://stackoverflow.com/questions/56062402/force-sphinx-to-interpret-markdown-in-python-docstrings-instead-of-restructuredt
def docstring(app, what, name, obj, options, lines):
    md = "\n".join(lines)
    ast = commonmark.Parser().parse(md)
    rst = commonmark.ReStructuredTextRenderer().render(ast)
    lines.clear()
    lines += rst.splitlines()


def setup(app):
    app.connect("autodoc-process-docstring", docstring)


# -- Project information -----------------------------------------------------

project = "Impira SDK"
copyright = "2021, Impira Inc."
author = "Impira Engineering"


version = version.VERSION

# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    "myst_parser",
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.autosummary",
    "enum_tools.autoenum",
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ["_templates"]

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
html_theme = "alabaster"

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = []  # ["_static"]

# -- MyST configuration ------------------------------------------------------
myst_enable_extensions = ["colon_fence", "substitution"]
myst_substitutions = {"version": version}
myst_heading_anchors = 2

# -- Autodoc configuration ---------------------------------------------------
autodoc_member_order = "bysource"
