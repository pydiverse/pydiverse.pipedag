# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html
import datetime
import importlib.metadata
import inspect
import os
import sys

import sphinx.util
from sphinx.ext.napoleon import GoogleDocstring


sys.path.insert(0, os.path.abspath("../.."))
sys.path.insert(0, os.path.abspath("../../src"))
os.environ["SPHINX_BUILD"] = "1"

_mod = importlib.import_module("pydiverse.pipedag")

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = "pydiverse.pipedag"
copyright = f"{datetime.date.today().year}, QuantCo, Inc"
author = "QuantCo, Inc."

release = importlib.metadata.version("pydiverse.pipedag")

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    "myst_parser",
    "sphinx.ext.linkcode",
    "sphinx.ext.autodoc",
    "sphinx.ext.autosummary",
    "sphinx.ext.autosectionlabel",
    "sphinx.ext.intersphinx",
    "sphinx_copybutton",
    "sphinx.ext.napoleon",
    "sphinx_click",
]

myst_enable_extensions = [
    "fieldlist",
    "deflist",
    "attrs_block",
    "attrs_inline",
]

autodoc_default_options = {
    "member-order": "bysource",
}

autosectionlabel_prefix_document = True

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    "sa": ("https://docs.sqlalchemy.org/en/stable", None),
    "pd": ("https://pandas.pydata.org/pandas-docs/stable", None),
    "pl": ("https://pola-rs.github.io/polars/py-polars/html", None),
    "tp": ("https://tidypolars.readthedocs.io/en/stable", None),
    "ibis": ("https://ibis-project.org/", None),
    "kazoo": ("https://kazoo.readthedocs.io/en/latest", None),
    "dask": ("https://docs.dask.org/en/stable", None),
}

napoleon_custom_sections = [
    "Config File",
]

templates_path = ["_templates"]
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = "furo"
# html_static_path = ["_static"]
html_title = "pydiverse.pipedag"

html_theme_options = {
    "footer_icons": [
        {
            "name": "GitHub",
            "url": "https://github.com/pydiverse/pydiverse.pipedag/",
            "html": """
                <svg stroke="currentColor" fill="currentColor" stroke-width="0" viewBox="0 0 16 16">
                    <path fill-rule="evenodd" d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0 0 16 8c0-4.42-3.58-8-8-8z"></path>
                </svg>
            """,
            "class": "",
        },
    ],
    "source_repository": "https://github.com/pydiverse/pydiverse.pipedag/",
    "source_branch": "main",
    "source_directory": "docs/",
}


# Fix signature
# Use __init__ instead of __new__
def _correct_signatures(app, what, name, obj, options, signature, return_annotation):
    if "__overload__" in str(signature):
        return str(signature).replace("__overload__", ""), return_annotation

    if what == "class":
        if obj.__init__ != object.__init__:
            signature = sphinx.util.inspect.signature(obj.__init__)

            parameters = signature.parameters.copy()
            parameters.popitem(last=False)  # Pop first to remove self

            signature = signature.replace(parameters=parameters.values())
            signature = str(signature)

    return signature, return_annotation


def setup(app):
    app.connect("autodoc-process-signature", _correct_signatures)
    pass


# Copied and adapted from
# https://github.com/pandas-dev/pandas/blob/4a14d064187367cacab3ff4652a12a0e45d0711b/doc/source/conf.py#L613-L659
# Required configuration function to use sphinx.ext.linkcode
def linkcode_resolve(domain, info):
    """Determine the URL corresponding to a given Python object."""
    try:
        if domain != "py":
            return None

        module_name = info["module"]
        full_name = info["fullname"]

        _submodule = sys.modules.get(module_name)
        if _submodule is None:
            return None

        _object = _submodule
        for _part in full_name.split("."):
            try:
                _object = getattr(_object, _part)
            except AttributeError:
                return None

        try:
            fn = inspect.getsourcefile(inspect.unwrap(_object))  # type: ignore
        except TypeError:
            fn = None
        if not fn:
            return None

        try:
            source, line_number = inspect.getsourcelines(_object)
        except OSError:
            line_number = None  # type: ignore

        if line_number:
            linespec = f"#L{line_number}-L{line_number + len(source) - 1}"
        else:
            linespec = ""

        fn = os.path.relpath(fn, start=os.path.dirname(str(_mod.__file__)))

        return (
            "https://github.com/pydiverse/pydiverse.pipedag"
            f"/tree/{release}/src/pydiverse/pipedag/{fn}{linespec}"
        )
    except Exception as e:
        return None


# Patch napoleon so that the type in the attributes section gets placed on the
# same line as the name of the attribute
def _parse_attributes_section(self, section: str) -> list[str]:
    lines = []
    for _name, _type, _desc in self._consume_fields():
        if not _type:
            _type = self._lookup_annotation(_name)
        if self._config.napoleon_use_ivar:
            field = ":ivar %s: " % _name
            lines.extend(self._format_block(field, _desc))
            if _type:
                lines.append(f":vartype {_name}: {_type}")
        else:
            lines.append(".. attribute:: " + _name)
            if _type:
                lines.extend(self._indent([":type: %s" % _type], 3))
            if self._opt and "noindex" in self._opt:
                lines.append("   :noindex:")
            lines.append("")

            fields = self._format_field("", "", _desc)
            lines.extend(self._indent(fields, 3))
            lines.append("")
    if self._config.napoleon_use_ivar:
        lines.append("")
    return lines


GoogleDocstring._parse_attributes_section = _parse_attributes_section
