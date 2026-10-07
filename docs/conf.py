from __future__ import annotations

import sys
import os
import subprocess

docsdir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(docsdir, "_extensions"))
sys.path.insert(0, os.path.normpath(os.path.join(docsdir, "..", "src")))

import law
from law._types import Any


# load all contrib packages
law.contrib.load_all()


project = law.__name__
author = law.__author__
copyright = law.__copyright__
copyright = copyright[10:] if copyright.startswith("Copyright ") else copyright
version = law.__version__[:law.__version__.index(".", 2)]
release = law.__version__
language = "en"

templates_path = ["_templates"]
html_static_path = ["_static"]
root_doc = "index"
source_suffix = {".rst": "restructuredtext", ".md": "markdown"}
exclude_patterns: list[str] = []
pygments_style = "sphinx"
add_module_names = False

html_title = f"{project} v{version}"
html_logo = "../assets/logo.png"
html_favicon = "../assets/favicon.ico"
html_theme = "sphinx_book_theme"
html_theme_options: dict[str, Any] = {}
if html_theme == "sphinx_rtd_theme":
    html_theme_options.update({
        "logo_only": True,
        "prev_next_buttons_location": None,
        "collapse_navigation": False,
    })
elif html_theme == "alabaster":
    html_theme_options.update({
        "github_user": "riga",
        "github_repo": "law",
        "travis_button": True,
    })
elif html_theme == "sphinx_book_theme":
    copyright = copyright.split(",", 1)[0]
    html_theme_options.update({
        "home_page_in_toc": True,
        "show_navbar_depth": 2,
        "show_toc_level": 3,
        "repository_url": "https://github.com/riga/law",
        "use_repository_button": True,
        "use_issues_button": True,
        "use_edit_page_button": True,
    })

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "autodocsumm",
    "myst_parser",
    "sphinx_lfs_content",
    "pydomain_patch",
    "pyref_resolver",
    "ansi_output",
]

# the readme is written for github and included into the index page, starting at level-2 headings
suppress_warnings = ["myst.header"]

autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}

intersphinx_mapping = {
    "python": ("https://docs.python.org/3", None),
    # docstrings inherited from luigi refer to labels in the luigi docs
    "luigi": ("https://luigi.readthedocs.io/en/stable", None),
    "numpy": ("https://numpy.org/doc/stable", None),
}

# references that cannot be resolved in nitpicky mode, mostly internal or private types in signatures
nitpick_ignore = [
    ("py:class", "T"),
    ("py:class", "law._types.T"),
    ("py:class", "argparse._SubParsersAction"),
    ("py:class", "gfal2.Gfal2Context"),
    ("py:class", "gfal2.TransferParameters"),
    ("py:exc", "GFALError_unlink"),
    ("py:exc", "GFALError_rmdir"),
    # inherited from collections.abc.Mapping, which is not documented for ConfigParser
    ("py:meth", "Config.keys"),
]
nitpick_ignore_regex = [
    # internal base classes and helpers that are not part of the public api
    ("py:class", r"law\.target\.luigi_shims\..*"),
    ("py:class", r"law\.(contrib\.)?\w+\.workflow\.\w+WorkflowProxy"),
    ("py:class", r"law\.task\.proxy\.ProxyAttributeTask"),
    ("py:class", r"law\.target\.collection\.SiblingFileCollectionBase"),
    ("py:class", r"law\.target\.remote\.base\.RemoteProxyBase"),
    ("py:class", r"law\.workflow\.base\.WorkflowRegister"),
    ("py:class", r"law\.util\.ClassPropertyDescriptor"),
    ("py:class", r"law\.sandbox\.base\.SandboxVariables"),
    # fragments of complex type annotations that are split by autodoc
    ("py:class", r"dict\[str"),
]


# event handlers
def generate_dynamic_pages(app):
    script_path = os.path.join(docsdir, "_scripts", "generate_dynamic_pages.py")
    subprocess.check_output([script_path])


def generate_cli_outputs(app):
    script_path = os.path.join(docsdir, "_scripts", "generate_cli_outputs.py")
    subprocess.check_output([sys.executable, script_path])


# setup the app
def setup(app):
    # connect events
    app.connect("builder-inited", generate_dynamic_pages)
    app.connect("builder-inited", generate_cli_outputs)

    # set style sheets
    app.add_css_file("styles_common.css")
    if html_theme in ("sphinx_rtd_theme", "alabaster", "sphinx_book_theme"):
        app.add_css_file(f"styles_{html_theme}.css")
