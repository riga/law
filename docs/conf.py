# coding: utf-8


import sys
import os
import subprocess


thisdir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(thisdir, "_extensions"))
sys.path.insert(0, os.path.dirname(thisdir))

import law


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
master_doc = "index"
source_suffix = ".rst"
exclude_patterns = []
pygments_style = "sphinx"
add_module_names = False

html_title = "{} v{}".format(project, version)
html_logo = "../logo.png"
html_theme = "sphinx_book_theme"
html_theme_options = {}
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
        "logo_only": True,
        "home_page_in_toc": True,
        "show_navbar_depth": 2,
        "repository_url": "https://github.com/riga/law",
        "use_repository_button": True,
        "use_issues_button": True,
        "use_edit_page_button": True,
    })

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.intersphinx",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosectionlabel",
    "autodocsumm",
    "myst_parser",
    "pydomain_patch",
]

autodoc_default_options = {
    "member-order": "bysource",
    "show-inheritance": True,
}

intersphinx_mapping = {"python": ("https://docs.python.org/3", None)}


# event handlers
def generate_dynamic_pages(app):
    script_path = os.path.join(thisdir, "_scripts", "generate_dynamic_pages.py")
    subprocess.check_output([script_path])


# setup the app
def setup(app):
    # connect events
    app.connect("builder-inited", generate_dynamic_pages)

    # set style sheets
    app.add_css_file("styles_common.css")
    if html_theme in ("sphinx_rtd_theme", "alabaster", "sphinx_book_theme"):
        app.add_css_file("styles_{}.css".format(html_theme))
