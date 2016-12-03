# -*- coding: utf-8 -*-


import sys
import os
import shlaw


sys.path.insert(0, os.path.abspath(".."))
import law


project = "law"
author = law.__author__
copyright = law.__copyright__
version = law.__version__
release = law.__version__


templates_path = ["_templates"]
html_static_path = ["_static"]
master_doc = "index"
source_suffix = ".rst"


exclude_patterns = []
pygments_style = "sphinx"
html_logo = "../logo.png"
html_theme = "alabaster"
html_sidebars = {"**": [
    "about.html",
    "localtoc.html",
    "searchbox.html"]
}
html_theme_options = {
    "github_user": "riga",
    "github_repo": "law",
    "travis_button": True
}

extensions = [
    "sphinx.ext.autodoc"
]
