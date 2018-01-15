# -*- coding: utf-8 -*-


import os
import sys
import warnings
from subprocess import Popen, PIPE
from setuptools import setup
from setuptools.command.install import install as _install

import law
from law.util import which


thisdir = os.path.dirname(os.path.abspath(__file__))

readme = os.path.join(thisdir, "README.md")
if os.path.isfile(readme) and "sdist" in sys.argv:
    cmd = "pandoc --from=markdown --to=rst " + readme
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        warnings.warn("pandoc conversion failed: " + err)
    long_description = out
else:
    long_description = ""

keywords = ["luigi", "workflow", "pipeline", "remote", "submission", "grid"]

classifiers = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 3",
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "License :: OSI Approved :: MIT License",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
    "Topic :: System :: Monitoring",
]

install_requires = []
with open(os.path.join(thisdir, "requirements.txt"), "r") as f:
    install_requires.extend(line.strip() for line in f.readlines() if line.strip())

# workaround to change the installed law script to _not_ use pkg_resources
class install(_install):
    def run(self):
        _install.run(self) # old-style

        with open(os.path.join(thisdir, "law", "scripts", "law")) as f:
            lines = f.readlines()
        with open(which("law"), "w") as f:
            f.write("".join(lines))

setup(
    name = law.__name__,
    version = law.__version__,
    author = law.__author__,
    author_email = law.__email__,
    description = law.__doc__.strip(),
    license = law.__license__,
    url = law.__contact__,
    keywords = " ".join(keywords),
    classifiers = classifiers,
    long_description = long_description,
    install_requires = install_requires,
    zip_safe = False,
    packages = [
        "law",
        "law.task",
        "law.target",
        "law.sandbox",
        "law.workflow",
        "law.job",
        "law.contrib.workflow",
        "law.contrib.workflow.glite",
        "law.contrib.job",
        "law.contrib.util",
        "law.scripts",
        "law.examples",
    ],
    package_data = {
        "": ["LICENSE", "requirements.txt"],
        "law": ["completion.sh"],
        "law.contrib.workflow.glite": ["*.sh"],
    },
    cmdclass = {"install": install},
    entry_points = {"console_scripts": ["law = law.scripts.cli:main"]},
)
