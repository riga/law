# coding: utf-8


import os
import re
from setuptools import setup, find_packages


this_dir = os.path.dirname(os.path.abspath(__file__))


# package keyworkds
keywords = [
    "luigi", "workflow", "pipeline", "remote", "gfal", "submission", "cluster", "grid", "condor",
    "lsf", "glite", "arc", "sandboxing", "docker", "singularity",
]


# package classifiers
classifiers = [
    "Programming Language :: Python",
    "Programming Language :: Python :: 2",
    "Programming Language :: Python :: 3",
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "License :: OSI Approved :: BSD License",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
    "Topic :: System :: Monitoring",
]


# read the readme file
with open(os.path.join(this_dir, "README.rst"), "r") as f:
    long_description = f.read()


# load installation requirements
readlines = lambda f: [line.strip() for line in f.readlines() if line.strip()]
with open(os.path.join(this_dir, "requirements.txt"), "r") as f:
    install_requires = readlines(f)


# load docs requirements
with open(os.path.join(this_dir, "requirements_docs.txt"), "r") as f:
    docs_requires = [line for line in readlines(f) if line not in install_requires]


# load package infos
pkg = {}
with open(os.path.join(this_dir, "law", "__version__.py"), "r") as f:
    exec(f.read(), pkg)


# install options
options = {}

# check for a custom executable for entry points
# note: when installing with pip, changing the executable in the shebang is only effective when
# running "pip install" with (e.g.) "--no-binary law" or "--no-binary :all:"
executable = os.getenv("LAW_INSTALL_EXECUTABLE", "")
if executable == "env":
    executable = "/usr/bin/env python"
elif re.match(r"^python(|\d|\d\.\d|\d\.\d\.\d)$", executable):
    executable = "/usr/bin/env " + executable
if executable:
    options["build_scripts"] = {"executable": executable}


setup(
    name="law",
    version=pkg["__version__"],
    author=pkg["__author__"],
    author_email=pkg["__email__"],
    description=pkg["__doc__"].strip().split("\n")[0].strip(),
    license=pkg["__license__"],
    url=pkg["__contact__"],
    keywords=" ".join(keywords),
    classifiers=classifiers,
    long_description=long_description,
    install_requires=install_requires,
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4'",
    extras_require={
        "docs": docs_requires,
    },
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    scripts=["bin/law"],
    options=options,
)
