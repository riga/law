# coding: utf-8


import os
import sys
import re
import glob
import json
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
    "Programming Language :: Python :: 2.7",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: 3.7",
    "Programming Language :: Python :: 3.8",
    "Programming Language :: Python :: 3.9",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
    "Programming Language :: Python :: 3.13",
    "Development Status :: 4 - Beta",
    "Operating System :: OS Independent",
    "License :: OSI Approved :: BSD License",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
    "Topic :: System :: Monitoring",
]


# helper to read non-empty, stripped lines from an opened file
def readlines(f):
    for line in f.readlines():
        if line.strip():
            yield line.strip()


# read the readme file
open_kwargs = {} if sys.version_info.major < 3 else {"encoding": "utf-8"}
with open(os.path.join(this_dir, "README.md"), "r", **open_kwargs) as f:
    long_description = f.read()


# load installation requirements
with open(os.path.join(this_dir, "requirements.txt"), "r") as f:
    install_requires = list(readlines(f))


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


# prepare console scripts from contrib packages
console_scripts = []
for contrib_init in glob.glob(os.path.join(this_dir, "law", "contrib", "*", "__init__.py")):
    # get the path of the scripts json file
    json_path = os.path.join(os.path.dirname(contrib_init), "console_scripts.json")
    if not os.path.exists(json_path):
        continue
    # read contents
    with open(json_path, "r") as f:
        _console_scripts = json.load(f)
    # store them
    for script_name, entry_point in _console_scripts.items():
        # adjust script name and entry point
        if not script_name.startswith("law_cms_"):
            script_name = "law_cms_" + script_name
        if not entry_point.startswith("law.contrib.cms."):
            entry_point = "law.contrib.cms." + entry_point
        # store
        console_scripts.append("{}={}".format(script_name, entry_point))


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
    long_description_content_type="text/markdown",
    install_requires=install_requires,
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, !=3.5.*, <4",
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    scripts=["bin/law", "bin/law2", "bin/law3"],
    entry_points={"console_scripts": console_scripts},
    options=options,
)
