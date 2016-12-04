# -*- coding: utf-8 -*-


import os
from subprocess import Popen, PIPE
from setuptools import setup

import law


thisdir = os.path.dirname(os.path.abspath(__file__))

readme = os.path.join(thisdir, "README.md")
if os.path.isfile(readme):
    cmd = "pandoc --from=markdown --to=rst " + readme
    p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
    out, err = p.communicate()
    if p.returncode != 0:
        raise Exception("pandoc conversion failed: " + err)
    long_description = out
else:
    long_description = ""

keywords = [
    "luigi", "workflow", "pipeline", "remote", "submission", "grid"
]

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
    "Topic :: System :: Monitoring"
]

install_requires = []
with open(os.path.join(thisdir, "requirements.txt"), "r") as f:
    install_requires.extend(line.strip() for line in f.readlines() if line.strip())

setup(
    name             = law.__name__,
    version          = law.__version__,
    author           = law.__author__,
    author_email     = law.__email__,
    description      = law.__doc__.strip(),
    license          = law.__license__,
    url              = law.__contact__,
    keywords         = keywords,
    classifiers      = classifiers,
    long_description = long_description,
    install_requires = install_requires,
    zip_safe         = False,
    packages         = ["law", "law.task", "law.target", "law.compat", "law.scripts",
                        "law.examples"],
    data_files       = ["LICENSE", "requirements.txt", "README.md", "completion.sh"],
    entry_points     = {
        "console_scripts": [
            "law = law.scripts._law:main",
            "law_db = law.scripts._law_db:main",
            "law_completion = law.scripts._law_completion:main"
        ]
    }
)
