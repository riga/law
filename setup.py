# coding: utf-8


import os
from setuptools import setup
from setuptools.command.install import install as _install


this_dir = os.path.dirname(os.path.abspath(__file__))


# workaround to change the installed law executable
class install(_install):

    def run(self):
        # run the install command
        _install.run(self)

        # get the path of the executable
        law_exec = os.path.join(self.install_scripts, "law")

        def write(content):
            try:
                with open(law_exec, "w") as f:
                    f.write(content)
                return True
            except Exception as e:
                print("could not update the law executable: {}".format(e))
                return False

        # when LAW_INSTALL_CUSTOM_SCRIPT is "1", replace the executable with law/cli/law
        if os.getenv("LAW_INSTALL_CUSTOM_SCRIPT", "0") == "1":
            with open(os.path.join(this_dir, "law", "cli", "law"), "r") as f:
                content = f.read()
            if not write(content):
                return

        # when LAW_INSTALL_CUSTOM_SHEBANG is set, replace the shebang in the executable
        shebang = os.getenv("LAW_INSTALL_CUSTOM_SHEBANG")
        if shebang:
            with open(law_exec, "r") as f:
                lines = f.readlines()
            if lines[0].startswith("#!"):
                lines.pop(0)
            if not shebang.startswith("#!"):
                shebang = "#!" + shebang
            content = "".join([shebang + "\n"] + lines)
            if not write(content):
                return


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
    "License :: OSI Approved :: MIT License",
    "Intended Audience :: Developers",
    "Intended Audience :: Science/Research",
    "Intended Audience :: Information Technology",
    "Topic :: System :: Monitoring",
]


# read the readme file
with open(os.path.join(this_dir, "README.rst"), "r") as f:
    long_description = f.read()


# load installation requirements
with open(os.path.join(this_dir, "requirements.txt"), "r") as f:
    install_requires = [line.strip() for line in f.readlines() if line.strip()]


# load package infos
pkg = {}
with open(os.path.join(this_dir, "law", "__version__.py"), "r") as f:
    exec(f.read(), pkg)


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
    python_requires=">=2.7",
    zip_safe=False,
    packages=[
        "law",
        "law.task",
        "law.target",
        "law.sandbox",
        "law.workflow",
        "law.job",
        "law.cli",
        "law.contrib",
        "law.contrib.arc",
        "law.contrib.cms",
        "law.contrib.dropbox",
        "law.contrib.git",
        "law.contrib.glite",
        "law.contrib.hdf5",
        "law.contrib.htcondor",
        "law.contrib.keras",
        "law.contrib.lsf",
        "law.contrib.matplotlib",
        "law.contrib.mercurial",
        "law.contrib.numpy",
        "law.contrib.root",
        "law.contrib.slack",
        "law.contrib.tasks",
        "law.contrib.telegram",
        "law.contrib.tensorflow",
        "law.contrib.wlcg",
    ],
    package_data={
        "": ["LICENSE", "requirements.txt", "README.rst"],
        "law": ["polyfills.sh"],
        "law.job": ["job.sh", "bash_wrapper.sh"],
        "law.cli": ["law", "completion.sh"],
        "law.contrib.cms": ["bundle_cmssw.sh", "cmsdashb_hooks.sh", "bin/apmon"],
        "law.contrib.git": ["bundle_repository.sh", "repository_checksum.sh"],
        "law.contrib.mercurial": ["bundle_repository.sh", "repository_checksum.sh"],
    },
    cmdclass={"install": install},
    entry_points={"console_scripts": ["law = law.cli:run"]},
)
