# coding: utf-8


import os
from setuptools import setup, find_packages
from setuptools.command.install import install as _install


this_dir = os.path.dirname(os.path.abspath(__file__))


# workaround to change the installed law executable
class install(_install):

    def run(self):
        # run the install command
        _install.run(self)

        # get the path of the executable
        law_exec = os.path.join(self.install_scripts, "law")

        # helper to read the lines of the law executable
        def read_lines(path=law_exec):
            try:
                with open(path, "r") as f:
                    return [line.strip() for line in f.readlines()]
            except Exception as e:
                print("could not read the law executable: {}".format(e))
                return None

        # helper to write the content of the law executable
        def write_lines(lines):
            try:
                with open(law_exec, "w") as f:
                    for line in lines:
                        f.write(line + "\n")
                return True
            except Exception as e:
                print("could not update the law executable: {}".format(e))
                return False

        # replace the executable with law/cli/law when LAW_INSTALL_CUSTOM_SCRIPT is true
        if os.getenv("LAW_INSTALL_CUSTOM_SCRIPT", "").lower() in ("1", "true", "yes"):
            custom_exec = os.path.join(this_dir, "law", "cli", "law")
            lines = read_lines(path=custom_exec)
            if not lines or not write_lines(lines):
                return

        # when LAW_INSTALL_CUSTOM_SHEBANG is set, replace the shebang in the executable
        shebang = os.getenv("LAW_INSTALL_CUSTOM_SHEBANG")
        if shebang:
            # ensure that the shebang starts with #!
            if not shebang.startswith("#!"):
                shebang = "#!" + shebang

            # read current content
            lines = read_lines()
            if not lines:
                return

            # replace the shebang
            for i, line in enumerate(list(lines)):
                if line.startswith("#!"):
                    lines[i] = shebang
                    break
            else:
                print("could not find shebang in law executable to replace")
                return

            # write new content
            if not write_lines(lines):
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
    python_requires=">=2.7, !=3.0.*, !=3.1.*, !=3.2.*, <4'",
    zip_safe=False,
    packages=find_packages(exclude=["tests"]),
    include_package_data=True,
    cmdclass={"install": install},
    entry_points={"console_scripts": ["law = law.cli:run"]},
)
