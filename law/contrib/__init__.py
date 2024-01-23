# coding: utf-8

"""
Main entry point and dynamic loading of contrib packages.
"""

from __future__ import annotations

__all__ = ["available_packages", "loaded_packages", "load", "load_all"]

import os
import glob

import law
from law.util import law_src_path, flatten
from law.logger import get_logger
from law._types import ModuleType


logger = get_logger(__name__)

this_dir = os.path.dirname(os.path.abspath(__file__))


#: List of names of available contrib packages.
available_packages = [
    os.path.basename(os.path.dirname(contrib_init))
    for contrib_init in glob.glob(os.path.join(this_dir, "*", "__init__.py"))
]

#: Dictionary of names to modules of already loaded contrib packages.
loaded_packages = {}


def load(*packages: str) -> ModuleType | list[ModuleType]:
    """
    Loads contrib *packages* and adds them to the law namespace. Example:

    .. code-block:: python

        import law
        law.contrib.load("docker")

        law.docker.DockerSandbox(...)

    It is ensured that packages are loaded only once.
    """
    for pkg in flatten(packages):
        if pkg in loaded_packages:
            logger.debug(f"skip contrib package '{pkg}', already loaded")
            continue
        if not os.path.exists(law_src_path("contrib", pkg, "__init__.py")):
            raise Exception(f"contrib package '{pkg}' does not exist")
        if getattr(law, pkg, None):
            raise Exception(
                f"cannot load contrib package '{pkg}', attribute with that name already exists in "
                "the law module",
            )

        # load the module
        mod = __import__(f"law.contrib.{pkg}", globals(), locals(), [pkg])
        setattr(law, pkg, mod)
        law.__all__.append(pkg)
        loaded_packages[pkg] = mod
        logger.debug(f"loaded contrib package '{pkg}'")

        # optionally call its auto_load hook when existing
        auto_load = getattr(mod, "auto_load", None)
        if callable(auto_load):
            auto_load()
            logger.debug(f"invoked auto_load hook of contrib package '{pkg}'")

    modules = [loaded_packages[pkg] for pkg in packages]

    return modules[0] if len(packages) == 1 else modules


def load_all() -> list[str]:
    """
    Loads all available contrib packages via :py:func:`load`. A package is skipped when an
    ImportError was raised. The list of names of loaded packages is returned.
    """
    loaded_packages = []
    for name in available_packages:
        try:
            load(name)
        except ImportError:
            continue
        loaded_packages.append(name)
    return loaded_packages
