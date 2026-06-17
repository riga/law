# coding: utf-8

"""
"law sw" cli subprogram.
"""

from __future__ import annotations

import os
import sys
import shutil
import pathlib
import argparse
import importlib

from law.config import Config
from law.logger import get_logger
from law._types import ModuleType


logger = get_logger(__name__)

# TODO: auto-detect
default_dep_names = ["luigi", "law", "tenacity", "dateutil"]

dep_names_str = os.getenv("LAW_SOFTWARE_DEPS", None)
if dep_names_str:
    dep_names = [name.strip() for name in dep_names_str.strip().split(",")]
else:
    dep_names = default_dep_names

_deps: list[ModuleType] | None = None

if "_reloaded_deps" not in globals():
    _reloaded_deps = False


def setup_parser(sub_parsers: argparse._SubParsersAction) -> None:
    """
    Sets up the command line parser for the *software* subprogram and adds it to *sub_parsers*.
    """
    csv = lambda s: [_s.strip() for _s in str(s).strip().split(",")]

    parser = sub_parsers.add_parser(
        "software",
        prog="law software",
        description=f"Create or update the law software cache ({get_sw_dir()}). This is only "
        "required for some sandboxes that need to forward software into (e.g.) containers.",
    )

    parser.add_argument(
        "--deps",
        "-d",
        type=csv,
        default=dep_names,
        help=f"comma-separated names of python packages to cache; default: {','.join(dep_names)}",
    )
    parser.add_argument(
        "--remove",
        "-r",
        action="store_true",
        help="remove the software cache directory and exit",
    )
    parser.add_argument(
        "--location",
        "-l",
        action="store_true",
        help="print the location of the software cache directory and exit",
    )
    parser.add_argument(
        "--print-deps",
        "-p",
        action="store_true",
        help="print the list of dependencies and exit",
    )


def execute(args: argparse.Namespace) -> int:
    """
    Executes the *software* subprogram with parsed commandline *args*.
    """
    sw_dir = get_sw_dir()

    # just print the cache location?
    if args.location:
        print(sw_dir)
        return 0

    # just print the list of dependencies?
    if args.print_deps:
        print(",".join(args.deps))
        return 0

    # just remove the current software cache?
    if args.remove:
        remove_software_cache(sw_dir)
        return 0

    # rebuild the software cache
    build_software_cache(sw_dir, dep_names=args.deps)

    return 0


def get_software_deps(names: list[str] | None = None) -> list[ModuleType]:
    global _deps

    if _deps is not None:
        return _deps
    _deps = []

    if names is None:
        names = list(dep_names)

    for name in names:
        try:
            mod = importlib.import_module(name)
        except ImportError as e:
            print(f"could not import software dependency '{name}': {e}")
            continue

        _deps.append(mod)

    return _deps


def build_software_cache(
    sw_dir: str | pathlib.Path | None = None,
    dep_names: list[str] | None = None,
) -> None:
    """
    Builds up the software cache directory at *sw_dir* by simply copying all required python
    modules identified by *dep_names*, defaulting to a predefined list of package names. *sw_dir*
    is evaluated with :py:func:`get_sw_dir`.
    """
    # ensure the cache is empty
    sw_dir = get_sw_dir(sw_dir)
    remove_software_cache(sw_dir)
    os.makedirs(sw_dir)

    # reload dependencies to find the proper module paths
    reload_dependencies(force=True)

    # get dependencies
    deps = get_software_deps(names=dep_names)

    for mod in deps:
        mod_file = mod.__file__
        if not mod_file:
            continue
        path = os.path.dirname(mod_file)
        name, ext = os.path.splitext(os.path.basename(mod_file))
        # single file or module?
        if name == "__init__":
            # copy the entire module
            name = os.path.basename(path)
            shutil.copytree(path, os.path.join(sw_dir, name))
        else:
            shutil.copy2(os.path.join(path, f"{name}.py"), sw_dir)
        logger.debug(f"cached '{mod}'")


def remove_software_cache(sw_dir: str | pathlib.Path | None = None) -> None:
    """
    Removes the software cache directory at *sw_dir* which is evaluated with :py:func:`get_sw_dir`.
    """
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        shutil.rmtree(sw_dir)


def reload_dependencies(force: bool = False, dep_names: list[str] | None = None) -> None:
    """
    Reloads all python modules that law depends on, idenfied by *dep_names* and defaulting to a
    predefined list of package names. Unless *force* is *True*, multiple calls to this function will
    not have any effect.
    """
    global _reloaded_deps

    # reload only once
    if _reloaded_deps and not force:
        return
    _reloaded_deps = True

    # get dependencies
    deps = get_software_deps(names=dep_names)

    # reload them
    for mod in deps:
        importlib.reload(mod)
        logger.debug(f"reloaded '{mod}'")


def use_software_cache(sw_dir: str | pathlib.Path | None = None, reload_deps: bool = False) -> None:
    """
    Adjusts ``sys.path`` so that the cached software at *sw_dir* is used. *sw_dir* is evaluated with
    :py:func:`get_sw_dir`. When *reload_deps* is *True*, :py:func:`reload_dependencies` is invoked.
    """
    sw_dir = get_sw_dir(sw_dir)
    if not os.path.exists(sw_dir):
        return

    # ammend the search path to favor the software cache
    sys.path.insert(1, sw_dir)

    # reload dependencies in case they were already loaded
    if reload_deps:
        reload_dependencies()


def get_sw_dir(sw_dir: str | pathlib.Path | None = None) -> str:
    """
    Returns the software directory defined in the ``core.software_dir`` config. When *sw_dir* is not
    *None*, it is expanded and returned instead.
    """
    if sw_dir is None:
        cfg = Config.instance()
        return cfg.get_expanded("core", "software_dir")

    return os.path.expandvars(os.path.expanduser(str(sw_dir)))
