# coding: utf-8

"""
"law sw" cli subprogram.
"""


import os
import sys
import shutil
from importlib import import_module

import six

from law.config import Config
from law.logger import get_logger


logger = get_logger(__name__)


# TODO: auto-detect
default_dep_names = ["six", "luigi", "law"]
if six.PY3:
    default_dep_names += ["tenacity", "dateutil"]

dep_names = os.getenv("LAW_SOFTWARE_DEPS", None)
if dep_names:
    dep_names = [name.strip() for name in dep_names.strip().split(",")]
else:
    dep_names = default_dep_names

_deps = None


if "_reloaded_deps" not in globals():
    _reloaded_deps = False


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *software* subprogram and adds it to *sub_parsers*.
    """
    csv = lambda s: [_s.strip() for _s in str(s).strip().split(",")]

    parser = sub_parsers.add_parser(
        "software",
        prog="law software",
        description="Create or update the law software cache ({}). This is only required for some "
        "sandboxes that need to forward software into (e.g.) containers.".format(get_sw_dir()),
    )

    parser.add_argument(
        "--deps",
        "-d",
        type=csv,
        default=dep_names,
        help="comma-separated names of python packages to cache; "
        "default: {}".format(",".join(dep_names)),
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


def execute(args):
    """
    Executes the *software* subprogram with parsed commandline *args*.
    """
    sw_dir = get_sw_dir()

    # just print the cache location?
    if args.location:
        print(sw_dir)
        return

    # just print the list of dependencies?
    if args.print_deps:
        print(",".join(args.deps))
        return

    # just remove the current software cache?
    if args.remove:
        remove_software_cache(sw_dir)
        return

    # rebuild the software cache
    build_software_cache(sw_dir, dep_names=args.deps)


def get_software_deps(names=None):
    global _deps

    if _deps is not None:
        return _deps
    _deps = []

    if names is None:
        names = list(dep_names)

    for name in names:
        try:
            mod = import_module(name)
        except ImportError as e:
            print("could not import software dependency '{}': {}".format(name, e))
            continue

        _deps.append(mod)

    return _deps


def build_software_cache(sw_dir=None, dep_names=None):
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
        path = os.path.dirname(mod.__file__)
        name, ext = os.path.splitext(os.path.basename(mod.__file__))
        # single file or module?
        if name == "__init__":
            # copy the entire module
            name = os.path.basename(path)
            shutil.copytree(path, os.path.join(sw_dir, name))
        else:
            shutil.copy2(os.path.join(path, name + ".py"), sw_dir)
        logger.debug("cached '{}'".format(mod))


def remove_software_cache(sw_dir=None):
    """
    Removes the software cache directory at *sw_dir* which is evaluated with :py:func:`get_sw_dir`.
    """
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        shutil.rmtree(sw_dir)


def reload_dependencies(force=False, dep_names=None):
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
        six.moves.reload_module(mod)
        logger.debug("reloaded '{}'".format(mod))


def use_software_cache(sw_dir=None, reload_deps=False):
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


def get_sw_dir(sw_dir=None):
    """
    Returns the software directory defined in the ``core.software_dir`` config. When *sw_dir* is not
    *None*, it is expanded and returned instead.
    """
    if sw_dir is None:
        cfg = Config.instance()
        sw_dir = cfg.get_expanded("core", "software_dir")

    sw_dir = os.path.expandvars(os.path.expanduser(sw_dir))

    return sw_dir
