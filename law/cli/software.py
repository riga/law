# coding: utf-8

"""
"law sw" cli subprogram.
"""


import os
import sys
import shutil
import logging

import six
import luigi

import law
from law.config import Config


logger = logging.getLogger(__name__)


deps = [six, luigi, law]


if "_reloaded_deps" not in globals():
    _reloaded_deps = False


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *software* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser("software", prog="law software", description="Create or update "
        "the law software cache ({}). This is only required for some sandboxes that need to "
        "forward software into containers.".format(get_sw_dir()))

    parser.add_argument("--remove", "-r", action="store_true", help="remove the software cache "
        "directory and exit")
    parser.add_argument("--location", "-l", action="store_true", help="print the location of the "
        "software cache directory and exit")


def execute(args):
    """
    Executes the *software* subprogram with parsed commandline *args*.
    """
    sw_dir = get_sw_dir()

    # just print the cache location?
    if args.location:
        print(sw_dir)
        return

    # just remove the current software cache?
    if args.remove:
        remove_software_cache(sw_dir)
        return

    # rebuild the software cache
    build_software_cache(sw_dir)


def build_software_cache(sw_dir=None):
    """
    Builds up the software cache directory at *sw_dir* by simply copying all required python
    modules. *sw_dir* is evaluated with :py:func:`get_sw_dir`.
    """
    # ensure the cache is empty
    sw_dir = get_sw_dir(sw_dir)
    remove_software_cache(sw_dir)
    os.makedirs(sw_dir)

    # reload dependencies to find the proper module paths
    reload_dependencies(force=True)

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


def remove_software_cache(sw_dir=None):
    """
    Removes the software cache directory at *sw_dir* which is evaluated with :py:func:`get_sw_dir`.
    """
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        shutil.rmtree(sw_dir)


def reload_dependencies(force=False):
    """
    Reloads all python modules that law depends on. Currently, this is just *luigi* and *six*.
    Unless *force* is *True*, multiple calls to this function will not have any effect.
    """
    global _reloaded_deps

    if _reloaded_deps and not force:
        return
    _reloaded_deps = True

    for mod in deps:
        six.moves.reload_module(mod)
        logger.debug("reloaded module '{}'".format(mod))


def use_software_cache(sw_dir=None, reload_deps=False):
    """
    Adjusts ``sys.path`` so that the cached software at *sw_dir* is used. *sw_dir* is evaluated with
    :py:func:`get_sw_dir`. When *reload_deps* is *True*, :py:func:`reload_dependencies` is invoked.
    """
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        sys.path.insert(1, sw_dir)

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
