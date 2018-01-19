# -*- coding: utf-8 -*-

"""
"law sw" command line tool.
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
    parser = sub_parsers.add_parser("software", prog="law software",
        description="law software caching tool")

    parser.add_argument("--remove", "-r", action="store_true",
        help="just remove the software cache directory")


def execute(args):
    sw_dir = get_sw_dir()

    # just remove the current software cache?
    if args.remove:
        remove_software_cache(sw_dir)
        return

    # rebuild the software cache
    build_software_cache(sw_dir)


def build_software_cache(sw_dir=None):
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
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        shutil.rmtree(sw_dir)


def reload_dependencies(force=False):
    global _reloaded_deps

    if _reloaded_deps and not force:
        return
    _reloaded_deps = True

    for mod in deps:
        six.moves.reload_module(mod)
        logger.debug("reloaded module '{}'".format(mod))


def use_software_cache(sw_dir=None, reload_deps=False):
    sw_dir = get_sw_dir(sw_dir)
    if os.path.exists(sw_dir):
        sys.path.insert(1, sw_dir)

    if reload_deps:
        reload_dependencies()


def get_sw_dir(sw_dir=None):
    if sw_dir is None:
        sw_dir = Config.instance().get("core", "software_dir")

    sw_dir = os.path.expandvars(os.path.expanduser(sw_dir))

    return sw_dir
