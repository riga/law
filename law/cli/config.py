# -*- coding: utf-8 -*-

"""
"law config" cli subprogram.
"""


from law.config import Config
from law.util import abort


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *config* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser("config", prog="law config",
        description="law config file lookup")

    parser.add_argument("name", help="the name of the config to get, set or remove")
    parser.add_argument("value", nargs="?", help="the value to set")
    parser.add_argument("--remove", "-r", action="store_true")


def execute(args):
    """
    Executes the *config* subprogram with parsed commandline *args*.
    """
    if args.value is None and not args.remove:
        # just print the value
        print(get_config(args.name))
    else:
        abort("config setting and removal not yet implemented")


def get_config(name):
    """
    Returns the config value that corresponds to *name*, which must have the format
    ``section.option``.
    """
    section, option = name.split(".", 1)
    return Config.instance().get(section, option)
