# coding: utf-8

"""
"law config" cli subprogram.
"""

import argparse

from law.config import Config
from law.util import abort


_cfg = Config.instance()


def setup_parser(sub_parsers: argparse._SubParsersAction) -> None:
    """
    Sets up the command line parser for the *config* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser(
        "config",
        prog="law config",
        description="Configuration helper to get, set or remove a value from the law configuration "
        f"file ({_cfg.config_file}).",
    )

    parser.add_argument(
        "name",
        nargs="?",
        help="the name of the config in the format <section>[.<option>]; when empty, all section "
        "names are printed; when no option is given, all section options are printed",
    )
    parser.add_argument(
        "value",
        nargs="?",
        help="the value to set; when empty, the current value is printed",
    )
    parser.add_argument(
        "--remove",
        "-r",
        action="store_true",
        help="remove the config",
    )
    parser.add_argument(
        "--expand",
        "-e",
        action="store_true",
        help="expand variables when getting a value",
    )
    parser.add_argument(
        "--location",
        "-l",
        action="store_true",
        help="print the location of the configuration file and exit",
    )


def execute(args: argparse.Namespace) -> int:
    """
    Executes the *config* subprogram with parsed commandline *args*.
    """
    cfg = Config.instance()

    # just print the file location?
    if args.location:
        print(cfg.config_file)
        return 0

    # print sections when none is given
    if not args.name:
        print("\n".join(cfg.sections()))
        return 0

    # print section options when none is given
    section, option = args.name.split(".", 1) if "." in args.name else (args.name, None)
    if not option:
        print("\n".join(cfg.options(section)))
        return 0

    # removal
    if args.remove:
        return abort("config removal not yet implemented")

    # setting
    if args.value:
        return abort("config setting not yet implemented")

    # getting
    print(cfg.get_default(section, option, expand_vars=args.expand, expand_user=args.expand))

    return 0
