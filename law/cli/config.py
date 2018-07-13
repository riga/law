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
    parser = sub_parsers.add_parser("config", prog="law config", description="Configuration helper"
        " to get, set or remove a value from the law configuration file ({}).".format(
            Config.instance().config_file))

    parser.add_argument("name", nargs="?", help="the name of the config in the format"
        " <section>.<option>")
    parser.add_argument("value", nargs="?", help="the value to set")
    parser.add_argument("--remove", "-r", action="store_true", help="remove the config")
    parser.add_argument("--expand", "-e", action="store_true", help="expand variables when getting"
        " a value")
    parser.add_argument("--location", "-l", action="store_true", help="print the location of the"
        " configuration file and exit")


def execute(args):
    """
    Executes the *config* subprogram with parsed commandline *args*.
    """
    # just print the file location?
    if args.location:
        print(Config.instance().config_file)
        return

    # every option below requires the name to be set
    if not args.name:
        abort("please give the name of the config in the format <section>.<option>")

    # removal
    if args.remove:
        abort("config removal not yet implemented")

    # setting
    if args.value:
        abort("config setting not yet implemented")

    # getting
    print(get_config(args.name, expand=args.expand))


def get_config(name, expand=False):
    """
    Returns the config value that corresponds to *name*, which must have the format
    ``section.option``.
    """
    cfg = Config.instance()
    func = cfg.get_expanded if expand else cfg.get

    section, option = name.split(".", 1)
    return func(section, option)
