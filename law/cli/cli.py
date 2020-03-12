# coding: utf-8

"""
Law command line interface entry point.
"""


import sys
from importlib import import_module
from argparse import ArgumentParser

import law


progs = ["run", "index", "config", "software", "completion", "location"]

# temporary command to show renaming note until v0.1
progs.append("db")


def run(argv=None):
    """
    Entry point to the law cli. Sets up all parsers, parses all arguments given by *argv*, and
    executes the requested subprogram. When *None*, *argv* defaults to ``sys.argv[1:]``.
    """
    # setup the main parser and sub parsers
    parser = ArgumentParser(prog="law", description="The law command line tool.")
    sub_parsers = parser.add_subparsers(help="subcommands", dest="command")

    # add main arguments
    parser.add_argument("--version", "-V", action="version", version=law.__version__)

    # setup all progs
    mods = {}
    for prog in progs:
        mods[prog] = import_module("law.cli." + prog)
        mods[prog].setup_parser(sub_parsers)

    # default argv
    if not argv:
        argv = sys.argv[1:]

    # parse args and dispatch execution, with "run" being a special case
    prog = argv[0] if argv else None
    if prog == "run":
        sys.argv = ["law run"] + argv
        args = parser.parse_args(argv[:2])
    else:
        args = parser.parse_args(argv)

    # the parser determines the prog, so overwrite it
    prog = args.command
    if prog:
        mods[prog].execute(args)
    else:
        parser.print_help()
