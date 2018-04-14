# -*- coding: utf-8 -*-

"""
Law command line interface entry point.
"""


import sys
from importlib import import_module
from argparse import ArgumentParser

import law


progs = ["run", "db", "config", "software", "completion"]
forward_progs = ["run"]


def run():
    """
    Entry point to the law cli. Sets up all parsers, parses all arguments, and executes the
    requested subprogram.
    """
    # setup the main parser and sub parsers
    parser = ArgumentParser(prog="law", description="law command line tool")
    sub_parsers = parser.add_subparsers(help="subcommands", dest="command")

    # add main arguments
    parser.add_argument("--version", "-V", action="version", version=law.__version__)

    # setup all progs
    mods = {}
    for prog in progs:
        mods[prog] = import_module("law.cli." + prog)
        mods[prog].setup_parser(sub_parsers)

    # parse args and dispatch execution
    if len(sys.argv) >= 2 and sys.argv[1] in forward_progs:
        args = parser.parse_args(sys.argv[1:3])
    else:
        args = parser.parse_args()

    if args.command:
        mods[args.command].execute(args)
    else:
        parser.print_help()
