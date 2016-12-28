# -*- coding: utf-8 -*-

"""
Main law command line interface.
"""


import os
import sys
from importlib import import_module
from argparse import ArgumentParser


def main():
    # setup the main parser and sub parsers
    parser = ArgumentParser(description="law command line tool")
    sub_parsers = parser.add_subparsers(help="subcommands", dest="command")

    # setup all progs
    progs = ["run", "db", "completion"]
    mods = {}
    for prog in progs:
        mods[prog] = import_module("law.scripts." + prog)
        mods[prog].setup_parser(sub_parsers)

    # parse args and dispatch execution
    # hack: for the "run" prog, we actually want to forward everything to the luigi interface
    if sys.argv[1] == "run":
        args = parser.parse_args(sys.argv[1:3])
    else:
        args = parser.parse_args()

    mods[args.command].execute(args)


if __name__ == "__main__":
    main()
