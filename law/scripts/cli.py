# -*- coding: utf-8 -*-

"""
Main law command line interface.
"""


import os
import sys
from importlib import import_module
from argparse import ArgumentParser


progs = ["run", "db", "config", "software", "completion"]
forward_progs = ["run"]


def main():
    # setup the main parser and sub parsers
    parser = ArgumentParser(prog="law", description="law command line tool")
    sub_parsers = parser.add_subparsers(help="subcommands", dest="command")

    # setup all progs
    mods = {}
    for prog in progs:
        mods[prog] = import_module("law.scripts." + prog)
        mods[prog].setup_parser(sub_parsers)

    # parse args and dispatch execution
    if len(sys.argv) >= 2 and sys.argv[1] in forward_progs:
        args = parser.parse_args(sys.argv[1:3])
    else:
        args = parser.parse_args()

    mods[args.command].execute(args)


if __name__ == "__main__":
    main()
