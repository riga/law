# coding: utf-8

"""
Law command line interface entry point.
"""

from __future__ import annotations

import sys
from importlib import import_module
from argparse import ArgumentParser

import law


progs = ["run", "index", "config", "software", "completion", "location", "quickstart", "luigid"]


def run(argv: list[str] | None = None) -> int:
    """
    Entry point to the law cli. Sets up all parsers, parses all arguments given by *argv*, and
    executes the requested subprogram. When *None*, *argv* defaults to ``sys.argv[1:]``.
    """
    # setup the main parser and sub parsers
    parser = ArgumentParser(
        prog="law",
        description="The law command line tool.",
    )
    sub_parsers = parser.add_subparsers(
        help="subcommands",
        dest="command",
    )

    # add main arguments
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=law.__version__,
    )

    # setup all progs
    mods = {}
    for prog in progs:
        mods[prog] = import_module(f"law.cli.{prog}")
        mods[prog].setup_parser(sub_parsers)

    # default argv
    if not argv:
        argv = sys.argv[1:]

    # argv that is passed to the prog execution when set
    prog_argv = None

    # parse args and dispatch execution, considering some special cases
    prog: str | None = argv[0] if argv else None
    if prog == "run":
        args = parser.parse_args(argv[:2])
        prog_argv = ["law run"] + argv
    elif prog == "luigid":
        args = parser.parse_args(argv[:1])
        prog_argv = ["law luigid"] + argv
    else:
        args = parser.parse_args(argv)

    # the parser determines the prog, so overwrite it
    prog = args.command
    if not prog:
        parser.print_help()
        return 0

    exec_args: tuple = (args,)
    if prog_argv is not None:
        exec_args += (prog_argv,)

    return mods[prog].execute(*exec_args)
