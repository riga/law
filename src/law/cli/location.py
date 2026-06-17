# coding: utf-8

"""
"law location" cli subprogram.
"""

import os
import argparse

from law.util import law_src_path


def setup_parser(sub_parsers: argparse._SubParsersAction) -> None:
    """
    Sets up the command line parser for the *location* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser(
        "location",
        prog="law location",
        description="Print the location of the law installation directory.",
    )
    parser.add_argument(
        "contrib",
        nargs="?",
        help="optional name of a contrib package whose location should be printed",
    )


def execute(args: argparse.Namespace) -> int:
    """
    Executes the *location* subprogram with parsed commandline *args*.
    """
    path = law_src_path()

    # lookup a specific contrib package
    if args.contrib:
        path = os.path.join(path, "contrib", args.contrib)
        if not os.path.exists(path):
            raise FileNotFoundError(f"contrib package '{args.contrib}' does not exist")

    print(path)

    return 0
