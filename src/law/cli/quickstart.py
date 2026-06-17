# coding: utf-8

"""
"law quickstart" cli subprogram.
"""

import os
import shutil
import argparse

from law.config import Config
from law.util import law_src_path


_cfg = Config.instance()


def setup_parser(sub_parsers: argparse._SubParsersAction) -> None:
    """
    Sets up the command line parser for the *quickstart* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser(
        "quickstart",
        prog="law quickstart",
        description="Quickstart to create a minimal project structure and law configuration.",
    )

    parser.add_argument(
        "--directory",
        "-d",
        help="the directory where the quickstart files are created; default: current directory",
    )
    parser.add_argument(
        "--no-tasks",
        action="store_true",
        help="skip creating tasks",
    )
    parser.add_argument(
        "--no-config",
        action="store_true",
        help="skip creating the law.cfg file",
    )
    parser.add_argument(
        "--no-setup",
        action="store_true",
        help="skip creating the setup.sh file",
    )


def execute(args: argparse.Namespace) -> int:
    """
    Executes the *quickstart* subprogram with parsed commandline *args*.
    """
    # get the quickstart directory
    qs_dir = law_src_path("templates", "quickstart")

    # prepare the directory if it does not exist yet
    out_dir = os.path.normpath(os.path.abspath(args.directory))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    # copy tasks
    if not args.no_tasks:
        dst = os.path.join(out_dir, "my_package")
        shutil.copytree(os.path.join(qs_dir, "my_package"), dst)
        print(f"created {dst}")

    # copy config
    if not args.no_config:
        shutil.copy2(os.path.join(qs_dir, "law.cfg"), out_dir)
        print(f"created {os.path.join(out_dir, 'law.cfg')}")

    # copy setup
    if not args.no_setup:
        shutil.copy2(os.path.join(qs_dir, "setup.sh"), out_dir)
        print(f"created {os.path.join(out_dir, 'setup.sh')}")

    return 0
