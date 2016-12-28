# -*- coding: utf-8 -*-

"""
"law run" command line tool
"""


import os
import sys

from luigi.cmdline import luigi_run

import law
from law.config import Config
from law.util import abort


def setup_parser(sub_parsers):
    parser = sub_parsers.add_parser("run", prog="law run", add_help=False,
                                    description="law run tool")

    parser.add_argument("task_family", help="the family of the task to run")
    parser.add_argument("parameter", nargs="*", help="task parameters to be passed to luigi")


def execute(args):
    # read the module id for that task family from the db file
    dbfile = Config.instance().get("core", "db_file")
    with open(dbfile, "r") as f:
        for line in f.readlines():
            line = line.strip()
            try:
                mid, fam, params = line.split(":", 2)
            except ValueError:
                pass
            if fam == args.task_family:
                break
        else:
            abort("task family '%s' not found" % args.task_family)

    # import the module and run luigi
    __import__(mid, globals(), locals())
    luigi_run([args.task_family] + sys.argv[3:])
