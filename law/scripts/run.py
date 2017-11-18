# -*- coding: utf-8 -*-

"""
"law run" command line tool.
"""


import os
import sys

from luigi.cmdline import luigi_run

from law.config import Config
from law.util import abort


def setup_parser(sub_parsers):
    parser = sub_parsers.add_parser("run", prog="law run", add_help=False,
        description="law run tool")

    parser.add_argument("task_family", help="the family of the task to run")
    parser.add_argument("parameter", nargs="*", help="task parameters to be passed to luigi")


def execute(args):
    # read task info from the db file
    info = read_task_from_db(args.task_family)
    if not info:
        abort("task family '{}' not found".format(args.task_family))

    # import the module and run luigi
    __import__(info[0], globals(), locals())
    luigi_run([args.task_family] + sys.argv[3:])


def read_task_from_db(task_family, dbfile=None):
    # read task information from the db file given a task family
    if dbfile is None:
        dbfile = Config.instance().get("core", "db_file")

    # open and go through lines
    with open(dbfile, "r") as f:
        for line in f.readlines():
            line = line.strip()
            try:
                modid, family, params = line.split(":", 2)
            except ValueError:
                pass
            if family == task_family:
                return modid, family, params

    return None
