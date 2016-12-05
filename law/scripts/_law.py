# -*- coding: utf-8 -*-

"""
law command line interface to run tasks.
"""


import os
import sys
from argparse import ArgumentParser

from luigi.cmdline import luigi_run

import law
import law.util
from law.config import Config


def main():
    # parser arguments to determine task family and parameters
    parser = ArgumentParser(description="law run tool")

    parser.add_argument("task_family", help="the family of the task to run")
    parser.add_argument("parameter", nargs="*", help="task parameters to be passed to luigi")

    args = parser.parse_args(sys.argv[1:2])

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
            law.util.abort("task family '%s' not found" % args.task_family)

    # import the module and run luigi
    __import__(mid, globals(), locals())
    luigi_run([args.task_family] + sys.argv[2:])


if __name__ == "__main__":
    main()
