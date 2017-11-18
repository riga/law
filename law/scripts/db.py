# -*- coding: utf-8 -*-

"""
"law db" command line tool.
"""


import os
import sys
from importlib import import_module

import luigi
import six

from law.task.base import Task
from law.config import Config
from law.util import multi_match


common_params = ["workers", "local-scheduler", "help", "log-level"]


def setup_parser(sub_parsers):
    parser = sub_parsers.add_parser("db", prog="law db", description="law db file updater")

    parser.add_argument("--modules", "-m", nargs="+", help="additional modules to traverse")
    parser.add_argument("--verbose", "-v", action="store_true", help="verbose output")


def execute(args):
    # get modules to lookup
    lookup = [m.strip() for m in Config.instance().keys("modules")]
    if args.modules:
        lookup += args.modules

    print("loading tasks from {} modules".format(len(lookup)))

    # loop through modules, import everything to load tasks
    for modid in lookup:
        if not modid:
            continue

        if args.verbose:
            print("loading module '{}'".format(modid))

        try:
            mod = import_module(modid)
        except ImportError as e:
            continue

        if args.verbose:
            print("loaded module '{}'".format(modid))

    if args.verbose:
        print("")

    # determine tasks to write into the db file
    seen_families = []
    task_classes = []
    lookup = [Task]
    while lookup:
        cls = lookup.pop(0)
        lookup.extend(cls.__subclasses__())

        if cls.task_family in seen_families:
            continue
        seen_families.append(cls.task_family)

        skip = cls.exclude_db or not six.callable(getattr(cls, "run", None)) \
            or getattr(cls.run, "__isabstractmethod__", False)
        if not skip:
            task_classes.append(cls)

            if args.verbose:
                print("add task '{}'".format(cls.task_family))
        else:
            if args.verbose:
                print("skip task '{}'".format(cls.task_family))

    def dbline(cls, default_namespace=None):
        # determine parameters
        params = []
        for attr in dir(cls):
            member = getattr(cls, attr)
            if isinstance(member, luigi.Parameter):
                exclude = getattr(cls, "exclude_params_db", set())
                if not multi_match(attr, exclude, any):
                    params.append(attr.replace("_", "-"))

        # add common parameters
        params += common_params

        # build and return the line
        # format: "module_id:task_family:param param ..."
        return "{}:{}:{}".format(cls.__module__, cls.task_family, " ".join(params))

    # write the db file
    dbfile = Config.instance().get("core", "db_file")
    if not os.path.exists(os.path.dirname(dbfile)):
        os.makedirs(os.path.dirname(dbfile))

    with open(dbfile, "w") as f:
        for cls in task_classes:
            f.write(dbline(cls) + "\n")

    print("written {} task(s) to db".format(len(task_classes)))
