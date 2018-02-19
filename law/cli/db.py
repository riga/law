# -*- coding: utf-8 -*-

"""
"law db" command line tool.
"""


import os
import sys
import traceback
from importlib import import_module
from collections import OrderedDict

import luigi
import six

from law.task.base import Task
from law.config import Config
from law.util import multi_match, colored


def setup_parser(sub_parsers):
    parser = sub_parsers.add_parser("db", prog="law db", description="law db file updater")

    parser.add_argument("--modules", "-m", nargs="+", help="additional modules to traverse")
    parser.add_argument("--remove", "-r", action="store_true", help="just remove the db file")
    parser.add_argument("--verbose", "-v", action="store_true", help="verbose output")


def execute(args):
    # just remove the db file?
    if args.remove:
        db_file = Config.instance().get("core", "db_file")
        if os.path.exists(db_file):
            os.remove(db_file)
            print("removed db file {}".format(db_file))
        return

    # get modules to lookup
    lookup = [m.strip() for m in Config.instance().keys("modules")]
    if args.modules:
        lookup += args.modules

    print("loading tasks from {} module(s)".format(len(lookup)))

    # loop through modules, import everything to load tasks
    for modid in lookup:
        if not modid:
            continue

        if args.verbose:
            sys.stdout.write("loading module '{}'".format(modid))

        try:
            import_module(modid)
        except Exception as e:
            if not args.verbose:
                print("Error in module '{}': {}".format(colored(modid, "red"), str(e)))
            else:
                print("\n\nError in module '{}':".format(colored(modid, "red")))
                traceback.print_exc()
            continue

        if args.verbose:
            print(", {}".format(colored("done", style="bright")))

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

        skip = cls.exclude_db or not callable(getattr(cls, "run", None)) \
            or getattr(cls.run, "__isabstractmethod__", False)
        if not skip:
            task_classes.append(cls)

    def get_task_params(cls):
        params = []
        for attr in dir(cls):
            member = getattr(cls, attr)
            if isinstance(member, luigi.Parameter):
                exclude = getattr(cls, "exclude_params_db", set())
                if not multi_match(attr, exclude, any):
                    params.append(attr.replace("_", "-"))
        return params

    def dbline(cls, params):
        # format: "module_id:task_family:param param ..."
        return "{}:{}:{}".format(cls.__module__, cls.task_family, " ".join(params))

    stats = OrderedDict()

    # write the db file
    db_file = Config.instance().get("core", "db_file")
    if not os.path.exists(os.path.dirname(db_file)):
        os.makedirs(os.path.dirname(db_file))

    with open(db_file, "w") as f:
        for cls in task_classes:
            # get prams
            params = get_task_params(cls)

            # fill stats
            if cls.__module__ not in stats:
                stats[cls.__module__] = []
            stats[cls.__module__].append((cls.task_family, params))

            f.write(dbline(cls, params) + "\n")

    # print stats
    if args.verbose:
        for mod, data in six.iteritems(stats):
            print("\nmodule '{}', {} task(s):".format(colored(mod, style="bright"), len(data)))
            for task_family, _ in data:
                print("    - {}".format(colored(task_family, "green")))
        print("")

    print("written {} task(s) to db file '{}'".format(len(task_classes), db_file))
