# -*- coding: utf-8 -*-

"""
"law db" command line tool.
"""


import os
import sys
from importlib import import_module

import luigi
import six

import law
from law.config import Config
from law.util import multi_match


def setup_parser(sub_parsers):
    parser = sub_parsers.add_parser("db", prog="law db", description="law db file updater")

    parser.add_argument("--default-namespace", "-n", help="a default task namespace to apply")
    parser.add_argument("--python-path", "-p", action="store_true",
                        help="also traverse in PYTHONPATH")
    parser.add_argument("--verbose", "-v", action="store_true", help="verbose output")


def execute(args):
    # determine paths to lookup
    lookup = [p.strip() for p in Config.instance().keys("paths")]
    if args.python_path and "PYTHONPATH" in os.environ:
        lookup.extend(p.strip() for p in os.environ["PYTHONPATH"].split(os.pathsep))

    # loop through paths, import everything to load tasks
    for path in lookup:
        path = str(os.path.expandvars(os.path.expanduser(path)))
        if not path:
            continue

        if args.verbose:
            print("looking into path '%s'" % path)

        for base, dirnames, filenames in os.walk(path):
            for filename in filenames:
                filebase, fileext = os.path.splitext(filename)
                if fileext != ".py" or filebase == "__main__":
                    continue
                fullpath = os.path.join(base, filename)
                # try to find a matching path in sys.path to be able to import it
                modparts = os.path.join(base, filebase).strip("/").split("/")
                if modparts[-1] == "__init__":
                    modparts.pop()
                modid = ""
                while modparts:
                    modid = modparts.pop() + (modid and ".") + modid
                    try:
                        mod = import_module(modid)

                        if args.verbose:
                            print("    loaded module '%s'" % modid)
                    except ImportError as e:
                        pass

    if args.verbose:
        print("")

    # determine data to write: "module_id:task_family:param param ..."
    seen_families = []
    task_classes = []
    lookup = [law.Task]
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
                print("added task class with family '%s'" % cls.task_family)

    def dbline(cls, default_namespace=None):
        # determine parameters
        params = ["workers", "local-scheduler", "help"]
        for attr in dir(cls):
            member = getattr(cls, attr)
            if isinstance(member, luigi.Parameter):
                exclude = getattr(cls, "exclude_params_db", set())
                if not multi_match(attr, exclude, any):
                    params.append(attr.replace("_", "-"))

        # use a truncated task family when a default namespace is set
        if default_namespace is None:
            family = cls.task_family
        else:
            family = cls.task_family[(len(default_namespace) + 1):]

        return cls.__module__ + ":" + family + ":" + " ".join(params)

    # write the db file
    dbfile = Config.instance().get("core", "db_file")
    if not os.path.exists(os.path.dirname(dbfile)):
        os.makedirs(os.path.dirname(dbfile))

    with open(dbfile, "w") as f:
        for cls in task_classes:
            f.write(dbline(cls) + "\n")

        # include lines for default namespace
        if args.default_namespace:
            for cls in task_classes:
                if cls.task_family.startswith(args.default_namespace + "."):
                    f.write(dbline(cls, args.default_namespace) + "\n")

            if args.verbose:
                print("")
                print("included tasks with default namespace '%s'" % args.default_namespace)
