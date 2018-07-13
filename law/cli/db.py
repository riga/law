# -*- coding: utf-8 -*-

"""
"law db" cli subprogram.
"""


import os
import sys
import traceback
from importlib import import_module
from collections import OrderedDict

import luigi
import six

from law.task.base import Task, ExternalTask
from law.config import Config
from law.util import multi_match, colored


def setup_parser(sub_parsers):
    """
    Sets up the command line parser for the *db* subprogram and adds it to *sub_parsers*.
    """
    parser = sub_parsers.add_parser("db", prog="law db", description="Create or update the"
        " (human-readable) law task database file ({}). This is only required for the shell"
        " auto-completion.".format(Config.instance().get("core", "db_file")))

    parser.add_argument("--modules", "-m", nargs="+", help="additional modules to traverse")
    parser.add_argument("--no-externals", "-e", action="store_true", help="skip external tasks")
    parser.add_argument("--location", "-l", action="store_true", help="print the location of the"
        " database file and exit")
    parser.add_argument("--remove", "-r", action="store_true", help="remove the database file and"
        " exit")
    parser.add_argument("--verbose", "-v", action="store_true", help="verbose output")


def execute(args):
    """
    Executes the *db* subprogram with parsed commandline *args*.
    """
    db_file = Config.instance().get_expanded("core", "db_file")

    # just print the file location?
    if args.location:
        print(db_file)
        return

    # just remove the db file?
    if args.remove:
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

        # skip already seen task families
        if cls.task_family in seen_families:
            continue
        seen_families.append(cls.task_family)

        # skip when explicitly excluded
        if cls.exclude_db:
            continue

        # skip external tasks
        is_external_task = issubclass(cls, ExternalTask)
        if args.no_externals and is_external_task:
            continue

        # skip non-external tasks without run implementation
        run_is_callable = callable(getattr(cls, "run", None))
        run_is_abstract = getattr(cls.run, "__isabstractmethod__", False)
        if not is_external_task and (not run_is_callable or run_is_abstract):
            continue

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


def get_global_parameters(config_names=("core", "scheduler", "worker", "retcode")):
    """
    Returns a list of global, luigi-internal configuration parameters. Each list item is a 4-tuple
    containing the configuration class, the parameter instance, the parameter name, and the full
    parameter name in the cli. When *config_names* is set, it should be a list of configuration
    class names that are exclusively taken into account.
    """
    params = []
    for cls in luigi.task.Config.__subclasses__():
        if config_names and cls.__name__ not in config_names:
            continue

        for attr in dir(cls):
            param = getattr(cls, attr)
            if not isinstance(param, luigi.Parameter):
                continue

            full_name = attr.replace("_", "-")
            if getattr(cls, "use_cmdline_section", True):
                full_name = "{}-{}".format(cls.__name__.replace("_", "-"), full_name)

            params.append((cls, param, attr, full_name))

    return params
