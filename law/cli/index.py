# coding: utf-8

"""
"law index" cli subprogram.
"""

from __future__ import annotations

import os
import sys
import traceback
import importlib
import argparse

import luigi  # type: ignore[import-untyped]

from law.config import Config
from law.task.base import Task, ExternalTask
from law.util import multi_match, colored, abort, makedirs, brace_expand
from law.logger import get_logger
from law._types import Sequence, Type


logger = get_logger(__name__)

_cfg = Config.instance()


def setup_parser(sub_parsers: argparse._SubParsersAction) -> None:
    """
    Sets up the command line parser for the *index* subprogram and adds it to *sub_parsers*.
    """
    index_file = _cfg.get_expanded("core", "index_file")
    parser = sub_parsers.add_parser(
        "index",
        prog="law index",
        description=f"Create or update the (human-readable) law task index file ({index_file}). "
        "This is only required for the shell auto-completion.",
    )

    parser.add_argument(
        "--modules",
        "-m",
        nargs="+",
        help="additional modules to traverse",
    )
    parser.add_argument(
        "--no-externals",
        "-e",
        action="store_true",
        help="skip external tasks",
    )
    parser.add_argument(
        "--remove",
        "-r",
        action="store_true",
        help="remove the index file and exit",
    )
    parser.add_argument(
        "--show",
        "-s",
        action="store_true",
        help="print the content of the index file and exit",
    )
    parser.add_argument(
        "--location",
        "-l",
        action="store_true",
        help="print the location of the index file and exit",
    )
    parser.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="quiet mode without output",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="verbose output, disables the quiet mode when set",
    )


def execute(args: argparse.Namespace) -> int:
    """
    Executes the *index* subprogram with parsed commandline *args*.
    """
    # update args
    if args.verbose:
        args.quiet = False

    cfg = Config.instance()
    index_file = cfg.get_expanded("core", "index_file")

    # just print the file location?
    if args.location:
        print(index_file)
        return 0

    # just show the file content?
    if args.show:
        if os.path.exists(index_file):
            with open(index_file, "r") as f:
                print(f.read())
            return 0
        return abort(f"index file {index_file} does not exist")

    # just remove the index file?
    if args.remove:
        if os.path.exists(index_file):
            os.remove(index_file)
            print(f"removed index file {index_file}")
        return 0

    # get modules to lookup
    lookup = [m.strip() for m in cfg.options("modules")]
    if args.modules:
        lookup += args.modules

    # expand braces
    lookup = sum(map(brace_expand, lookup), [])

    if not args.quiet:
        print(f"indexing tasks in {len(lookup)} module(s)")

    exit_code = 0

    # loop through modules, import everything to load tasks
    for modid in lookup:
        if not modid:
            continue

        if args.verbose:
            sys.stdout.write(f"loading module '{modid}'")

        try:
            importlib.import_module(modid)
        except Exception as e:
            exit_code += 1
            if not args.verbose:
                print(f"error in module '{colored(modid, 'red')}': {e}")
            else:
                print(f"\n\nerror in module '{colored(modid, 'red')}':")
                traceback.print_exc()
            continue

        if args.verbose:
            print(f", {colored('done', style='bright')}")

    # determine tasks to write into the index file
    seen_families = []
    task_classes = []
    lookup: list[Type[Task]] = [Task]
    while lookup:
        cls: Type[Task] = lookup.pop(0)  # type: ignore
        lookup.extend(cls.__subclasses__())

        # skip tasks in __main__ module in interactive sessions
        if cls.__module__ == "__main__":
            continue

        # skip when explicitly excluded
        if cls.exclude_index:
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

        # show an error when there is a "-" in the task family as the luigi command line parser will
        # automatically map it to "_", i.e., it will fail to lookup the actual task class
        # skip the task
        task_family = cls.get_task_family()
        if "-" in task_family:
            logger.critical(
                f"skipping task '{cls}' as its family '{task_family}' contains a '-' which cannot "
                "be interpreted by luigi's command line parser, please use '_' or alike",
            )
            continue

        # show an error when there is a "_" after a "." in the task family, i.e., when there is a
        # "_" in the class name (which is bad python practice anyway), as the shell autocompletion
        # is not able to decide whether it should complete the task family or a task-level parameter
        # skip the task
        if "_" in task_family.rsplit(".", 1)[-1]:
            logger.error(
                f"skipping task '{cls}' as its family '{task_family}' contains a '_' after the "
                "namespace definition which would lead to ambiguities between task families and "
                "task-level parameters in the law shell autocompletion",
            )
            continue

        # skip already seen task families and warn when the class is not added yet in the classes to
        # index as this is usually a sign of multiple definitions of the same task, e.g. through
        # imports of the same physical file via different module ids
        if task_family in seen_families:
            if cls not in task_classes:
                logger.error(
                    f"skipping task '{cls}' as a task with the same family '{task_family}' but a "
                    "different different address was already seen; this is likely due to multiple "
                    "imports of the same physical file through different module ids and since it "
                    "is no longer unique, luigi's task lookup will probably fail",
                )
            continue
        seen_families.append(task_family)

        task_classes.append(cls)

    def get_task_params(cls) -> list[str]:
        params = []
        for attr in dir(cls):
            member = getattr(cls, attr)
            if isinstance(member, luigi.Parameter):
                exclude: set[str] = getattr(cls, "exclude_params_index", set())
                if not multi_match(attr, exclude, any):
                    params.append(attr.replace("_", "-"))
        return params

    def index_line(cls, params):
        return f"{cls.__module__}:{cls.get_task_family()}:{' '.join(params)}"

    stats: dict[str, list[tuple[str, list[str]]]] = dict()

    # write the index file
    makedirs(os.path.dirname(index_file))

    with open(index_file, "w") as f:
        for cls in task_classes:
            # get prams
            params = get_task_params(cls)

            # fill stats
            if cls.__module__ not in stats:
                stats[cls.__module__] = []
            stats[cls.__module__].append((cls.get_task_family(), params))

            f.write(index_line(cls, params) + "\n")

    # print stats
    if args.verbose:
        for mod, data in stats.items():
            print(f"\nmodule '{colored(mod, style='bright')}', {len(data)} task(s):")
            for task_family, _ in data:
                print(f"    - {colored(task_family, 'green')}")
        print("")

    if not args.quiet:
        print(f"written {len(task_classes)} task(s) to index file '{index_file}'")

    return exit_code


def get_global_parameters(
    config_names: Sequence[str] = ("core", "scheduler", "worker", "retcode"),
) -> list[tuple[type, luigi.Parameter, str, str]]:
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
                full_name = f"{cls.__name__.replace('_', '-')}-{full_name}"

            params.append((cls, param, attr, full_name))

    return params
