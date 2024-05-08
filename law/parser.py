# coding: utf-8

"""
Helpers to extract useful information from the luigi command line parser.
"""

from __future__ import annotations

__all__: list[str] = []

from argparse import ArgumentParser

import luigi  # type: ignore[import-untyped]

from law.logger import get_logger
from law.util import multi_match
from law._types import Sequence, Any


logger = get_logger(__name__)


# cached objects
_root_task: luigi.Task | None = None
_full_parser: luigi.cmdline_parser.CmdlineParser | None = None
_root_task_parser: ArgumentParser | None = None
_global_cmdline_args: dict[str, str] | None = None
_global_cmdline_values: dict[str, Any] | None = None


def root_task(task: luigi.Task | None = None) -> luigi.Task | None:
    """
    Returns the instance of the task that was triggered on the command line. The returned instance
    is cached. When *task* is define and no root task was cached yet, this methods acts as a setter.
    """
    global _root_task

    if not _root_task:
        if task:
            _root_task = task
            logger.debug("set root task to externally passed instance")
        else:
            luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
            if luigi_parser is None:
                return None

            _root_task = luigi_parser.get_task_obj()

            logger.debug("built root task instance using luigi argument parser")

    return _root_task


def full_parser() -> luigi.cmdline_parser.CmdlineParser | None:
    """
    Returns the full *ArgumentParser* used by the luigi ``CmdlineParser``. The returned instance is
    cached.
    """
    global _full_parser

    if not _full_parser:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if luigi_parser is None:
            return None

        # build the full argument parser with luigi helpers
        root_task = luigi_parser.known_args.root_task
        _full_parser = luigi_parser._build_parser(root_task=root_task)

        logger.debug("built full luigi argument parser")

    return _full_parser


def root_task_parser() -> ArgumentParser | None:
    """
    Returns a new *ArgumentParser* instance that only contains parameter actions of the root task.
    The returned instance is cached.
    """
    global _root_task_parser

    if not _root_task_parser:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if luigi_parser is None:
            return None

        _full_parser = full_parser()
        if _full_parser is None:
            return None

        root_task = luigi_parser.known_args.root_task

        # get all root task parameter destinations
        root_dests = []
        for task_name, _, param_name, _ in luigi.task_register.Register.get_all_params():
            if task_name == root_task:
                root_dests.append(param_name)

        # create a new parser and add all root actions
        _root_task_parser = ArgumentParser(add_help=False)
        for action in list(_full_parser._actions):
            if not action.option_strings or action.dest in root_dests:
                _root_task_parser._add_action(action)

        logger.debug(f"built luigi argument parser for root task {root_task}")

    return _root_task_parser


def global_cmdline_args(exclude: Sequence[str] | None = None) -> dict[str, str] | None:
    """
    Returns a dictionary with keys and string values of command line arguments that do not belong to
    the root task. For bool parameters, such as ``--local-scheduler``, ``"True"`` is assumed if they
    are used as flags, i.e., without a parameter value. The returned dict is cached. *exclude* can
    be a list of argument names (with or without the leading ``"--"``) to be removed. Example:

    .. code-block:: python

        global_cmdline_args()
        # -> {"--local-scheduler": "True", "--workers": "4"}

        global_cmdline_args(exclude=["workers"])
        # -> {"--local-scheduler": "True"}
    """
    global _global_cmdline_args

    if _global_cmdline_args is None:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if luigi_parser is None:
            return None

        _root_task_parser = root_task_parser()
        if _root_task_parser is None:
            return None

        _global_cmdline_args = {}
        args: list[str] = list(_root_task_parser.parse_known_args(luigi_parser.cmdline_args)[1])

        # expand bool flags
        while args:
            arg = args.pop(0)

            # the argument must start with "--"
            if not arg.startswith("--"):
                raise Exception(f"global argument must start with '--', found '{arg}'")

            # get the corresponding value which is either part of the argument itself in the format
            # "--arg=value" or passed in the next argument which must not start with "--" (in this
            # case it is interpreted as a boolean "True" value)
            value: str
            if "=" in arg:
                arg, value = arg.split("=", 1)
            elif args and not args[0].startswith("--"):
                value = args.pop(0)
            else:
                value = "True"

            _global_cmdline_args[arg] = value

    global_args = _global_cmdline_args

    if exclude:
        # create a copy and remove excluded keys, which can be patterns
        global_args = dict(global_args)
        _exclude = ["--" + pattern.lstrip("-") for pattern in exclude]
        for arg in list(global_args.keys()):
            if multi_match(arg, _exclude, mode=any):
                global_args.pop(arg)

    return global_args


def global_cmdline_values() -> dict[str, Any] | None:
    """
    Returns a dictionary of global command line arguments (computed with
    :py:func:`global_cmdline_args`) to their current values. The returnd dictionary is cached.
    Example:

    .. code-block:: python

        global_cmdline_values()
        # -> {"core_local_scheduler": True}
    """
    global _global_cmdline_values

    if _global_cmdline_values is None:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if luigi_parser is None:
            return None

        _full_parser = full_parser()
        if _full_parser is None:
            return None

        global_args = global_cmdline_args()
        if global_args is None:
            return None

        # go through all actions of the luigi parser and compare options with global cmdline args
        _global_cmdline_values = {}
        for action in _full_parser._actions:
            if any(arg in action.option_strings for arg in global_args):
                _global_cmdline_values[action.dest] = getattr(luigi_parser.known_args, action.dest)

    return _global_cmdline_values


def _reset() -> None:
    """
    Resets all singletons defined by the parser functions above.
    """
    global _root_task
    global _full_parser
    global _root_task_parser
    global _global_cmdline_args
    global _global_cmdline_values

    _root_task = None
    _full_parser = None
    _root_task_parser = None
    _global_cmdline_args = None
    _global_cmdline_values = None
