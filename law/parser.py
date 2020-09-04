# coding: utf-8

"""
Helpers to extract useful information from the luigi command line parser.
"""


__all__ = []


import logging
from collections import OrderedDict
from argparse import ArgumentParser

import luigi

from law.util import quote_cmd


logger = logging.getLogger(__name__)


# cached objects
_root_task = None
_full_parser = None
_root_task_parser = None
_global_cmdline_args = None
_global_cmdline_values = None


def root_task():
    """
    Returns the instance of the task that was triggered on the command line. The returned instance
    is cached.
    """
    global _root_task

    if not _root_task:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        _root_task = luigi_parser.get_task_obj()

        logger.debug("built root task instance using luigi argument parser")

    return _root_task


def full_parser():
    """
    Returns the full *ArgumentParser* used by the luigi ``CmdlineParser``. The returned instance is
    cached.
    """
    global _full_parser

    if not _full_parser:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        # build the full argument parser with luigi helpers
        root_task = luigi_parser.known_args.root_task
        _full_parser = luigi_parser._build_parser(root_task)

        logger.debug("built full luigi argument parser")

    return _full_parser


def root_task_parser():
    """
    Returns a new *ArgumentParser* instance that only contains parameter actions of the root task.
    The returned instance is cached.
    """
    global _root_task_parser

    if not _root_task_parser:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        root_task = luigi_parser.known_args.root_task

        # get all root task parameter destinations
        root_dests = []
        for task_name, _, param_name, _ in luigi.task_register.Register.get_all_params():
            if task_name == root_task:
                root_dests.append(param_name)

        # create a new parser and add all root actions
        _root_task_parser = ArgumentParser(add_help=False)
        for action in list(full_parser()._actions):
            if not action.option_strings or action.dest in root_dests:
                _root_task_parser._add_action(action)

        logger.debug("built luigi argument parser for root task {}".format(root_task))

    return _root_task_parser


def global_cmdline_args(exclude=None, join=False):
    """
    Returns a dictionary with keys and string values of command line arguments that do not belong to
    the root task. For bool parameters, such as ``--local-scheduler``, ``"True"`` is assumed if they
    are used as flags, i.e., without a parameter value. The returned dict is cached. *exclude* can
    be a list of argument names (with or without the leading ``"--"``) to be removed. When *join* is
    *True*, the key value pairs are joined into quoted strings and returned as a list. Example:

    .. code-block:: python

        global_cmdline_args()
        # -> {"--local-scheduler": "True", "--workers": "4"}

        global_cmdline_args(exclude=["workers"])
        # -> {"--local-scheduler": "True"}

        global_cmdline_args(join=True)
        # -> ["--local-scheduler=True", "--workers=4"]
    """
    global _global_cmdline_args

    if not _global_cmdline_args:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        _global_cmdline_args = OrderedDict()

        args = list(root_task_parser().parse_known_args(luigi_parser.cmdline_args)[1])

        # expand bool flags
        while args:
            arg = args.pop(0)

            # the argument must start with "--"
            if not arg.startswith("--"):
                raise Exception("global argument must start with '--', found '{}'".format(arg))

            # when the argument is a boolean flag without subsequent value, assume "True"
            is_flag = not args or args[0].startswith("--")
            value = "True" if is_flag else args.pop(0)

            _global_cmdline_args[arg] = value

    args = _global_cmdline_args

    if exclude:
        args = OrderedDict(args)

        for name in exclude:
            if not name.startswith("--"):
                name = "--" + name
            args.pop(name, None)

    if join:
        return ["{}={}".format(k, quote_cmd([v])) for k, v in args.items()]
    else:
        return args


def global_cmdline_values():
    """
    Returns a dictionary of global command line arguments (computed with
    :py:func:`global_cmdline_args`) to their current values. The returnd dictionary is cached.
    Example:

    .. code-block:: python

        global_cmdline_values()
        # -> {"core_local_scheduler": True}
    """
    global _global_cmdline_values

    if not _global_cmdline_values:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        # go through all actions of the full luigi parser and compare option strings
        # with the global cmdline args
        parser = full_parser()
        global_args = global_cmdline_args()
        _global_cmdline_values = {}
        for action in parser._actions:
            if any(arg in action.option_strings for arg in global_args):
                _global_cmdline_values[action.dest] = getattr(luigi_parser.known_args, action.dest)

    return _global_cmdline_values
