# coding: utf-8

"""
Helpers to extract useful information from the luigi command line parser.
"""


__all__ = []


import logging
from argparse import ArgumentParser

import luigi


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


def global_cmdline_args(exclude=None):
    """
    Returns the list of command line arguments that do not belong to the root task. For bool
    parameters, such as ``--local-scheduler``, ``"True"`` is inserted if they are used as flags,
    i.e., without a parameter value. The returned list is cached. *exclude* can be a list of
    arguments (including ``"--"`` prefix) that should be removed from the returned list. As
    :py:func:`remove_cmdline_arg` is used internally, an argument can be specified either by a name
    or a tuple of argument name and expected number of values on the command line. Example:

    .. code-block:: python

        global_cmdline_args()
        # -> ["--local-scheduler", "True", --workers", "4"]

        global_cmdline_args(exclude=[("--workers", 1)])
        # -> ["--local-scheduler", "True"]
    """
    global _global_cmdline_args

    if not _global_cmdline_args:
        luigi_parser = luigi.cmdline_parser.CmdlineParser.get_instance()
        if not luigi_parser:
            return None

        _global_cmdline_args = []

        args = root_task_parser().parse_known_args(luigi_parser.cmdline_args)[1]

        # expand bool flags
        for i, arg in enumerate(args):
            _global_cmdline_args.append(arg)
            if arg.startswith("--"):
                is_flag = i == (len(args) - 1) or args[i + 1].startswith("--")
                if is_flag:
                    _global_cmdline_args.append("True")

    if not exclude:
        return _global_cmdline_args

    else:
        args = list(_global_cmdline_args)
        for value in exclude:
            if not isinstance(value, tuple):
                value = (value,)
            args = remove_cmdline_arg(args, *value)
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


def add_cmdline_arg(args, arg, *values):
    """
    Adds a command line argument *arg* to a list of argument *args*, e.g. as returned from
    :py:func:`global_cmdline_args`. When *arg* exists, *args* is returned unchanged. Otherwise,
    *arg* is appended to the end with optional argument *values*. Examples:

    .. code-block:: python

        args = global_cmdline_values()
        # -> ["--local-scheduler", "True"]

        add_cmdline_arg(args, "--local-scheduler", True)
        # -> ["--local-scheduler", "True"]

        add_cmdline_arg(args, "--workers", 4)
        # -> ["--local-scheduler", "--workers", "4"]
    """
    if arg not in args:
        args = list(args) + [arg] + [str(value) for value in values]
    return args


def get_cmdline_arg(args, arg, n=None, exact=True):
    """
    Returns the value of a command line argument *arg* from a list of arguments *args*. When *arg*
    is not found within *args*, *None* is returned. The type of the return value depends on *n*.


    - When *n* is not set (or *None*, the default), the next value following *arg* is returned if
      there is any. *None* is returned when *arg* is the last element in *args*.
    - When *n* is 0, *True* is returned when *arg* was found and *None* otherwise.
    - When *n* is larger than 0, the next *n* values following *arg* are returned in a list. When
      the number of expected values is not matched and *exact* is *True*, *None* is returned.
      Otherwise, the number of returned values might be smaller than *n*.

    Examples:

    .. code-block:: python

        args = global_cmdline_values()
        # -> ["--local-scheduler", "True", "--workers", "4"]

        get_cmdline_arg(args, "--local-scheduler")
        # -> "True"

        get_cmdline_arg(args, "--local-scheduler", n=0)
        # -> True

        get_cmdline_arg(args, "--workers")
        # -> "4"

        get_cmdline_arg(args, "--workers", n=1)
        # -> ["4"]

        get_cmdline_arg(args, "--workers", n=2)
        # -> None

        get_cmdline_arg(args, "--workers", n=2, exact=False)
        # -> ["4"]
    """
    if arg not in args:
        return None
    elif n == 0:
        return True

    idx = args.index(arg)

    if n is None:
        values = args[idx + 1:idx + 2]
        return None if not values else values[0]
    else:
        values = args[idx + 1:idx + 1 + n]
        return None if len(values) != n and exact else values


def remove_cmdline_arg(args, arg, n=0):
    """
    Removes the command line argument *arg* from a list of arguments *args*, e.g. as returned from
    :py:func:`global_cmdline_args`. When *n* is 0 (the default), only the argument is removed.
    Otherwise, the following *n* values are removed. Examples:

    .. code-block:: python

        args = global_cmdline_values()
        # -> ["--local-scheduler", "--workers", "4"]

        remove_cmdline_arg(args, "--local-scheduler")
        # -> ["--workers", "4"]

        remove_cmdline_arg(args, "--workers", 1)
        # -> ["--local-scheduler"]
    """
    if arg in args:
        idx = args.index(arg)
        args = list(args)
        del args[idx:idx + max(n + 1, 1)]
    return args
