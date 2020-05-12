# coding: utf-8

"""
Logging optimization using rich.
"""


__all__ = ["replace_console_handlers"]


import logging

import six

from law.logger import is_tty_handler
from law.util import make_list, multi_match


def replace_console_handlers(loggers=("luigi", "luigi.*", "luigi-*", "law", "law.*"), level=None,
        force_add=False, check_fn=None):
    """
    Removes all tty stream handlers (i.e. those logging to *stdout* or *stderr*) from certain
    *loggers* and adds a ``rich.logging.RichHandler`` with a specified *level*. *loggers* can either
    be logger instances or names. In the latter case, the names are used as patterns to identify
    matching loggers. Unless *force_add* is *True*, no new handler is added when no tty stream
    handler was previously registered.

    *check_fn* can be a function with two arguments, a logger instance and a handler instance, that
    should return *True* if that handler should be removed. When *None*, all handlers inheriting
    from the basic ``logging.StreamHandler`` are removed if their *stream* attibute referes to a
    tty stream. When *level* is *None*, it defaults to the log level of the first removed handler.
    In case no default level can be determined, *INFO* is used.

    The removed handlers are returned in a list of 2-tuples (*logger*, *removed_handlers*).
    """
    from rich import logging as rich_logging

    # prepare the return value
    ret = []

    # default check_fn
    if check_fn is None:
        check_fn = lambda logger, handler: is_tty_handler(handler)

    loggers = make_list(loggers)
    for name, logger in logging.root.manager.loggerDict.items():
        # check if the logger is selected
        for l in loggers:
            if logger == l:
                break
            elif isinstance(l, six.string_types) and multi_match(name, l):
                break
        else:
            # when this point is reached, the logger was not selected
            continue

        removed_handlers = []
        handlers = getattr(logger, "handlers", [])
        for handler in handlers:
            if check_fn(logger, handler):
                # get the level
                if level is None:
                    level = getattr(handler, "level", None)

                # remove it
                logger.removeHandler(handler)
                removed_handlers.append(handler)

        # when at least one handler was found and removed, or force_add is True, add a rich handler
        if removed_handlers or force_add:
            # make sure the level is set
            if level is None:
                level = logging.INFO

            # add the rich handler
            logger.addHandler(rich_logging.RichHandler(level))

        # add the removed handlers to the returned list
        if removed_handlers:
            ret.append((logger, removed_handlers))

    return ret
