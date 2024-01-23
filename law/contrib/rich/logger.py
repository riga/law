# coding: utf-8

"""
Logging optimization using rich.
"""

from __future__ import annotations

__all__ = ["replace_console_handlers"]

import logging

from law.config import Config
from law.logger import is_tty_handler, Logger
from law.util import make_list, multi_match
from law._types import Sequence, Callable


def replace_console_handlers(
    loggers: Sequence[str] = ("luigi", "luigi.*", "luigi-*", "law", "law.*"),
    level: str | int | None = None,
    force_add: bool = False,
    check_fn: Callable[[logging.Logger, logging.Handler], bool] | None = None,
    **kwargs,
) -> list[tuple[logging.Logger, list[logging.Handler]]]:
    """
    Removes all tty stream handlers (i.e. those logging to *stdout* or *stderr*) from certain
    *loggers* and adds a new ``rich.logging.RichHandler`` instance with a specified *level* and all
    *kwargs* passed as additional options to its constructor. *loggers* can either be logger
    instances or names. In the latter case, the names are used as patterns to identify matching
    loggers. Unless *force_add* is *True*, no new handler is added when no tty stream handler was
    previously registered.

    *check_fn* can be a function with two arguments, a logger instance and a handler instance, that
    should return *True* if that handler should be removed. When *None*, all handlers inheriting
    from the basic ``logging.StreamHandler`` are removed if their *stream* attibute referes to a
    tty stream. When *level* is *None*, it defaults to the log level of the first removed handler.
    In case no default level can be determined, *INFO* is used.

    The removed handlers are returned in a list of 2-tuples (*logger*, *removed_handlers*).
    """
    from rich import logging as rich_logging  # type: ignore[import-untyped, import-not-found]

    # prepare the return value
    ret = []

    # default check_fn
    if check_fn is None:
        check_fn = lambda logger, handler: is_tty_handler(handler)

    loggers = make_list(loggers)
    for name, logger in logging.root.manager.loggerDict.items():
        if not isinstance(logger, logging.Logger):
            continue

        # check if the logger is selected
        for l in loggers:
            if logger == l or (isinstance(l, str) and multi_match(name, l)):
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
            logger.addHandler(rich_logging.RichHandler(level, **kwargs))

            # emit warning for colored_* configs
            cfg = Config.instance()
            opts = [(s, o) for s in ["task", "target"] for o in ["colored_str", "colored_repr"]]
            if isinstance(logger, Logger) and any(cfg.get_expanded_bool(*opt) for opt in opts):
                logger.warning_once(
                    "interfering_colors_in_rich_handler",
                    "law is currently configured to colorize string representations of tasks and "
                    "targets which might lead to malformed logs of the RichHandler; to avoid this, "
                    f"consider updating your law configuration file ({cfg.config_file}) to:\n"
                    "[task]\ncolored_repr: False\ncolored_str: False\n\n"
                    "[target]\ncolored_repr: False\ncolored_str: False",
                )

        # add the removed handlers to the returned list
        if removed_handlers:
            ret.append((logger, removed_handlers))

    return ret
