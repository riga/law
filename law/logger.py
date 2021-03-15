# coding: utf-8

"""
Law logging setup.
"""

__all__ = [
    "console_handler", "setup_logging", "setup_logger", "is_tty_handler", "get_tty_handlers",
    "LogFormatter",
]


import logging

import six

from law.config import Config
from law.util import colored, ipykernel


#: Instance of a ``logging.StreamHandler`` that is used by logs in law. Its formatting is done by
#: :py:class:`LogFormatter`.
console_handler = None


def setup_logging():
    """
    Sets up the internal logging mechanism, i.e., it creates the :py:attr:`console_handler`, sets
    its formatting, and adds it to the main logger which propagates settings to lower level loggers.
    In addition, all other loggers listed in the ``"logging"`` config section as (*name*, *level*)
    pairs are set up. This includes loggers that do not use the ``"law.*"`` namespace which can be
    seen as a convenient feature to set up custom loggers.
    """
    global console_handler

    # make sure logging is setup only once
    if console_handler:
        return

    # set the handler of the law root logger which propagates it to lower level loggers
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LogFormatter())
    logging.getLogger("law").addHandler(console_handler)

    # set levels for all loggers and add the console handler for all non-law loggers
    cfg = Config.instance()
    for name, level in cfg.items("logging"):
        add_console_handler = not name.startswith("law.") and not get_tty_handlers(name)
        setup_logger(name, level, add_console_handler=add_console_handler, clear=False)


def setup_logger(name, level=None, add_console_handler=True, clear=False):
    """
    Sets up a logger given by its *name*, configures it to have a certain *level* and adds a
    preconfigured console handler when *add_console_handler* is *True*. The *name* can either be an
    integer or the name of a level present in the *logging* module. When no *level* is given, the
    level of the ``"law"`` base logger is used as a default. When the logger already existed and
    *clear* is *True*, all handlers and filters are removed first. The logger object is returned.
    """
    # sanitize the level
    if isinstance(level, six.string_types):
        level = getattr(logging, level.upper(), None)
    if level is None:
        level = logging.getLogger("law").level

    # clear handlers and filters
    is_existing = name in logging.root.manager.loggerDict
    logger = logging.getLogger(name)
    if is_existing and clear:
        for h in list(logger.handlers):
            logger.removeHandler(h)
        for f in list(logger.filters):
            logger.removeFilter(f)

    # set the level
    logger.setLevel(level)

    # add the global console handler
    if console_handler and add_console_handler:
        logger.addHandler(console_handler)

    return logger


def is_tty_handler(handler):
    """
    Returns *True* if a logging *handler* is a *StreamHandler* which logs to a tty (i.e. *stdout* or
    *stderr*), an IPython *OutStream*, or a base *Handler* with a *console* attribute evaluating to
    *True*. The latter check is intended to cover a variety of handlers provided by custom modules.
    """
    if isinstance(handler, logging.StreamHandler) and getattr(handler, "stream", None):
        if callable(getattr(handler.stream, "isatty", None)) and handler.stream.isatty():
            return True
        elif ipykernel and isinstance(handler.stream, ipykernel.iostream.OutStream):
            return True
    if isinstance(handler, logging.Handler) and getattr(handler, "console", None):
        return True
    return False


def get_tty_handlers(logger):
    """
    Returns a list of all handlers of a *logger* that log to a tty.
    """
    if isinstance(logger, six.string_types):
        logger = logging.getLogger(logger)
    return [handler for handler in getattr(logger, "handlers", []) if is_tty_handler(handler)]


class LogFormatter(logging.Formatter):
    """
    Law log formatter class using timestamps and colored log levels.

    .. py:classattribute:: tmpl
       type: string

       Template for log messages.

    .. py:classattribute:: level_styles
       type: dict

       Style attributes per log level for coloring and highlighting via :py:func:`law.util.colored`.
    """

    tmpl = "{level}: {name} - {msg}"
    tmpl_error = "{level}: {name} - {msg}\n{traceback}"

    level_styles = {
        "NOTSET": {},
        "DEBUG": {"color": "cyan"},
        "INFO": {"color": "green"},
        "WARNING": {"color": "yellow"},
        "ERROR": {"color": "red"},
        "CRITICAL": {"color": "red", "style": "bright"},
    }

    def format(self, record):
        """"""
        data = dict(
            level=colored(record.levelname, **self.level_styles.get(record.levelname, {})),
            name=record.name,
            msg=record.getMessage(),
        )
        tmpl = self.tmpl

        # add traceback and change the template when the record contains exception info
        if record.exc_info:
            data["traceback"] = self.formatException(record.exc_info)
            tmpl = self.tmpl_error

        return tmpl.format(**data)
