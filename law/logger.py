# coding: utf-8

"""
Law logging setup.
"""

__all__ = [
    "setup_logging", "setup_logger", "create_stream_handler", "is_tty_handler", "get_tty_handlers",
    "LogFormatter",
]


import logging

import six

from law.config import Config
from law.util import no_value, colored, ipykernel


_logging_setup = False


def setup_logging():
    """
    Sets up the internal law loggers as well as all other loggers listed in the ``"logging"`` config
    section as (*name*, *level*) pairs. This includes loggers that do not use the ``"law.*"``
    namespace which can be seen as a convenient feature to set up custom loggers.
    """
    global _logging_setup

    # make sure logging is setup only once
    if _logging_setup:
        return
    _logging_setup = True

    # set the handler of the law root logger which propagates it to lower level loggers
    logging.getLogger("law").addHandler(create_stream_handler())

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

    # add a console handler
    if add_console_handler:
        logger.addHandler(create_stream_handler())

    return logger


def create_stream_handler(handler_kwargs=None, formatter_kwargs=None, formatter_cls=no_value):
    """ create_stream_handler(handler_kwargs=None, formatter_kwargs=None, formatter_cls=LogFormatter)
    Creates a new StreamHandler instance, passing all *handler_kwargs* to its constructor, and
    returns it. When not *None*, an instance of *formatter_cls* is created using *formatter_kwargs*
    and added to the handler instance.
    """
    # create the handler
    handler = logging.StreamHandler(**(handler_kwargs or {}))

    # add a formatter
    if formatter_cls == no_value:
        formatter_cls = LogFormatter
    if formatter_cls is not None:
        formatter = formatter_cls(**(formatter_kwargs or {}))
        handler.setFormatter(formatter)

    return handler


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

    format_msg = None

    def format(self, record):
        """"""
        data = dict(
            level=colored(record.levelname, **self.level_styles.get(record.levelname, {})),
            name=record.name,
            msg=self.format_msg(record) if callable(self.format_msg) else record.getMessage(),
        )
        tmpl = self.tmpl

        # add traceback and change the template when the record contains exception info
        if record.exc_info:
            data["traceback"] = self.formatException(record.exc_info)
            tmpl = self.tmpl_error

        return tmpl.format(**data)
