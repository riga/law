# coding: utf-8

"""
Law logging setup.
"""

__all__ = [
    "get_logger", "setup_logging", "setup_logger", "create_stream_handler", "is_tty_handler",
    "get_tty_handlers", "Logger", "LogFormatter",
]


from collections import defaultdict
import logging

import six

from law.util import no_value, colored, ipykernel


_logging_setup = False


def get_logger(*args, **kwargs):
    """
    Replacement for *logging.getLogger* that makes sure that the custom :py:class:`Logger` class is
    used when new loggers are created.
    """
    orig_cls = logging.getLoggerClass()
    logging.setLoggerClass(Logger)
    try:
        return logging.getLogger(*args, **kwargs)
    finally:
        logging.setLoggerClass(orig_cls)


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
    get_logger("law").addHandler(create_stream_handler())

    # set levels for all loggers and add the console handler for all non-law loggers
    cfg = Config.instance()
    for name, level in cfg.items("logging"):
        add_console_handler = not name.startswith("law.") and not get_tty_handlers(name)
        setup_logger(name, level, add_console_handler=add_console_handler, clear=False)


def setup_logger(name, level=None, add_console_handler=True, clear=False):
    """
    Sets up a logger given by its *name*, configures it to have a certain *level* and adds a
    preconfigured console handler when *add_console_handler* is *True*. When *add_console_handler*
    is a dictionary, it items are forwarded as keyword arguments to the
    :py:func:`create_stream_handler` which handles the handler setup internally.

    *name* can either be an integer or the name of a level present in the *logging* module. When no
    *level* is  given, the level of the ``"law"`` base logger is used as a default. When the logger
    already existed and *clear* is *True*, all handlers and filters are removed first. The logger
    object is returned.
    """
    # sanitize the level
    if isinstance(level, six.string_types):
        level = getattr(logging, level.upper(), None)
    if level is None:
        level = get_logger("law").level

    # clear handlers and filters
    is_existing = name in logging.root.manager.loggerDict
    logger = get_logger(name)
    if is_existing and clear:
        for h in list(logger.handlers):
            logger.removeHandler(h)
        for f in list(logger.filters):
            logger.removeFilter(f)

    # set the level
    logger.setLevel(level)

    # add a console handler
    if add_console_handler or isinstance(add_console_handler, dict):
        kwargs = add_console_handler if isinstance(add_console_handler, dict) else {}
        logger.addHandler(create_stream_handler(**kwargs))

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
        logger = get_logger(logger)
    return [handler for handler in getattr(logger, "handlers", []) if is_tty_handler(handler)]


class Logger(logging.Logger):
    """
    Custom logger class that adds an additional set of log methods, i.e., :py:meth:`debug_once`,
    :py:meth:`info_once`, :py:meth:`warning_once`, :py:meth:`error_once`, :py:meth:`critical_once`
    and :py:meth:`fatal_once`, that log certain messages only once depending on a string identifier.
    """

    def __init__(self, *args, **kwargs):
        super(Logger, self).__init__(*args, **kwargs)

        # names of logs per level that are issued only once
        self._once_logs = defaultdict(set)

    def debug_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["debug"]:
            self._once_logs["debug"].add(log_id)
            self.debug(*args, **kwargs)

    def info_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["info"]:
            self._once_logs["info"].add(log_id)
            self.info(*args, **kwargs)

    def warning_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["warning"]:
            self._once_logs["warning"].add(log_id)
            self.warning(*args, **kwargs)

    def error_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["error"]:
            self._once_logs["error"].add(log_id)
            self.error(*args, **kwargs)

    def critical_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["critical"]:
            self._once_logs["critical"].add(log_id)
            self.critical(*args, **kwargs)

    def fatal_once(self, log_id, *args, **kwargs):
        if log_id not in self._once_logs["fatal"]:
            self._once_logs["fatal"].add(log_id)
            self.fatal(*args, **kwargs)


class LogFormatter(logging.Formatter):
    """ __init__(*args, log_template=None, err_template=None, level_styles=None, name_styles=None, \
        msg_styles=None, format_level=None, format_name=None, format_msg=None, **kwargs)
    Configurable formatter class for colored logs. When set, *log_template*, *err_template*,
    *level_styles*, *name_styles*, *msg_styles*, *format_level*, *format_name* and *format_msg*
    control the log formats and styles on instance level. When *None*, they default to the upper
    case class level attributes described below. All *args* and *kwargs* are forwarded to
    :py:class:`logging.Formatter`.

    .. py:classattribute:: LOG_TEMPLATE
       type: string

       Template for log messages without stack traces.

    .. py:classattribute:: ERR_TEMPLATE
       type: string

       Template for log messages including stack traces.

    .. py:classattribute:: LEVEL_STYLES
       type: dict

       Style attributes forwarded to :py:func:`law.util.colored` per log level for styling level
       names in logs

    .. py:classattribute:: NAME_STYLES
       type: dict

       Style attributes forwarded to :py:func:`law.util.colored` per log level for styling logger
       names in logs.

    .. py:classattribute:: MSG_STYLES
       type: dict

       Style attributes forwarded to :py:func:`law.util.colored` per log level for styling messages
       in logs.

    .. py:classattribute:: FORMAT_LEVEL
       type: callable or None

       Custom callback to format the log level using the full record.

    .. py:classattribute:: FORMAT_NAME
       type: callable or None

       Custom callback to format the loger name using the full record.

    .. py:classattribute:: FORMAT_MSG
       type: callable or None

       Custom callback to format the log message using the full record.
    """

    LOG_TEMPLATE = "{level}: {name} - {msg}"
    ERR_TEMPLATE = "{level}: {name} - {msg}\n{traceback}"

    LEVEL_STYLES = {
        "DEBUG": {"color": "cyan"},
        "INFO": {"color": "green"},
        "WARNING": {"color": "yellow"},
        "ERROR": {"color": "red"},
        "CRITICAL": {"color": "red", "style": "bright"},
        "FATAL": {"color": "red", "style": "bright"},
    }
    NAME_STYLES = {}
    MSG_STYLES = {
        "WARNING": {"color": "yellow"},
        "ERROR": {"color": "red"},
        "CRITICAL": {"color": "red", "style": "bright"},
        "FATAL": {"color": "red", "style": "bright"},
    }

    FORMAT_LEVEL = None
    FORMAT_NAME = None
    FORMAT_MSG = None

    def __init__(self, *args, **kwargs):
        self.log_template = kwargs.pop("log_template", self.LOG_TEMPLATE)
        self.err_template = kwargs.pop("err_template", self.ERR_TEMPLATE)
        self.level_styles = kwargs.pop("level_styles", self.LEVEL_STYLES)
        self.name_styles = kwargs.pop("name_styles", self.NAME_STYLES)
        self.msg_styles = kwargs.pop("msg_styles", self.MSG_STYLES)
        self.format_level = kwargs.pop("format_level", self.FORMAT_LEVEL)
        self.format_name = kwargs.pop("format_name", self.FORMAT_NAME)
        self.format_msg = kwargs.pop("format_msg", self.FORMAT_MSG)

        super(LogFormatter, self).__init__(*args, **kwargs)

    def format(self, record):
        """"""
        # get and style the level
        level = self.format_level(record) if callable(self.format_level) else record.levelname
        level = colored(level, **self.level_styles.get(record.levelname, {}))

        # get and style the name
        name = self.format_name(record) if callable(self.format_name) else record.name
        name = colored(name, **self.name_styles.get(record.levelname, {}))

        # get and style the message
        msg = self.format_msg(record) if callable(self.format_msg) else record.getMessage()
        msg = colored(msg, **self.msg_styles.get(record.levelname, {}))

        # build template data
        tmpl = self.log_template
        data = dict(level=level, name=name, msg=msg)

        # add traceback and change the template when the record contains exception info
        if record.exc_info:
            tmpl = self.err_template
            data["traceback"] = self.formatException(record.exc_info)

        return tmpl.format(**data)


# trailing imports
from law.config import Config
