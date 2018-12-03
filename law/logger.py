# -*- coding: utf-8 -*-

"""
Law logging setup.
"""


__all__ = ["console_handler", "setup_logging", "LogFormatter"]


import logging

from law.util import colored
from law.config import Config


#: Instance of a ``logging.StreamHandler`` that is used by logs in law. Its formatting is done by
#: :py:class:`LogFormatter`.
console_handler = None


def setup_logging():
    """
    Sets up the internal logging mechanism, i.e., it creates the :py:attr:`console_handler`, sets
    its formatting, and mounts on on the main ``"law"`` logger. It also sets the levels of all
    loggers that are given in the law config.
    """
    global console_handler

    # make sure logging is setup only once
    if console_handler:
        return

    # set the handler of the law root logger
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(LogFormatter())
    logging.getLogger("law").addHandler(console_handler)

    # set levels for all loggers
    for name, level in Config.instance().items("logging"):
        if hasattr(logging, level):
            logger = logging.getLogger(name)
            logger.setLevel(getattr(logging, level))
            logger.info("registered logger with level '{}'".format(level))


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
        return self.tmpl.format(
            level=colored(record.levelname, **self.level_styles.get(record.levelname, {})),
            name=record.name,
            msg=record.msg,
        )
