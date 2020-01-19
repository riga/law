# coding: utf-8

"""
Law logging setup.
"""


__all__ = ["console_handler", "setup_logging", "LogFormatter"]


import logging

from law.config import Config
from law.util import colored


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
    for name, level in Config.instance().items("logging"):
        level = level.upper()
        if hasattr(logging, level):
            # create / get the logger and set the level
            logger = logging.getLogger(name)
            logger.setLevel(getattr(logging, level))

            # add the console handler when not part of the law.* namespace
            if not name.startswith("law."):
                logger.addHandler(console_handler)

            logger.debug("registered logger with level '{}'".format(level))


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
