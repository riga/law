# -*- coding: utf-8 -*-

"""
Law logging setup.
"""


__all__ = ["console_handler", "setup_logging", "LogFormatter"]


import logging

from law.util import colored
from law.config import Config


console_handler = None


def setup_logging():
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

    tmpl = "{level}{spaces}: {name} - {msg}"

    level_styles = {
        "NOTSET": {},
        "DEBUG": {"color": "cyan"},
        "INFO": {"color": "green"},
        "WARNING": {"color": "yellow"},
        "ERROR": {"color": "red"},
        "CRITICAL": {"color": "red", "style": "bright"},
    }

    max_level_len = max(map(len, level_styles))

    def format(self, record):
        return self.tmpl.format(
            level=colored(record.levelname, **self.level_styles.get(record.levelname, {})),
            spaces=" " * (self.max_level_len - len(record.levelname)),
            name=record.name,
            msg=record.msg,
        )
