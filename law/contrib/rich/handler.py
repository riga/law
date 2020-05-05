# coding: utf-8

"""
Rich logging handlers.
"""


__all__ = ["RichHandler"]


import logging

import luigi
from rich import logging as rich_logging


class RichHandler:

    name = "rich_handler"

    @classmethod
    def set_handlers(self, scopes=["luigi", "law"]):
        for name, logger in logging.root.manager.loggerDict.items():
            if any(name.startswith(scope) for scope in scopes):
                try:
                    for hdlr in logger.handlers:
                        # remove old handler
                        logger.removeHandler(hdlr)

                        # create and add new handler
                        new_hdlr = rich_logging.RichHandler(logger.level)
                        logger.addHandler(new_hdlr)
                except AttributeError:
                    # pass if logger has no handlers
                    pass
