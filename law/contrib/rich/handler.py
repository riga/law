# coding: utf-8

"""
Set console logging handlers to rich logging handlers.
"""


__all__ = ["set_handlers"]

import logging

from law.util import multi_match


def set_handlers(scopes=("luigi.*", "luigi-*", "law.*")):
    from rich import logging as rich_logging

    for name, logger in logging.root.manager.loggerDict.items():
        if any(multi_match(name, scope) for scope in scopes):
            if getattr(logger, "handlers", None) is not None:
                level = logger.level
                replace_handler = False
                for hdlr in logger.handlers:
                    if isinstance(hdlr, logging.StreamHandler) and hdlr.stream.isatty():
                        if getattr(hdlr, "level", None) is not None:
                            level = hdlr.level

                        # remove old handler
                        logger.removeHandler(hdlr)
                        replace_handler = True

                if replace_handler:
                    # create and add new handler
                    new_hdlr = rich_logging.RichHandler(level)
                    logger.addHandler(new_hdlr)


#
#
# rename file as logging
#
# doc eintrag
