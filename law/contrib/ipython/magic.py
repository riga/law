# coding: utf-8

"""
IPython magics for law.
"""


__all__ = ["register_magics"]


import logging

import six
import IPython.core as ipc

from law.util import quote_cmd


logger = logging.getLogger(__name__)


def create_magics(init_cmd=None, line_cmd=None, log_level=None):
    # prepare commands
    init_cmd = quote_cmd(init_cmd) if isinstance(init_cmd, list) else init_cmd
    line_cmd = quote_cmd(line_cmd) if isinstance(line_cmd, list) else line_cmd

    # set the log level
    if isinstance(log_level, six.string_types):
        log_level = getattr(logging, log_level.upper(), None)
    if isinstance(log_level, six.integer_types + (float,)):
        logger.setLevel(log_level)
        logger.debug("log level set to {}".format(log_level))

    @ipc.magic.magics_class
    class LawMagics(ipc.magic.Magics):

        def __init__(self, *args, **kwargs):
            super(LawMagics, self).__init__(*args, **kwargs)

            # run the initial command when set
            if init_cmd:
                logger.info("running initial command '{}'".format(init_cmd))
                self._run_bash(init_cmd)

        @ipc.magic.line_magic
        def law(self, line):
            # build the full command
            cmd = "law " + line
            if line_cmd:
                cmd = "{} && {}".format(line_cmd, cmd)
            logger.debug("running law command '{}'".format(cmd))

            # run it
            self._run_bash(cmd)

        def _run_bash(self, cmd):
            # build the command
            if not isinstance(cmd, six.string_types):
                cmd = quote_cmd(cmd)
            cmd = quote_cmd(["bash", "-c", cmd])

            # run it
            self.shell.system_piped(cmd)

    return LawMagics


def register_magics(*args, **kwargs):
    ipy = None
    magics = None

    try:
        ipy = get_ipython()
    except NameError:
        logger.error("no running notebook kernel found")

    # create the magics
    if ipy:
        magics = create_magics(*args, **kwargs)

    # register it
    if ipy and magics:
        ipy.register_magics(magics)
        logger.debug("magics successfully registered")
    else:
        logger.error("no magics registered")
