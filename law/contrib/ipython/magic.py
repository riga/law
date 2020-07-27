# coding: utf-8

"""
IPython magics for law.
"""


__all__ = ["register_magics"]


import shlex
import logging

import six

import law.cli
from law.util import law_run, quote_cmd


logger = logging.getLogger(__name__)


def create_magics(init_cmd=None, init_fn=None, line_cmd=None, line_fn=None, log_level=None):
    import IPython.core as ipc

    # prepare commands
    if init_cmd:
        init_cmd = quote_cmd(init_cmd) if isinstance(init_cmd, list) else init_cmd.strip()
    if line_cmd:
        line_cmd = quote_cmd(line_cmd) if isinstance(line_cmd, list) else line_cmd.strip()

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

            # run the init_cmd when set
            if init_cmd:
                logger.info("running initialization command '{}'".format(init_cmd))
                self._run_bash(init_cmd)

            # call the init_fn when set
            if callable(init_fn):
                logger.info("calling initialization function '{}'".format(init_fn.__name__))
                init_fn()

        @ipc.magic.line_magic
        def law(self, line):
            """
            Interprets the input *line* as command line arguments to the ``law`` executable and runs
            it in a subprocess using bash. Output and error streams are piped to the cell.
            """
            line = line.strip()
            if not line:
                logger.error(r"the command passed to %law must not be empty")
                return

            # build the full command
            cmd = "law " + line
            if line_cmd:
                cmd = "{} && {}".format(line_cmd, cmd)
            logger.debug("running law command '{}'".format(cmd))

            # run it
            self._run_bash(cmd)

        @ipc.magic.line_magic
        def ilaw(self, line):
            """
            Interprets the input *line* as command line arguments to the ``law`` executable, but
            rather than invoking it in a subprocess, it is evaluated interactively (or inline, thus
            the *i*) within the running process. This is especially useful for programmatically
            running tasks that were defined e.g. in the current notebook.
            """
            line = line.strip()
            if not line:
                logger.error(r"the command passed to %ilaw must not be empty")
                return

            argv = shlex.split(line)
            prog = argv.pop(0)

            # prog must be a valid law cli prog
            if prog not in law.cli.cli.progs:
                raise ValueError("'{}' is not a valid law cli program".format(prog))

            # forward to the actual prog, run special case
            try:
                # call the line_fn when set
                if callable(line_fn):
                    logger.info("calling line function '{}'".format(line_fn.__name__))
                    line_fn(line)

                if prog == "run":
                    law_run(argv)
                else:
                    law.cli.cli.run([prog] + argv)
            except SystemExit as e:
                # reraise when the exit code is non-zero
                if e.code:
                    raise

        def _run_bash(self, cmd):
            # build the command
            if not isinstance(cmd, six.string_types):
                cmd = quote_cmd(cmd)
            cmd = quote_cmd(["bash", "-c", cmd])

            # run it
            self.shell.system_piped(cmd)

    return LawMagics


def register_magics(*args, **kwargs):
    """ register_magics(init_cmd=None, init_fn=None, line_cmd=None, line_fn=None, log_level=None)
    Registers the two IPython magic methods ``%law`` and ``%ilaw`` which execute law commands either
    via a subprocess in bash (``%law``) or interactively / inline within the running process
    (``%ilaw``). *init_cmd* can be a shell command that is called before the magic methods are
    registered. Similarly, *init_fn* can be a callable that is invoked prior to the method setup.
    *line_cmd*, a shell command, and *line_fn*, a callable, are executed before a line magic is
    called. The former is run before ``%law`` is evaluated, while the latter is called before
    ``%ilaw`` with the line to interpret as the only argument. *log_level* conveniently sets the
    level of the *law.contrib.ipython.magic* logger that is used within the magic methods. It should
    be a number, or a string denoting a Python log level.
    """
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

        names = list(magics.magics["cell"].keys()) + list(magics.magics["line"].keys())
        names = ", ".join("%{}".format(name) for name in names)
        logger.info("magics successfully registered: {}".format(names))
    else:
        logger.error("no magics registered")
