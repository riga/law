# coding: utf-8

"""
IPython magics for law.
"""

from __future__ import annotations

__all__ = ["register_magics"]

import shlex
import logging

import law.cli
from law.util import law_run, quote_cmd
from law.logger import get_logger
from law._types import Callable, Sequence, Any


logger = get_logger(__name__)


def create_magics(
    init_cmd: str | Sequence[str] | None = None,
    init_fn: Callable | None = None,
    line_cmd: str | Sequence[str] | None = None,
    line_fn: Callable[[str], Any] | None = None,
    log_level: int | str | None = None,
) -> type:
    import IPython.core as ipc  # type: ignore[import-untyped, import-not-found]

    # prepare commands
    if init_cmd:
        init_cmd = init_cmd.strip() if isinstance(init_cmd, str) else quote_cmd(init_cmd)
    if line_cmd:
        line_cmd = line_cmd.strip() if isinstance(line_cmd, str) else quote_cmd(line_cmd)

    # set the log level
    if isinstance(log_level, str):
        log_level = getattr(logging, log_level.upper(), None)
    if isinstance(log_level, int):
        logger.setLevel(log_level)
        logger.debug(f"log level set to {log_level}")

    @ipc.magic.magics_class
    class LawMagics(ipc.magic.Magics):

        def __init__(self, *args, **kwargs) -> None:
            super().__init__(*args, **kwargs)

            # run the init_cmd when set
            if init_cmd:
                logger.info(f"running initialization command '{init_cmd}'")
                self._run_bash(init_cmd)

            # call the init_fn when set
            if callable(init_fn):
                logger.info(f"calling initialization function '{init_fn.__name__}'")
                init_fn()

        @ipc.magic.line_magic
        def law(self, line: str) -> Any:
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
                cmd = f"{line_cmd} && {cmd}"
            logger.debug(f"running law command '{cmd}'")

            # run it
            return self._run_bash(cmd)

        @ipc.magic.line_magic
        def ilaw(self, line: str) -> int:
            """
            Interprets the input *line* as command line arguments to the ``law`` executable, but
            rather than invoking it in a subprocess, it is evaluated interactively (or inline, thus
            the *i*) within the running process. This is especially useful for programmatically
            running tasks that were defined e.g. in the current notebook.
            """
            line = line.strip()
            if not line:
                logger.error(r"the command passed to %ilaw must not be empty")
                return -1

            argv = shlex.split(line)
            prog = argv.pop(0)

            # prog must be a valid law cli prog
            if prog not in law.cli.cli.progs:
                raise ValueError(f"'{prog}' is not a valid law cli program")

            # forward to the actual prog, run special case
            try:
                # call the line_fn when set
                if callable(line_fn):
                    logger.info(f"calling line function '{line_fn.__name__}'")
                    line_fn(line)

                if prog == "run":
                    # perform the run call interactively
                    return law_run(argv)
                # forward all other progs to the cli interface
                return law.cli.cli.run([prog] + argv)

            except SystemExit as e:
                # reraise when the exit code is non-zero
                if e.code:
                    raise
                return 0

        def _run_bash(self, cmd: str | Sequence[str]) -> Any:
            # build the command
            if not isinstance(cmd, str):
                cmd = quote_cmd(cmd)
            cmd = quote_cmd(["bash", "-c", cmd])

            # run it
            return self.shell.system_piped(cmd)  # type: ignore[attr-defined, union-attr]

    return LawMagics


def register_magics(*args, **kwargs) -> None:
    """ register_magics(init_cmd=None, init_fn=None, line_cmd=None, line_fn=None, log_level=None)
    Registers the two IPython magic methods ``%law`` and ``%ilaw`` which execute law commands either
    via a subprocess in bash (``%law``) or interactively / inline within the running process
    (``%ilaw``).

    *init_cmd* can be a shell command that is called before the magic methods are registered.
    Similarly, *init_fn* can be a callable that is invoked prior to the method setup. *line_cmd*, a
    shell command, and *line_fn*, a callable, are executed before a line magic is called. The former
    is run before ``%law`` is evaluated, while the latter is called before ``%ilaw`` with the line
    to interpret as the only argument.

    *log_level* conveniently sets the level of the *law.contrib.ipython.magic* logger that is used
    within the magic methods. It should be a number, or a string denoting a Python log level.
    """
    ipy = None
    magics = None

    try:
        ipy = get_ipython()  # type: ignore[name-defined]
    except NameError:
        logger.error("no running notebook kernel found")

    # create the magics
    if ipy is not None:
        magics = create_magics(*args, **kwargs)

    # register it
    if ipy is not None and magics is not None:
        ipy.register_magics(magics)

        names = list(magics.magics["cell"].keys()) + list(magics.magics["line"].keys())  # type: ignore[attr-defined] # noqa
        names_str = ", ".join(f"%{name}" for name in names)
        logger.info(f"magics successfully registered: {names_str}")
    else:
        logger.error("no magics registered")
