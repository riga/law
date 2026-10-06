"""
CMS-related tasks. https://home.cern/about/experiments/cms
"""

from __future__ import annotations

__all__ = ["BundleCMSSW"]

import abc
import os
import pathlib
import subprocess

import luigi

from law.decorator import log
from law.parameter import NO_STR, CSVParameter
from law.target.file import FileSystemFileTarget, get_path
from law.target.local import LocalFileTarget
from law.task.base import Task
from law.util import interruptable_popen, quote_cmd, rel_path


class BundleCMSSW(Task):
    """
    Task that bundles a CMSSW checkout into a tarball, e.g. to send it along with jobs. Inheriting classes must
    implement :py:meth:`get_cmssw_path`. Files and directories relative to ``CMSSW_BASE`` can be excluded via the
    *exclude* regular expression, or included via *include*. The name of the output file contains a checksum of the
    checkout, unless :py:attr:`cmssw_checksumming` is *False*.

    .. py:classattribute:: cmssw_checksumming

        type: bool

        Whether a checksum of the checkout is computed and added to the output file name. Defaults
        to *True*.
    """

    task_namespace = "law.cms"

    exclude = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="regular expression for excluding files or directories relative to CMSSW_BASE; default: empty",
    )
    include = CSVParameter(
        default=(),
        significant=False,
        description="comma-separated list of files or directories relative to CMSSW_BASE to include; default: empty",
    )
    custom_checksum = luigi.Parameter(
        default=NO_STR,
        description="a custom checksum to use; default: empty",
    )

    cmssw_checksumming = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._checksum: str | None = None

    @abc.abstractmethod
    def get_cmssw_path(self) -> str | pathlib.Path | LocalFileTarget:
        """
        Hook that returns the path of the CMSSW checkout to bundle, i.e., its ``CMSSW_BASE`` directory. Must be
        implemented by inheriting classes.

        :return: The path.
        """
        ...

    @property
    def checksum(self) -> str | None:
        """
        The checksum of the CMSSW checkout, or the *custom_checksum* parameter when set. It is *None* when
        :py:attr:`cmssw_checksumming` is *False*.
        """
        if not self.cmssw_checksumming:
            return None

        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        if self._checksum is None:
            cmd = [
                rel_path(__file__, "scripts", "cmssw_checksum.sh"),
                get_path(self.get_cmssw_path()),
            ]
            if self.exclude != NO_STR:
                cmd += [self.exclude]
            _cmd = quote_cmd(cmd)

            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                _cmd,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
            )
            if code != 0:
                raise RuntimeError("cmssw checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self) -> FileSystemFileTarget:
        base = os.path.basename(get_path(self.get_cmssw_path()))
        if self.checksum:
            base += f".{self.checksum}"
        base = os.path.abspath(os.path.expandvars(os.path.expanduser(base)))
        return LocalFileTarget(f"{base}.tgz")

    @log
    def run(self) -> None:
        with self.output().localize("w") as tmp, self.publish_step("bundle CMSSW ..."):
            self.bundle(tmp.path)

    def get_cmssw_bundle_command(self, dst_path: str | pathlib.Path | LocalFileTarget) -> list[str]:
        """
        Returns the command that bundles the CMSSW checkout into *dst_path*.

        :param dst_path: The path of the bundle.
        :return: The command as a list of strings.
        """
        return [
            rel_path(__file__, "scripts", "bundle_cmssw.sh"),
            get_path(self.get_cmssw_path()),
            get_path(dst_path),
            self.exclude if self.exclude not in (None, NO_STR) else "",
            " ".join(self.include),
        ]

    def bundle(self, dst_path: str | pathlib.Path | LocalFileTarget) -> None:
        """
        Bundles the CMSSW checkout into a tarball at *dst_path*.

        :param dst_path: The path of the tarball.
        :raises RuntimeError: When the bundling failed.
        """
        cmd = self.get_cmssw_bundle_command(dst_path)
        code = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise RuntimeError("cmssw bundling failed")
