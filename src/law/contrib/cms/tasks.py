# coding: utf-8

"""
CMS-related tasks.
https://home.cern/about/experiments/cms
"""

from __future__ import annotations

__all__ = ["BundleCMSSW"]

import os
import subprocess
import pathlib
import abc

import luigi  # type: ignore[import-untyped]

from law.task.base import Task
from law.target.file import get_path, FileSystemFileTarget
from law.target.local import LocalFileTarget
from law.parameter import NO_STR, CSVParameter
from law.decorator import log
from law.util import rel_path, interruptable_popen, quote_cmd


class BundleCMSSW(Task):

    task_namespace = "law.cms"

    exclude = luigi.Parameter(
        default=NO_STR,
        significant=False,
        description="regular expression for excluding files or directories relative to CMSSW_BASE; "
        "default: empty",
    )
    include = CSVParameter(
        default=(),
        significant=False,
        description="comma-separated list of files or directories relative to CMSSW_BASE to "
        "include; default: empty",
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
        ...

    @property
    def checksum(self) -> None | str:
        if not self.cmssw_checksumming:
            return None

        if self.custom_checksum != NO_STR:
            return self.custom_checksum  # type: ignore[return-value]

        if self._checksum is None:
            cmd = [
                rel_path(__file__, "scripts", "cmssw_checksum.sh"),
                get_path(self.get_cmssw_path()),
            ]
            if self.exclude != NO_STR:
                cmd += [self.exclude]  # type: ignore[list-item]
            _cmd = quote_cmd(cmd)

            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                _cmd,
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
            )
            if code != 0:
                raise Exception("cmssw checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self) -> FileSystemFileTarget:
        base = os.path.basename(get_path(self.get_cmssw_path()))
        if self.checksum:
            base += f"{self.checksum}."
        base = os.path.abspath(os.path.expandvars(os.path.expanduser(base)))
        return LocalFileTarget(f"{base}.tgz")

    @log
    def run(self) -> None:
        with self.output().localize("w") as tmp:
            with self.publish_step("bundle CMSSW ..."):
                self.bundle(tmp.path)

    def get_cmssw_bundle_command(self, dst_path: str | pathlib.Path | LocalFileTarget) -> list[str]:
        return [
            rel_path(__file__, "scripts", "bundle_cmssw.sh"),
            get_path(self.get_cmssw_path()),
            get_path(dst_path),
            self.exclude if self.exclude not in (None, NO_STR) else "",  # type: ignore[list-item]
            " ".join(self.include),  # type: ignore[arg-type]
        ]

    def bundle(self, dst_path: str | pathlib.Path | LocalFileTarget) -> None:
        cmd = self.get_cmssw_bundle_command(dst_path)
        code = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("cmssw bundling failed")
