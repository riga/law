"""
Git-related tasks.
"""

from __future__ import annotations

__all__ = ["BundleGitRepository"]

import abc
import os
import pathlib
import subprocess
import threading

import luigi

from law.decorator import log
from law.parameter import NO_STR, CSVParameter
from law.target.file import FileSystemFileTarget, get_path
from law.target.local import LocalFileTarget
from law.task.base import Task
from law.util import interruptable_popen, quote_cmd, rel_path


class BundleGitRepository(Task):

    task_namespace = "law.git"

    exclude_files = CSVParameter(
        default=(),
        description="patterns of files to exclude, default: ()",
    )
    include_files = CSVParameter(
        default=(),
        description="patterns of files to force-include, takes precedence over .gitignore, "
        "default: ()",
    )
    custom_checksum = luigi.Parameter(
        default=NO_STR,
        description="a custom checksum to use, default: NO_STR",
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self._checksum: str | None = None
        self._checksum_lock = threading.RLock()

    @abc.abstractmethod
    def get_repo_path(self) -> str | pathlib.Path | FileSystemFileTarget:
        ...

    @property
    def checksum(self) -> str:
        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        with self._checksum_lock:
            if self._checksum is None:
                cmd = quote_cmd([
                    rel_path(__file__, "scripts", "repository_checksum.sh"),
                    get_path(self.get_repo_path()),
                    "1",  # recursive flag
                    " ".join(self.include_files),
                    " ".join(self.exclude_files),
                ])

                out: str
                code, out, _ = interruptable_popen(  # type: ignore[assignment]
                    cmd,
                    shell=True,
                    executable="/bin/bash",
                    stdout=subprocess.PIPE,
                )
                if code != 0:
                    raise Exception("repository checksum calculation failed")

                self._checksum = out.strip()

        return self._checksum

    def output(self) -> FileSystemFileTarget:
        repo_base = os.path.basename(get_path(self.get_repo_path()))
        repo_base = os.path.abspath(os.path.expandvars(os.path.expanduser(repo_base)))
        return LocalFileTarget(f"{repo_base}.{self.check_sum}.tgz")  # type: ignore[attr-defined]

    @log
    def run(self) -> None:
        with self.output().localize("w") as tmp, self.publish_step("bundle git repository ..."):
            self.bundle(tmp.path)

    def bundle(self, dst_path: str | pathlib.Path | FileSystemFileTarget) -> None:
        # helper to filter out files that are not actually present in the repository
        repo_path = os.path.abspath(os.path.expandvars(os.path.expanduser(str(self.get_repo_path()))))
        is_in_repo = lambda p: not os.path.relpath(os.path.join(str(p)), repo_path).startswith("..")

        # build the command
        cmd: list | str
        cmd = [
            rel_path(__file__, "scripts", "bundle_repository.sh"),
            get_path(repo_path),
            get_path(dst_path),
            " ".join(filter(is_in_repo, self.include_files)),
            " ".join(filter(is_in_repo, self.exclude_files)),
        ]
        cmd = quote_cmd(cmd)

        code = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("repository bundling failed")
