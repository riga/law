# coding: utf-8

"""
Git-related tasks.
"""


__all__ = ["BundleGitRepository"]


import os
import subprocess
from abc import abstractmethod

import luigi

from law.task.base import Task
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.parameter import NO_STR, CSVParameter
from law.decorator import log
from law.util import rel_path, interruptable_popen, quote_cmd


class BundleGitRepository(Task):

    task_namespace = "law.git"

    exclude_files = CSVParameter(default=(), description="patterns of files to exclude, default: "
        "()")
    include_files = CSVParameter(default=(), description="patterns of files to force-include, "
        "takes precedence over .gitignore, default: ()")
    custom_checksum = luigi.Parameter(default=NO_STR, description="a custom checksum to use, "
        "default: NO_STR")

    def __init__(self, *args, **kwargs):
        super(BundleGitRepository, self).__init__(*args, **kwargs)

        self._checksum = None

    @abstractmethod
    def get_repo_path(self):
        return

    @property
    def checksum(self):
        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        if self._checksum is None:
            cmd = [rel_path(__file__, "scripts", "repository_checksum.sh"), self.get_repo_path()]
            cmd = quote_cmd(cmd)

            code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE)
            if code != 0:
                raise Exception("repository checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self):
        repo_base = os.path.basename(self.get_repo_path())
        return LocalFileTarget("{}.{}.tgz".format(repo_base, self.checksum))

    @log
    def run(self):
        with self.output().localize("w") as tmp:
            self.bundle(tmp.path)

    def bundle(self, dst_path):
        cmd = [
            rel_path(__file__, "scripts", "bundle_repository.sh"),
            self.get_repo_path(),
            get_path(dst_path),
            " ".join(self.exclude_files),
            " ".join(self.include_files),
        ]
        cmd = quote_cmd(cmd)

        code = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("repository bundling failed")
