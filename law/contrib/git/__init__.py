# -*- coding: utf-8 -*-

"""
Git-related tasks.
"""


__all__ = ["BundleGitRepository"]


import os
import subprocess

import luigi

from law.task.base import Task
from law.target.local import LocalFileTarget
from law.parameter import CSVParameter, NO_STR
from law.decorator import log
from law.util import rel_path, interruptable_popen


class BundleGitRepository(Task):

    path = luigi.Parameter(description="the path to the repository to bundle")
    exclude = CSVParameter(default=[], description="patterns of files to exclude")
    force_include = CSVParameter(default=[], description="patterns of files to force-include, "
        "takes precedence over .gitignore")
    custom_checksum = luigi.Parameter(default=NO_STR, description="a custom checksum to use")

    def __init__(self, *args, **kwargs):
        super(BundleGitRepository, self).__init__(*args, **kwargs)

        self.path = os.path.expandvars(os.path.expanduser(os.path.abspath(self.path)))
        self._checksum = None

    @property
    def checksum(self):
        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        if self._checksum is None:
            cmd = [rel_path(__file__, "repository_checksum.sh"), self.path]

            code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE)
            if code != 0:
                raise Exception("repository checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self):
        return LocalFileTarget("{}_{}.tgz".format(os.path.basename(self.path), self.checksum))

    @log
    def run(self):
        with self.output().localize("w") as tmp:
            self.bundle(tmp.path)

    def bundle(self, dst_path):
        cmd = [rel_path(__file__, "bundle_repository.sh"), self.path, dst_path]
        cmd += [" ".join(self.exclude)]
        cmd += [" ".join(self.force_include)]

        code = interruptable_popen(cmd)[0]
        if code != 0:
            raise Exception("repository bundling failed")
