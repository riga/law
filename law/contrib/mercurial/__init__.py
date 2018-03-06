# -*- coding: utf-8 -*-

"""
Mercurial-related tasks.
"""


__all__ = ["BundleMercurialRepository"]


import os
import subprocess

import luigi

from law import Task, LocalFileTarget, CSVParameter, NO_STR
from law.target.file import get_path
from law.decorator import log
from law.util import rel_path, interruptable_popen


class BundleMercurialRepository(Task):

    task_namespace = "law.mercurial"

    repo_path = luigi.Parameter(description="the path to the repository to bundle")
    exclude_files = CSVParameter(default=[], description="patterns of files to exclude")
    include_files = CSVParameter(default=[], description="patterns of files to force-include, "
        "takes precedence over .hgignore")
    custom_checksum = luigi.Parameter(default=NO_STR, description="a custom checksum to use")

    def __init__(self, *args, **kwargs):
        super(BundleMercurialRepository, self).__init__(*args, **kwargs)

        self.repo_path = os.path.expandvars(os.path.expanduser(os.path.abspath(self.repo_path)))
        self._checksum = None

    @property
    def checksum(self):
        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        if self._checksum is None:
            cmd = [rel_path(__file__, "repository_checksum.sh"), self.repo_path]

            code, out, _ = interruptable_popen(cmd, stdout=subprocess.PIPE)
            if code != 0:
                raise Exception("repository checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self):
        return LocalFileTarget("{}_{}.tgz".format(os.path.basename(self.repo_path), self.checksum))

    @log
    def run(self):
        with self.output().localize("w") as tmp:
            self.bundle(tmp.path)

    def bundle(self, dst_path):
        cmd = [rel_path(__file__, "bundle_repository.sh"), self.repo_path, get_path(dst_path)]
        cmd += [" ".join(self.exclude_files)]
        cmd += [" ".join(self.include_files)]

        code = interruptable_popen(cmd)[0]
        if code != 0:
            raise Exception("repository bundling failed")
