# coding: utf-8

"""
CMS-related tasks.
https://home.cern/about/experiments/cms
"""


__all__ = ["BundleCMSSW"]


import os
import subprocess
from abc import abstractmethod

import luigi


from law.task.base import Task
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.parameter import NO_STR
from law.decorator import log
from law.util import rel_path, interruptable_popen, quote_cmd


class BundleCMSSW(Task):

    task_namespace = "law.cms"

    exclude = luigi.Parameter(default=NO_STR, description="regular expression for excluding files "
        "or directories, relative to CMSSW_BASE, default: empty")
    custom_checksum = luigi.Parameter(default=NO_STR, description="a custom checksum to use, "
        "default: NO_STR")

    cmssw_checksumming = True

    def __init__(self, *args, **kwargs):
        super(BundleCMSSW, self).__init__(*args, **kwargs)

        self._checksum = None

    @abstractmethod
    def get_cmssw_path(self):
        return

    @property
    def checksum(self):
        if not self.cmssw_checksumming:
            return None

        if self.custom_checksum != NO_STR:
            return self.custom_checksum

        if self._checksum is None:
            cmd = [rel_path(__file__, "scripts", "cmssw_checksum.sh"), self.get_cmssw_path()]
            if self.exclude != NO_STR:
                cmd += [self.exclude]
            cmd = quote_cmd(cmd)

            code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
                stdout=subprocess.PIPE)
            if code != 0:
                raise Exception("cmssw checksum calculation failed")

            self._checksum = out.strip()

        return self._checksum

    def output(self):
        base = os.path.basename(self.get_cmssw_path())
        if self.checksum:
            base += "{}.".format(self.checksum)
        return LocalFileTarget("{}.tgz".format(base))

    @log
    def run(self):
        with self.output().localize("w") as tmp:
            self.bundle(tmp.path)

    def bundle(self, dst_path):
        cmd = [
            rel_path(__file__, "scripts", "bundle_cmssw.sh"),
            self.get_cmssw_path(),
            get_path(dst_path),
        ]
        if self.exclude != NO_STR:
            cmd += [self.exclude]
        cmd = quote_cmd(cmd)

        code = interruptable_popen(cmd, shell=True, executable="/bin/bash")[0]
        if code != 0:
            raise Exception("cmssw bundling failed")
