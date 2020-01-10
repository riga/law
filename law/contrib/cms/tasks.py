# coding: utf-8

"""
CMS-related tasks.
https://home.cern/about/experiments/cms
"""


__all__ = ["BundleCMSSW"]


import os
from abc import abstractmethod

import luigi


from law.task.base import Task
from law.target.file import get_path
from law.target.local import LocalFileTarget
from law.parameter import NO_STR
from law.decorator import log
from law.util import rel_path, interruptable_popen


class BundleCMSSW(Task):

    task_namespace = "law.cms"

    exclude = luigi.Parameter(default=NO_STR, description="regular expression for excluding files "
        "or directories, relative to CMSSW_BASE, default: empty")

    @abstractmethod
    def get_cmssw_path(self):
        return

    def output(self):
        return LocalFileTarget("{}.tgz".format(os.path.basename(self.get_cmssw_path())))

    @log
    def run(self):
        with self.output().localize("w") as tmp:
            self.bundle(tmp.path)

    def bundle(self, dst_path):
        bundle_script = rel_path(__file__, "scripts", "bundle_cmssw.sh")
        cmd = [bundle_script, self.get_cmssw_path(), get_path(dst_path)]
        if self.exclude != NO_STR:
            cmd += [self.exclude]

        code = interruptable_popen(cmd)[0]
        if code != 0:
            raise Exception("cmssw bundling failed")
