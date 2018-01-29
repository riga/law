# -*- coding: utf-8 -*-

"""
Tasks that provide common and often used functionality.
"""


__all__ = ["TransferLocalFile"]


import os
from abc import abstractmethod

import luigi

from law.task.base import Task
from law.target.local import LocalFileTarget
from law.target.collection import SiblingFileCollection
from law.decorator import log


class TransferLocalFile(Task):

    source_path = luigi.Parameter(description="path to the file to transfer")
    replicas = luigi.IntParameter(default=0, description="number of replicas to generate, uses "
        "replica_format when > 0 for creating target basenames, default: 0")

    replica_format = "{name}.{i}{ext}"

    exclude_db = True

    def get_source_target(self):
        # when self.source_path is set, return a target around it
        # otherwise assume self.requires() returns a task with a single local target
        return LocalFileTarget(self.soure_path) if self.soure_path else self.input()

    @abstractmethod
    def single_output(self):
        pass

    def output(self):
        output = self.single_output()
        if self.replicas <= 0:
            return output

        # prepare replica naming
        name, ext = os.path.splitext(output.basename)
        basename = lambda i: self.replica_format.format(name=name, ext=ext, i=i)

        # return the replicas in a SiblingFileCollection
        output_dir = output.parent
        return SiblingFileCollection([
            output_dir.child(basename(i), "f") for i in range(self.replicas)
        ])

    @log
    def run(self):
        self.transfer(self.get_source_target())

    def transfer(self, src_path):
        output = self.output()

        # single output or replicas?
        if not isinstance(output, SiblingFileCollection):
            output.copy_from_local(src_path, cache=False)
        else:
            # upload all replicas
            progress_callback = self.create_progress_callback(self.replicas)
            for i, replica in enumerate(output.targets):
                replica.copy_from_local(src_path, cache=False)
                progress_callback(i)
