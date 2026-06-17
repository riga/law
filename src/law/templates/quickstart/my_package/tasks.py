# coding: utf-8

"""
Location of tasks.
"""

import law


class MyTask(law.Task):

    def output(self) -> law.Target:
        return law.LocalFileTarget("$QS_DATA/output.txt")

    def run(self) -> None:
        self.output().dump(f"output of {self!r}")
