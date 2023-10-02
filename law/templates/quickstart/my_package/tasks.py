# coding: utf-8

"""
Location of tasks.
"""

import law


class MyTask(law.Task):

    def output(self):
        return law.LocalFileTarget("$QS_DATA/output.txt")

    def run(self):
        self.output().dump("output of {!r}".format(self))
