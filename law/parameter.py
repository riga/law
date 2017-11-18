# -*- coding: utf-8 -*-

"""
Custom luigi parameters.
"""


__all__ = ["NO_STR", "NO_INT", "NO_FLOAT",
           "TaskInstanceParameter", "CSVParameter"]


import luigi


# "no" value representations

NO_STR = "NO_STR"

NO_INT = -1

NO_FLOAT = -1.


# custom task classes

class TaskInstanceParameter(luigi.Parameter):

    def serialize(self, x):
        return str(x)


class CSVParameter(luigi.Parameter):

    def __init__(self, *args, **kwargs):
        cls = kwargs.pop("cls", luigi.Parameter)

        super(CSVParameter, self).__init__(*args, **kwargs)

        self.inst = cls()

    def parse(self, inp):
        if inp == NO_STR:
            return NO_STR
        else:
            return [self.inst.parse(elem) for elem in inp.split(",")]

    def serialize(self, value):
        if value == NO_STR:
            return NO_STR
        else:
            return ",".join(str(self.inst.serialize(elem)) for elem in value)
