# -*- coding: utf-8 -*-

"""
Custom luigi parameters.
"""


__all__ = ["NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
           "CSVParameter"]


import luigi


# "no" value representations

NO_STR = "NO_STR"

NO_INT = -1

NO_FLOAT = -1.


def is_no_param(value):
    return value in (NO_STR, NO_INT, NO_FLOAT)


def get_param(value, default=None):
    return default if is_no_param(value) else value


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
        if not inp:
            return []
        else:
            return [self.inst.parse(elem) for elem in inp.split(",")]

    def serialize(self, value):
        if not value:
            return ""
        else:
            return ",".join(str(self.inst.serialize(elem)) for elem in value)
