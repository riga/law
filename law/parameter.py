# -*- coding: utf-8 -*-

"""
Custom luigi parameters.
"""


__all__ = ["NO_STR", "NO_INT", "NO_FLOAT",
           "TaskInstanceParameter"]


import luigi


# "no" value representations

NO_STR = "NO_STR"

NO_INT = -1

NO_FLOAT = -1.


# custom task classes

class TaskInstanceParameter(luigi.Parameter):

    def serialize(self, x):
        return str(x)
