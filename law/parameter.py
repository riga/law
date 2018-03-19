# -*- coding: utf-8 -*-

"""
Custom luigi parameters.
"""


__all__ = ["NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
           "CSVParameter"]


import luigi


#: String value denoting an empty parameter.
NO_STR = "NO_STR"

#: Integer value denoting an empty parameter.
NO_INT = -1

#: Float value denoting an empty parameter.
NO_FLOAT = -1.


def is_no_param(value):
    """
    Checks whether a parameter *value* denotes an empty parameter, i.e., if the value is either
    :py:attr:`NO_STR`, :py:attr:`NO_INT` or :py:attr:`NO_FLOAT`.
    """
    return value in (NO_STR, NO_INT, NO_FLOAT)


def get_param(value, default=None):
    """
    Returns the passed *value* when it does not refer to an empty parameter value, checked with
    :py:func:`is_no_param`. Otherwise, *default* is returned, which defaults to *None*.
    """
    return default if is_no_param(value) else value


class TaskInstanceParameter(luigi.Parameter):
    """
    Parameter that can be used pass the instance of a task. This class does not implement parameter
    value parsing.
    """

    def serialize(self, x):
        """"""
        return str(x)


class CSVParameter(luigi.Parameter):
    """
    Parameter that can be parsed from a comma-separated value (CSV). *cls* can refer to an other
    luigi parameter class that will be used to parse and serialize the particular items. Example:

    .. code-block:: python

        p = CSVParameter(cls=luigi.IntParameter, default=[1, 2, 3])

        p.parse("4,5,6")
        # => [4, 5, 6]

        p.serialize([7, 8, 9])
        # => "7,8,9"

    .. py:attribute:: _inst
       type: cls

        Instance of the luigi parameter class *cls* that is used internally for parameter parsing
        and serialization.
    """

    def __init__(self, *args, **kwargs):
        """ __init__(cls=luigi.Parameter, *args, **kwargs) """
        cls = kwargs.pop("cls", luigi.Parameter)

        super(CSVParameter, self).__init__(*args, **kwargs)

        self._inst = cls()

    def parse(self, inp):
        """"""
        if not inp:
            return []
        else:
            return [self._inst.parse(elem) for elem in inp.split(",")]

    def serialize(self, value):
        """"""
        if not value:
            return ""
        else:
            return ",".join(str(self._inst.serialize(elem)) for elem in value)
