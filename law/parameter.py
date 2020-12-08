# coding: utf-8

"""
Custom luigi parameters.
"""


__all__ = [
    "NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
    "DurationParameter", "CSVParameter", "MultiCSVParameter", "NotifyParameter",
    "NotifyMultiParameter", "NotifyMailParameter",
]


import luigi
import six

from law.notification import notify_mail
from law.util import (
    human_duration, parse_duration, time_units, time_unit_aliases, is_lazy_iterable, make_tuple,
    make_unique, brace_expand,
)


# make luigi's BoolParameter parsing explicit globally, https://github.com/spotify/luigi/pull/2427
luigi.BoolParameter.parsing = getattr(luigi.BoolParameter, "EXPLICIT_PARSING", "explicit")

# also update existing BoolParameter instances in luigi's config classes to have explicit parsing
for cls in luigi.task.Config.__subclasses__():
    for attr in dir(cls):
        member = getattr(cls, attr)
        if isinstance(member, luigi.BoolParameter):
            member.parsing = luigi.BoolParameter.parsing

#: String value denoting an empty parameter.
NO_STR = "NO_STR"

#: Integer value denoting an empty parameter.
NO_INT = -1

#: Float value denoting an empty parameter.
NO_FLOAT = -1.


def is_no_param(value):
    """
    Checks whether a parameter *value* denotes an empty parameter, i.e., if the value is either
    :py:attr:`NO_STR`, :py:attr:`NO_INT`, or :py:attr:`NO_FLOAT`.
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


class DurationParameter(luigi.Parameter):
    """ __init__(unit="s", *args, **kwargs)
    Parameter that interprets a string (or float) value as a duration, represented by a float
    number with a configurable unit. *unit* is forwarded as both the *unit* and *input_unit*
    argument to :py:func:`law.util.parse_duration` which is used for the conversion. For best
    precision, value serialization uses :py:func:`law.util.human_duration` with *colon_format*.
    Example:

    .. code-block:: python

        p = DurationParameter(unit="s")
        p.parse("5")                      # -> 5. (using the unit implicitly)
        p.parse("5s")                     # -> 5.
        p.parse("5m")                     # -> 300.
        p.parse("05:10")                  # -> 310.
        p.parse("5 minutes, 15 seconds")  # -> 310.
        p.serialize(310)                  # -> 05:15

        p = DurationParameter(unit="m")
        p.parse("5")                      # -> 5. (using the unit implicitly)
        p.parse("5s")                     # -> 0.083
        p.parse("5m")                     # -> 5.
        p.parse("05:10")                  # -> 5.167
        p.parse("5 minutes, 15 seconds")  # -> 5.25
        p.serialize(310)                  # -> 05:15

    For more info, see :py:func:`law.util.parse_duration` and :py:func:`law.util.human_duration`.
    """

    def __init__(self, *args, **kwargs):
        # validate and set the unit
        self._unit = None
        self.unit = kwargs.pop("unit", "s")

        super(DurationParameter, self).__init__(*args, **kwargs)

    @property
    def unit(self):
        return self._unit

    @unit.setter
    def unit(self, unit):
        unit = time_unit_aliases.get(unit, unit)
        if unit not in time_units:
            raise ValueError("unknown unit '{}', valid values are {}".format(
                unit, time_units.keys()))

        self._unit = unit

    def parse(self, inp):
        """"""
        if not inp:
            return 0.
        else:
            return parse_duration(inp, input_unit=self.unit, unit=self.unit)

    def serialize(self, value):
        """"""
        if not value:
            return "0"
        else:
            value_seconds = parse_duration(value, input_unit=self.unit, unit="s")
            return human_duration(seconds=value_seconds, colon_format=True)


class CSVParameter(luigi.Parameter):
    """ __init__(*args, cls=luigi.Parameter, unique=False, min_len=None, max_len=None, \
        brace_expand=False, x=y, **kwargs)
    Parameter that parses a comma-separated value (CSV) and produces a tuple. *cls* can refer to an
    other parameter class that will be used to parse and serialize the particular items.

    When *unique* is *True*, both parsing and serialization methods make sure that values are
    unique.

    When *min_len* (*max_len*) is set to an integer, an error is raised in case the number of
    elements to serialize or parse is deceeds (exceeds) that value.

    When *brace_expand* is *True*, brace expansion is applied, potentially extending the list of
    values.

    Example:

    .. code-block:: python

        p = CSVParameter(cls=luigi.IntParameter)
        p.parse("4,5,6,6")
        # => (4, 5, 6, 6)
        p.serialize((7, 8, 9))
        # => "7,8,9"

        p = CSVParameter(cls=luigi.IntParameter, unique=True)
        p.parse("4,5,6,6")
        # => (4, 5, 6)

        p = CSVParameter(cls=luigi.IntParameter, max_len=2)
        p.parse("4,5,6")
        # => ValueError

        p = CSVParameter(cls=luigi.IntParameter, brace_expand=True)
        p.parse("1{2,3,4}9")
        # => (129, 139, 149)

    .. note::

        Due to the way `instance caching
        <https://luigi.readthedocs.io/en/stable/parameters.html#parameter-instance-caching>`__
        is implemented in luigi, parameters should always have hashable values. Therefore, this
        parameter produces a tuple and, in particular, not a list. To avoid undesired side effects,
        the *default* value given to the constructor is also converted to a tuple.

    .. py:classattribute:: CSV_SEP
       type: string

        Character used as a separator between CSV elements when parsing strings and serializing
        values.

    .. py:attribute:: _inst
       type: cls

        Instance of the luigi parameter class *cls* that is used internally for parameter parsing
        and serialization.
    """

    CSV_SEP = ","

    def __init__(self, *args, **kwargs):
        self._cls = kwargs.pop("cls", luigi.Parameter)
        self._unique = kwargs.pop("unique", False)
        self._min_len = kwargs.pop("min_len", None)
        self._max_len = kwargs.pop("max_len", None)
        self._brace_expand = kwargs.pop("brace_expand", False)

        # ensure that the default value is a tuple
        if "default" in kwargs:
            kwargs["default"] = make_tuple(kwargs["default"])

        super(CSVParameter, self).__init__(*args, **kwargs)

        self._inst = self._cls()

    def _check_len(self, value):
        str_repr = lambda: self.CSV_SEP.join(str(v) for v in value)

        if self._min_len is not None and len(value) < self._min_len:
            raise ValueError("'{}' contains {} value(s), a minimum of {} is required".format(
                str_repr(), len(value), self._min_len))

        # check max_len
        if self._max_len is not None and len(value) > self._max_len:
            raise ValueError("'{}' contains {} value(s), a maximum of {} is required".format(
                str_repr(), len(value), self._max_len))

    def parse(self, inp):
        """"""
        if inp in (None, "", NO_STR):
            ret = tuple()
        elif isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            ret = make_tuple(inp)
        elif isinstance(inp, six.string_types):
            if self._brace_expand:
                elems = brace_expand(inp, split_csv=True)
            else:
                elems = inp.split(self.CSV_SEP)
            ret = tuple(map(self._inst.parse, elems))
        else:
            ret = (ret,)

        # ensure uniqueness
        if self._unique:
            ret = make_unique(ret)

        # check min_len and max_len
        self._check_len(ret)

        return ret

    def serialize(self, value):
        """"""
        if not value:
            return ""
        else:
            # ensure uniqueness
            if self._unique:
                value = make_unique(value)

            # check min_len and max_len
            self._check_len(value)

            return self.CSV_SEP.join(str(self._inst.serialize(elem)) for elem in value)


class MultiCSVParameter(CSVParameter):
    """ __init__(*args, cls=luigi.Parameter, unique=False, min_len=None, max_len=None, \
        brace_expand=False, **kwargs)
    Parameter that parses several comma-separated values (CSV), separated by colons, and produces a
    nested tuple. *cls* can refer to an other parameter class that will be used to parse and
    serialize the particular items.

    Except for the additional support for multuple CSV sequences, the implementation is based on
    :py:class:`CSVParameter`, which also handles the features controlled by *unique*, *max_len* and
    *min_len*.

    When *brace_expand* is *True*, brace expansion is applied, potentially extending the lists of
    values.

    Example:

    .. code-block:: python

        p = MultiCSVParameter(cls=luigi.IntParameter)
        p.parse("4,5:6,6")
        # => ((4, 5), (6, 6))
        p.serialize((7, 8, (9,)))
        # => "7,8:9"

        p = MultiCSVParameter(cls=luigi.IntParameter, unique=True)
        p.parse("4,5:6,6")
        # => ((4, 5), (6,))

        p = MultiCSVParameter(cls=luigi.IntParameter, max_len=2)
        p.parse("4,5:6,7,8")
        # => ValueError

        p = MultiCSVParameter(cls=luigi.IntParameter, brace_expand=True)
        p.parse("4,5:6,7,8{8,9}")
        # => ((4, 5), (6, 7, 88, 89))

    .. note::

        Due to the way `instance caching
        <https://luigi.readthedocs.io/en/stable/parameters.html#parameter-instance-caching>`__
        is implemented in luigi, parameters should always have hashable values. Therefore, this
        parameter produces a (nested) tuple and, in particular, not a list. To avoid undesired side
        effects, the *default* value given to the constructor is also converted to a tuple.

    .. py:classattribute:: MULTI_CSV_SEP
       type: string

        Character used as a separator between CSV sequences when parsing strings and serializing
        values.

    .. py:attribute:: _inst
       type: cls

        Instance of the luigi parameter class *cls* that is used internally for parameter parsing
        and serialization.
    """

    MULTI_CSV_SEP = ":"

    def parse(self, inp):
        """"""
        if isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            ret = tuple(super(MultiCSVParameter, self).parse(v) for v in inp)
        elif isinstance(inp, six.string_types):
            elems = inp.split(self.MULTI_CSV_SEP)
            ret = tuple(map(super(MultiCSVParameter, self).parse, elems))
        else:
            ret = (super(MultiCSVParameter, self).parse(inp),)

        return ret

    def serialize(self, value):
        """"""
        if not value:
            return ""
        else:
            return self.MULTI_CSV_SEP.join(
                super(MultiCSVParameter, self).serialize(v) for v in value)


class NotifyParameter(luigi.BoolParameter):
    """
    Base class for notification parameters. A notification parameter must provide a notification
    transport in :py:meth:`get_transport`, e.g.

    .. code-block:: python

        def get_transport(self):
            return {
                "func": notification_func,
                "raw": True,  # or False
            }

    When a task has a specific notification parameter set to *True* and its run method is decorated
    with the :py:func:`law.notify` function, *notification_func* is called with at least three
    arguments: *success*, *title* and *message*. *success* is a boolean which is *True* when the
    decorated function did not raise an exception. *title* is always a string. When *raw* is *False*
    (the default), *message* is also a string. Otherwise, it is an ordered dictionary containing key
    value pairs describing the message content. All options passed to
    :py:func:`law.decorator.notify` are forwarded to *notification_func* as optional arguments.
    """

    def get_transport(self):
        """
        Method to configure the transport that is toggled by this parameter. Should return a
        dictionary with ``"func"`` and ``"raw"`` (optional) fields.
        """
        return None


class NotifyMultiParameter(NotifyParameter):
    """ __init__(parameters=[], *args, **kwargs)
    Parameter that takes multiple other :py:class:`NotifyParameter`'s to join their notification
    functionality in a single parameter. Example:

    .. code-block:: python

        class MyTask(law.Task):

            notify = law.NotifyMultiParameter(parameters=[
                law.NotifyMailParameter(significant=False),
                ...  # further NotifyParameters
            ])
    """

    def __init__(self, *args, **kwargs):
        self.parameters = kwargs.pop("parameters", [])

        super(NotifyMultiParameter, self).__init__(*args, **kwargs)

    def get_transport(self):
        """"""
        return [param.get_transport() for param in self.parameters]


class NotifyMailParameter(NotifyParameter):
    """
    Notification parameter defining a basic email transport. Uses
    :py:meth:`law.notification.notify_mail` internally.
    """

    def __init__(self, *args, **kwargs):
        super(NotifyMailParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, an email notification is sent once the task finishes"

    @classmethod
    def notify(success, *args, **kwargs):
        """"""
        return notify_mail(*args, **kwargs)

    def get_transport(self):
        """"""
        return {
            "func": self.notify,
            "raw": False,
        }
