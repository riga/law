# coding: utf-8

"""
Custom luigi parameters.
"""

__all__ = [
    "NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
    "DurationParameter", "CSVParameter", "MultiCSVParameter", "RangeParameter",
    "MultiRangeParameter", "NotifyParameter", "NotifyMultiParameter", "NotifyMailParameter",
]


import luigi
import six

from law.notification import notify_mail
from law.util import (
    human_duration, parse_duration, time_units, time_unit_aliases, is_lazy_iterable, make_tuple,
    make_unique, brace_expand,
)
from law.logger import get_logger


logger = get_logger(__name__)

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
        if not inp or inp == NO_STR:
            inp = "0"

        return parse_duration(inp, input_unit=self.unit, unit=self.unit)

    def serialize(self, value):
        """"""
        if not value:
            value = 0

        value_seconds = parse_duration(value, input_unit=self.unit, unit="s")
        return human_duration(seconds=value_seconds, colon_format=True)


class CSVParameter(luigi.Parameter):
    """ __init__(*args, cls=luigi.Parameter, unique=False, sort=False, min_len=None, max_len=None, \
        choices=None, brace_expand=False, escape_sep=True, **kwargs)
    Parameter that parses a comma-separated value (CSV) and produces a tuple. *cls* can refer to an
    other parameter class that will be used to parse and serialize the particular items.

    When *unique* is *True*, both parsing and serialization methods make sure that values are
    unique. *sort* can be a boolean or a function for sorting parameter values.

    When *min_len* (*max_len*) is set to an integer, an error is raised in case the number of
    elements to serialize or parse (evaluated after potentially ensuring uniqueness) deceeds
    (exceeds) that value. Just like done in luigi's *ChoiceParamater*, *choices* can be a sequence
    of accepted values.

    When *brace_expand* is *True*, brace expansion is applied, potentially extending the list of
    values. Unless *escape_sep* is *False*, escaped separators (comma) are not split when parsing
    strings and, likewise, separators contained in values to serialze are escaped.

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

        p = CSVParameter(cls=luigi.IntParameter, choices=(1, 2))
        p.parse("2,3")
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
        values. Defaults to ``","``.

    .. py:attribute:: _inst
       type: cls

        Instance of the luigi parameter class *cls* that is used internally for parameter parsing
        and serialization.
    """

    CSV_SEP = ","

    def __init__(self, *args, **kwargs):
        self._cls = kwargs.pop("cls", luigi.Parameter)
        self._unique = kwargs.pop("unique", False)
        self._sort = kwargs.pop("sort", False)
        self._min_len = kwargs.pop("min_len", None)
        self._max_len = kwargs.pop("max_len", None)
        self._choices = kwargs.pop("choices", None)
        self._brace_expand = kwargs.pop("brace_expand", False)
        self._escape_sep = kwargs.pop("escape_sep", True)

        # ensure that the default value is a tuple
        if "default" in kwargs:
            kwargs["default"] = make_tuple(kwargs["default"])

        super(CSVParameter, self).__init__(*args, **kwargs)

        self._inst = self._cls()

        # brace expansion is only supported when CSV_SEP is ","
        if self._brace_expand and self.CSV_SEP != ",":
            logger.warning("{!r} does not support brace expansion when CSV_SEP is not a ','".format(
                self))
            self._brace_expand = False

    def _check_unique(self, value):
        if not self._unique:
            return value

        return make_unique(value)

    def _check_sort(self, value):
        if not self._sort:
            return value

        key = self._sort if callable(self._sort) else None
        value = sorted(value, key=key)

        return tuple(value)

    def _check_len(self, value):
        str_repr = lambda: self.CSV_SEP.join(str(v) for v in value)

        if self._min_len is not None and len(value) < self._min_len:
            raise ValueError("'{}' contains {} value(s), a minimum of {} is required".format(
                str_repr(), len(value), self._min_len))

        # check max_len
        if self._max_len is not None and len(value) > self._max_len:
            raise ValueError("'{}' contains {} value(s), a maximum of {} is required".format(
                str_repr(), len(value), self._max_len))

    def _check_choices(self, value):
        if not self._choices:
            return

        unknown = []
        for v in value:
            if v not in self._choices:
                unknown.append(v)

        if unknown:
            str_repr = lambda value: ",".join(str(v) for v in value)
            raise ValueError("invalid parameter value(s) '{}', valid choices are '{}'".format(
                str_repr(make_unique(unknown)), str_repr(self._choices)))

    def parse(self, inp):
        """"""
        if not inp or inp == NO_STR:
            value = tuple()
        elif isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            value = make_tuple(inp)
        elif isinstance(inp, six.string_types):
            if self._brace_expand:
                elems = brace_expand(inp, split_csv=True, escape_csv_sep=self._escape_sep)
            else:
                # replace escaped separators
                if self._escape_sep:
                    escaped_sep = "__law_escaped_csv_sep__"
                    inp = inp.replace("\\" + self.CSV_SEP, escaped_sep)
                # split
                elems = inp.split(self.CSV_SEP)
                # add back escaped separators per element
                if self._escape_sep:
                    elems = [elem.replace(escaped_sep, self.CSV_SEP) for elem in elems]
            value = tuple(map(self._inst.parse, elems))
        else:
            value = (inp,)

        # apply uniqueness, sort, length and choices checks
        value = self._check_unique(value)
        value = self._check_sort(value)
        self._check_len(value)
        self._check_choices(value)

        return value

    def serialize(self, value):
        """"""
        if not value:
            value = tuple()

        value = make_tuple(value)

        # apply uniqueness, sort, length and choices checks
        value = self._check_unique(value)
        value = self._check_sort(value)
        self._check_len(value)
        self._check_choices(value)

        return self.CSV_SEP.join(str(self._inst.serialize(elem)) for elem in value)


class MultiCSVParameter(CSVParameter):
    """ __init__(*args, cls=luigi.Parameter, unique=False, sort=False, min_len=None, max_len=None, \
        choices=None, brace_expand=False, escape_sep=True, **kwargs)
    Parameter that parses several comma-separated values (CSV), separated by colons, and produces a
    nested tuple. *cls* can refer to an other parameter class that will be used to parse and
    serialize the particular items.

    Except for the additional support for multiple CSV sequences, the parsing and serialization
    implementation is based on :py:class:`CSVParameter`, which also handles the features controlled
    by *unique*, *sort*, *max_len*, *min_len*, *choices*, *brace_expand* and *escape_sep* per
    sequence of values.

    Unless *escape_sep* is *False*, escaped separators (colon) are not split when parsing strings
    and, likewise, separators contained in values to serialze are escaped.

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

        p = MultiCSVParameter(cls=luigi.IntParameter, choices=(1, 2))
        p.parse("1,2:2,3")
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
        values. Defaults to ``":"``.

    .. py:attribute:: _inst
       type: cls

        Instance of the luigi parameter class *cls* that is used internally for parameter parsing
        and serialization.
    """

    MULTI_CSV_SEP = ":"

    def parse(self, inp):
        """"""
        if not inp or inp == NO_STR:
            value = tuple()
        elif isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            value = tuple(super(MultiCSVParameter, self).parse(v) for v in inp)
        elif isinstance(inp, six.string_types):
            # replace escaped separators
            if self._escape_sep:
                escaped_sep = "__law_escaped_multi_csv_sep__"
                inp = inp.replace("\\" + self.MULTI_CSV_SEP, escaped_sep)
            # split
            elems = inp.split(self.MULTI_CSV_SEP)
            # add back escaped separators per element
            if self._escape_sep:
                elems = [elem.replace(escaped_sep, self.MULTI_CSV_SEP) for elem in elems]
            value = tuple(super(MultiCSVParameter, self).parse(e) for e in elems)
        else:
            value = (super(MultiCSVParameter, self).parse(inp),)

        return value

    def serialize(self, value):
        """"""
        if not value:
            return ""

        # ensure that value is a nested tuple
        value = tuple(map(make_tuple, make_tuple(value)))

        return self.MULTI_CSV_SEP.join(
            super(MultiCSVParameter, self).serialize(v) for v in value)


class RangeParameter(luigi.Parameter):
    """ __init__(*args, require_start=True, require_end=True, single_value=False, **kwargs)
    Parameter that parses a range in the format ``start:stop`` and returns a tuple with two integer
    elements.

    When *require_start* or *require_stop* are *False*, the formats ``:stop`` and ``start:``,
    respectively, are accepted as well. In these cases, the tuple will contain an attribute
    :py:attr:`OPEN` do denote that either side is unconstrained.

    When *single_value* is *True*, single integer values are accepted and lead to a tuple with one
    element.

    .. code-block:: python

        p = RangeParameter()
        p.parse("4:8")
        # => (4, 8)
        p.serialize((5, 9))
        # => "5:9"
        p.parse("4:")
        # => ValueError
        p.parse("4")
        # => ValueError

        p = RangeParameter(require_start=False, require_stop=False)
        p.parse("4:5")
        # => (4, 5)
        p.parse("4:")
        # => (4, OPEN)
        p.parse(":5")
        # => (OPEN, 5)
        p.parse(":")
        # => (OPEN, OPEN)
        p.serialize((OPEN, 8))
        # => ":8"

        p = RangeParameter(single_value=True)
        p.parse("4")
        # => (4,)
        p.serialize((5,))
        # => "4"

    .. py:classattribute:: RANGE_SEP
       type: string

        Character used as a separator between range edges when parsing strings and serializing
        values. Defaults to ``":"``.

    .. py:classattribute:: OPEN
       type: None

        Value denoting open edges in parsed ranges.
    """

    RANGE_SEP = ":"
    OPEN = None

    def __init__(self, *args, **kwargs):
        self._require_start = kwargs.pop("require_start", True)
        self._require_end = kwargs.pop("require_end", True)
        self._single_value = kwargs.pop("single_value", False)

        # ensure that the default value is a tuple
        if "default" in kwargs:
            kwargs["default"] = make_tuple(kwargs["default"])

        super(RangeParameter, self).__init__(*args, **kwargs)

    def _check(self, value):
        if not isinstance(value, tuple):
            raise TypeError("invalid type of range {}, must be a tuple".format(value))

        elif len(value) == 1 and self._single_value:
            if not isinstance(value[0], six.integer_types):
                raise TypeError("invalid type of single value in range {}".format(value))

        elif len(value) == 2:
            start, end = value
            if start == self.OPEN:
                if self._require_start:
                    raise ValueError("range {} lacks start value which is required".format(value))
            elif not isinstance(start, six.integer_types):
                raise TypeError("invalid type of start value in range {}".format(value))
            if end == self.OPEN:
                if self._require_end:
                    raise ValueError("range {} lacks end value which is required".format(value))
            elif not isinstance(end, six.integer_types):
                raise TypeError("invalid type of end value in range {}".format(value))

        elif value:
            raise ValueError("cannot interpret {} with {} elements as {}".format(
                value, len(value), self.__class__.__name__))

    def parse(self, inp):
        """"""
        if not inp or inp == NO_STR:
            value = tuple()
        elif isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            value = make_tuple(inp)
        elif isinstance(inp, six.integer_types):
            value = (inp,)
        elif isinstance(inp, six.string_types):
            parts = inp.split(self.RANGE_SEP)
            # convert integers
            try:
                value = tuple((int(p) if p else self.OPEN) for p in parts)
            except ValueError:
                raise ValueError("range '{}'' contains non-integer elements".format(inp))
        else:
            value = (inp,)

        self._check(value)

        return value

    def serialize(self, value):
        """"""
        if not value:
            value = tuple()

        self._check(value)

        value = [("" if v == self.OPEN else str(v)) for v in value]
        return self.RANGE_SEP.join(value)


class MultiRangeParameter(RangeParameter):
    """ __init__(*args, require_start=True, require_end=True, single_value=False, **kwargs)
    Parameter that parses several integer ranges (each in the format ``start-end``), separated by
    comma, and produces a nested tuple.

    Except for the additional support for multiple ranges, the parsing and serialization
    implementation is based on :py:class:`RangeParameter`, which also handles the control of open
    edges with *require_start* and *require_end*, and the acceptance of single integer values with
    *single_value*. Example:

    .. code-block:: python

        p = MultiRangeParameter()
        p.parse("4:8,12:14")
        # => ((4, 8), (12, 14))
        p.serialize(((5, 9), (13, 15)))
        # => ""5:9,13:15""

    .. py:classattribute:: MULTI_RANGE_SEP
       type: string

        Character used as a separator between ranges when parsing strings and serializing
        values. Defaults to ``","``.
    """

    MULTI_RANGE_SEP = ","

    def parse(self, inp):
        """"""
        if not inp or inp == NO_STR:
            value = tuple()
        elif isinstance(inp, (tuple, list)) or is_lazy_iterable(inp):
            value = tuple(super(MultiRangeParameter, self).parse(v) for v in inp)
        elif isinstance(inp, six.string_types):
            elems = inp.split(self.MULTI_RANGE_SEP)
            value = tuple(super(MultiRangeParameter, self).parse(e) for e in elems)
        else:
            value = (super(MultiRangeParameter, self).parse(inp),)

        return value

    def serialize(self, value):
        """"""
        if not value:
            return ""

        # ensure that value is a nested tuple
        value = tuple(map(make_tuple, make_tuple(value)))

        return self.MULTI_RANGE_SEP.join(
            super(MultiRangeParameter, self).serialize(v) for v in value)


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
            "colored": False,
        }
