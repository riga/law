# coding: utf-8

"""
Helpful utility functions.
"""

__all__ = [
    "default_lock", "io_lock", "console_lock", "no_value", "rel_path", "law_src_path",
    "law_home_path", "law_run", "print_err", "abort", "is_number", "try_int", "round_discrete",
    "str_to_int", "flag_to_bool", "empty_context", "common_task_params", "colored", "uncolored",
    "query_choice", "is_pattern", "brace_expand", "range_expand", "range_join", "multi_match",
    "is_iterable", "is_lazy_iterable", "make_list", "make_tuple", "make_unique", "is_nested",
    "flatten", "merge_dicts", "which", "map_verbose", "map_struct", "mask_struct", "tmp_file",
    "interruptable_popen", "readable_popen", "create_hash", "create_random_string", "copy_no_perm",
    "makedirs", "user_owns_file", "iter_chunks", "human_bytes", "parse_bytes", "human_duration",
    "parse_duration", "is_file_exists_error", "send_mail", "DotDict", "ShorthandDict",
    "open_compat", "patch_object", "join_generators", "quote_cmd", "escape_markdown",
    "classproperty", "BaseStream", "TeeStream", "FilteredStream",
]


import os
import sys
import types
import re
import math
import fnmatch
import itertools
import tempfile
import subprocess
import signal
import hashlib
import uuid
import shutil
import copy
import collections
import contextlib
import smtplib
import time
import datetime
import random
import threading
import io
import shlex
import logging

import six

try:
    import ipykernel
    import ipykernel.iostream
except ImportError:
    ipykernel = None


logger = logging.getLogger(__name__)

# some globally usable thread locks
default_lock = threading.Lock()
io_lock = threading.Lock()
console_lock = threading.Lock()


class NoValue(object):

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(NoValue, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False

    def __str__(self):
        return "{}.no_value".format(self.__module__)


#: Unique dummy value that is used to denote missing values and always evaluates to *False*.
no_value = NoValue()


def rel_path(anchor, *paths):
    """
    Returns a path made of framgment *paths* relativ to an *anchor* path. When *anchor* is a file,
    its absolute directory is used instead.
    """
    anchor = os.path.abspath(os.path.expandvars(os.path.expanduser(anchor)))
    if os.path.exists(anchor) and os.path.isfile(anchor):
        anchor = os.path.dirname(anchor)
    return os.path.normpath(os.path.join(anchor, *paths))


def law_src_path(*paths):
    """
    Returns the law installation directory, optionally joined with *paths*.
    """
    return rel_path(__file__, *paths)


def law_home_path(*paths):
    """
    Returns the law home directory, optionally joined with *paths*.
    """
    from law.config import law_home_path
    return law_home_path(*paths)


def law_run(argv, **kwargs):
    """
    Runs a task with certain parameters as defined in *argv*, which can be a string or a list of
    strings. It must start with the family of the task to run, followed by the desired parameters.
    All *kwargs* are forwarded to :py:func:`luigi.interface.run`. Example:

    .. code-block:: python

        law_run(["MyTask", "--param", "value"])
        law_run("MyTask --param value")
    """
    from luigi.interface import run as luigi_run

    # ensure that argv is a list of strings
    if isinstance(argv, six.string_types):
        argv = shlex.split(argv)
    else:
        argv = [str(arg) for arg in argv]

    # luigi's pid locking must be disabled
    argv.append("--no-lock")

    return luigi_run(argv, **kwargs)


def print_err(*args, **kwargs):
    """ print_err(*args, flush=False)
    Same as *print*, but outputs to stderr. If *flush* is *True*, stderr is flushed after printing.
    """
    sys.stderr.write(" ".join(str(arg) for arg in args) + "\n")
    if kwargs.get("flush", False):
        sys.stderr.flush()


def abort(msg=None, exitcode=1, color=True):
    """
    Aborts the process (*sys.exit*) with an *exitcode*. If *msg* is not *None*, it is printed first
    to stdout if *exitcode* is 0 or *None*, and to stderr otherwise. When *color* is *True* and
    *exitcode* is not 0 or *None*, the message is printed in red.
    """
    if msg is not None:
        if exitcode in (None, 0):
            print(msg)
        else:
            if color:
                msg = colored(msg, color="red")
            print_err(msg)
    sys.exit(exitcode)


def is_number(n):
    """
    Returns *True* if *n* is a number, i.e., integer or float, and in particular no boolean.
    """
    return isinstance(n, six.integer_types + (float,)) and not isinstance(n, bool)


def try_int(n):
    """
    Takes a number *n* and tries to convert it to an integer. When *n* has no decimals, an integer
    is returned with the same value as *n*. Otherwise, a float is returned.
    """
    n_int = int(n)
    return n_int if n == n_int else n


def round_discrete(n, base=1., round_fn=round):
    """ round_discrete(n, base=1.0, round_fn="round")
    Rounds a number *n* to a discrete *base*. *round_fn* can be a function used for rounding and
    defaults to the built-in ``round`` function. It also accepts string values ``"round"``,
    ``"floor"`` and ``"ceil"`` which are resolved to the corresponding math functions. Example:

    .. code-block:: python

        round_discrete(17, 5)
        # -> 15.

        round_discrete(17, 2.5)
        # -> 17.5

        round_discrete(17, 2.5)
        # -> 17.5

        round_discrete(17, 2.5, math.floor)
        round_discrete(17, 2.5, "floor")
        # -> 15.0
    """
    if isinstance(round_fn, six.string_types):
        if round_fn == "round":
            round_fn = round
        elif round_fn == "floor":
            round_fn = math.floor
        elif round_fn == "ceil":
            round_fn = math.ceil
        else:
            raise ValueError("unknown round function '{}'".format(round_fn))

    return base * round_fn(float(n) / base)


def str_to_int(s):
    """
    Converts a string *s* into an integer under consideration of binary, octal, decimal and
    hexadecimal representations, such as ``"0o0660"``.
    """
    s = str(s).lower()
    m = re.match(r"^0(b|o|d|x)\d+$", s)
    base = {"b": 2, "o": 8, "d": 10, "x": 16}[m.group(1)] if m else 10
    return int(s, base=base)


def flag_to_bool(s, silent=False):
    """
    Takes a string flag *s* and returns whether it evaluates to *True* (values ``"1"``, ``"true"``
    ``"yes"``, ``"y"``, ``"on"``, case-insensitive) or *False* (values ``"0"``, ``"false"``,
    `"no"``, ``"n"``, ``"off"``, case-insensitive). When *s* is already a boolean, it is returned
    unchanged. An error is thrown when *s* is neither of the allowed values and *silent* is *False*.
    Otherwise, *None* is returned.
    """
    if isinstance(s, bool):
        return s
    elif isinstance(s, six.string_types):
        if s.lower() in ("true", "1", "yes", "y", "on"):
            return True
        elif s.lower() in ("false", "0", "no", "n", "off"):
            return False

    if silent:
        return None
    else:
        raise ValueError("cannot convert to bool: {}".format(s))


@contextlib.contextmanager
def empty_context():
    """
    Yields an empty context that can be used in case of dynamically choosing context managers while
    maintaining code structure.
    """
    yield


def common_task_params(task_instance, task_cls):
    """
    Returns the parameters that are common between a *task_instance* and a *task_cls* in a
    dictionary with values taken directly from the task instance. The difference with respect to
    ``luigi.util.common_params`` is that the values are not parsed using the parameter objects of
    the task class, which might be faster for some purposes.
    """
    task_cls_param_names = [name for name, _ in task_cls.get_params()]
    common_param_names = [
        name for name, _ in task_instance.get_params()
        if name in task_cls_param_names
    ]
    return {name: getattr(task_instance, name) for name in common_param_names}


colors = {
    "default": 39,
    "black": 30,
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "magenta": 35,
    "cyan": 36,
    "light_gray": 37,
    "dark_gray": 90,
    "light_red": 91,
    "light_green": 92,
    "light_yellow": 93,
    "light_blue": 94,
    "light_magenta": 95,
    "light_cyan": 96,
    "white": 97,
}

backgrounds = {
    "default": 49,
    "black": 40,
    "red": 41,
    "green": 42,
    "yellow": 43,
    "blue": 44,
    "magenta": 45,
    "cyan": 46,
    "light_gray": 47,
    "dark_gray": 100,
    "light_red": 101,
    "light_green": 102,
    "light_yellow": 103,
    "light_blue": 104,
    "light_magenta": 105,
    "light_cyan": 106,
    "white": 107,
}

styles = {
    "default": 0,
    "bright": 1,
    "dim": 2,
    "underlined": 4,
    "blink": 5,
    "inverted": 7,
    "hidden": 8,
}

uncolor_cre = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


def colored(msg, color=None, background=None, style=None, force=False):
    """
    Return the colored version of a string *msg*. For *color*, *background* and *style* options, see
    https://misc.flogisoft.com/bash/tip_colors_and_formatting. They can also be explicitely set to
    ``"random"`` to get a random value. Unless *force* is *True*, the *msg* string is returned
    unchanged in case the output is neither a tty nor an IPython output stream.
    """
    if not force:
        tty = False
        ipy = False

        try:
            tty = os.isatty(sys.stdout.fileno())
        except:
            pass

        if not tty and ipykernel is not None:
            ipy = isinstance(sys.stdout, ipykernel.iostream.OutStream)

        if not tty and not ipy:
            return msg

    if color == "random":
        color = random.choice(list(colors.values()))
    else:
        color = colors.get(color, colors["default"])

    if background == "random":
        background = random.choice(list(backgrounds.values()))
    else:
        background = backgrounds.get(background, backgrounds["default"])

    if not isinstance(style, (tuple, list, set)):
        style = (style,)
    style_values = list(styles.values())
    style = ";".join(
        str(random.choice(style_values) if s == "random" else styles.get(s, styles["default"]))
        for s in style
    )

    return "\033[{};{};{}m{}\033[0m".format(style, background, color, msg)


def uncolored(s):
    """
    Removes all color codes from a string *s* and returns it.
    """
    return uncolor_cre.sub("", s)


def query_choice(msg, choices, default=None, descriptions=None, lower=True):
    """
    Interactively query a choice from the prompt until the input matches one of the *choices*. The
    prompt can be configured using *msg* and *descriptions*, which, if set, must have the same
    length as *choices*. When *default* is not *None* it must be one of the choices and is used when
    the input is empty. When *lower* is *True*, the input is compared to the choices in lower case.
    """
    choices = _choices = [str(c) for c in choices]
    if lower:
        _choices = [c.lower() for c in choices]

    if default is not None:
        if default not in choices:
            raise Exception("default must be one of the choices")

    hints = [(choice if choice != default else choice + "*") for choice in choices]
    if descriptions is not None:
        if len(descriptions) != len(choices):
            raise ValueError("length of descriptions must match length of choices")
        hints = ["{}({})".format(*tpl) for tpl in zip(hints, descriptions)]
    msg += " [{}] ".format(", ".join(hints))

    choice = None
    while choice not in _choices:
        if choice is not None:
            print("invalid choice: '{}'".format(choice))
        choice = six.moves.input(msg)
        if default is not None and choice == "":
            choice = default
        if lower:
            choice = choice.lower()

    return choice


def is_pattern(s):
    """
    Returns *True* if the string *s* represents a pattern, i.e., if it contains characters such as
    ``"*"`` or ``"?"``.
    """
    return "*" in s or "?" in s


def brace_expand(s, split_csv=False, escape_csv_sep=True):
    """
    Expands brace statements in a string *s* and returns a list containing all possible string
    combinations. When *split_csv* is *True*, the input string is split by all comma characters
    located outside braces, except for escaped ones when *escape_csv_sep* is *True*, and the
    expansion is performed sequentially on all elements. Example:

    .. code-block:: python

        brace_expand("A{1,2}B")
        # -> ["A1B", "A2B"]

        brace_expand("A{1,2}B{3,4}C")
        # -> ["A1B3C", "A1B4C", "A2B3C", "A2B4C"]

        brace_expand("A{1,2}B,C{3,4}D")
        # note the full 2x2 expansion
        # -> ["A1B,C3D", "A1B,C4D", "A2B,C3D", "A2B,C4D"]

        brace_expand("A{1,2}B,C{3,4}D", split_csv=True)
        # note the 2+2 sequential expansion
        # -> ["A1B", "A2B", "C3D", "C4D"]

        brace_expand("A{1,2}B,C{3}D", split_csv=True)
        # note the 2+1 sequential expansion
        # -> ["A1B", "A2B", "C3D"]
    """
    # first, replace escaped braces
    br_open = "__law_brace_open__"
    br_close = "__law_brace_close__"
    s = s.replace(r"\{", br_open).replace(r"\}", br_close)

    # compile the expression that finds brace statements
    cre = re.compile(r"\{[^\{]*\}")

    # take into account csv splitting
    if split_csv:
        # replace csv separators in brace statements to avoid splitting
        br_sep = "__law_brace_csv_sep__"
        _s = cre.sub(lambda m: m.group(0).replace(",", br_sep), s)
        # replace escaped commas
        if escape_csv_sep:
            escaped_sep = "__law_escaped_csv_sep__"
            _s = _s.replace(r"\,", escaped_sep)
        # split by real csv separators except escaped ones when requested,
        parts = _s.split(",")
        # add back normal commas
        if escape_csv_sep:
            parts = [part.replace(escaped_sep, ",") for part in parts]
        # start recursion when a comma was found, otherwise continue
        if len(parts) > 1:
            # replace csv separators in braces again and recurse
            parts = [part.replace(br_sep, ",") for part in parts]
            return sum((brace_expand(part, split_csv=False) for part in parts), [])

    # split the string into n sequences with values to expand and n+1 fixed entities
    sequences = cre.findall(s)
    entities = cre.split(s)
    if len(sequences) + 1 != len(entities):
        raise ValueError("the number of sequences ({}) and the number of fixed entities ({}) are "
            "not compatible".format(",".join(sequences), ",".join(entities)))

    # split each sequence by comma
    sequences = [seq[1:-1].split(",") for seq in sequences]

    # create a template using the fixed entities used for formatting
    tmpl = "{}".join(entities)

    # build all combinations
    res = []
    for values in itertools.product(*sequences):
        _s = tmpl.format(*values)

        # insert escaped braces again
        _s = _s.replace(br_open, r"\{").replace(br_close, r"\}")

        res.append(_s)

    return res


def range_expand(s, include_end=True, min_value=None, max_value=None, sep=":"):
    """
    Takes a string, or a sequence of strings in the format ``"1:3"``, or a tuple or a sequence of
    tuples containing start and stop values of a range and returns a list of all intermediate
    values. When *include_end* is *True*, the end value is included. One sided range expressions
    such as ``":4"`` or ``"4:"`` for strings and ``(None, 4)`` or ``(4, None)`` for tuples are also
    expanded but they require *min_value* and *max_value* to be set (an exception is raised
    otherwise). Also, when a *min_value* (*max_value*) is given, no value in the returned list of
    numbers can be smaller (larger). Example:

    .. code-block:: python

        range_expand("5:8")
        # -> [5, 6, 7, 8]

        range_expand((6, 9))
        # -> [6, 7, 8, 9]

        range_expand("5:8", include_end=False)
        # -> [5, 6, 7]

        range_expand(["5-8", "10"])
        # -> [5, 6, 7, 8, 10]

        range_expand(["5-8", "10-"])
        # -> Exception, no max_value set

        range_expand(["5-8", "10-"], max_value=12)
        # -> [5, 6, 7, 8, 10, 11, 12]
    """
    def to_int(v, s=None):
        try:
            return int(v)
        except ValueError:
            raise ValueError("invalid number or range '{}'".format(v if s is None else s))

    numbers = []
    for s in make_list(s):
        start, stop, value = None, None, None
        single_value = False

        if isinstance(s, (tuple, list)):
            # parse tuple
            if len(s) == 1:
                value = s[0]
                single_value = True
            elif len(s) == 2:
                start, stop = s
            else:
                raise ValueError("invalid range tuple length: {}".format(s))

        else:
            # parse as string
            s = str(s)
            if sep in s:
                parts = s.split(sep, 1)
                start = parts[0] or None
                stop = parts[1] or None
            else:
                value = s
                single_value = True

        if single_value:
            # add a single value
            numbers.append(to_int(value))

        else:
            # build the range
            if start is None:
                if min_value is None:
                    raise Exception("range '{}' with missing start value requires min_value to be "
                        "set".format(s))
                start = min_value
            if stop is None:
                if max_value is None:
                    raise Exception("range '{}' with missing stop value requires max_value to be "
                        "set".format(s))
                stop = max_value

            # convert to integers and potentially swap
            start = to_int(start)
            stop = to_int(stop)
            if start > stop:
                start, stop = stop, start

            # add numbers
            numbers.extend(range(start, stop + int(bool(include_end))))

    # apply min and max cuts when given
    if min_value is not None:
        numbers = [n for n in numbers if n >= min_value]
    if max_value is not None:
        numbers = [n for n in numbers if n <= max_value]

    # remove duplicates preserving the order
    numbers = make_unique(numbers)

    return numbers


def range_join(numbers, to_str=False, sep=",", range_sep=":"):
    """
    Takes a sequence of positive integer numbers given either as integer or string types, and
    returns a sequence 1- and 2-tuples, denoting either single numbers or inclusive start and stop
    values of possible ranges. When *to_str* is *True*, a string is returned in a format consistent
    to :py:func:`range_expand` with ranges constructed by *range_sep* and merged with *sep*.
    Example:

    .. code-block:: python

        range_join([1, 2, 3, 5])
        # -> [(1, 3), (5,)]

        range_join([1, 2, 3, 5, 7, 8, 9])
        # -> [(1, 3), (5,), (7, 9)]

        range_join([1, 2, 3, 5, 7, 8, 9], to_str=True)
        # -> "1:3,5,7:9"
    """
    if not numbers:
        return "" if to_str else []

    # check type, convert, make unique and sort
    _numbers = []
    for n in numbers:
        if isinstance(n, six.string_types):
            try:
                n = int(n)
            except ValueError:
                raise ValueError("invalid number format '{}'".format(n))
        if isinstance(n, six.integer_types):
            _numbers.append(n)
        else:
            raise TypeError("cannot handle non-integer value '{}' in numbers to join".format(n))
    numbers = sorted(set(_numbers))

    # iterate through numbers, keep track of last starts and stops and fill a list of range tuples
    ranges = []
    start = stop = numbers[0]
    for n in numbers[1:]:
        if n == stop + 1:
            stop += 1
        else:
            ranges.append((start,) if start == stop else (start, stop))
            start = stop = n
    ranges.append((start,) if start == stop else (start, stop))

    # convert to string representation
    if to_str:
        ranges = sep.join(
            (str(r[0]) if len(r) == 1 else "{1}{0}{2}".format(range_sep, *r))
            for r in ranges
        )

    return ranges


def multi_match(name, patterns, mode=any, regex=False):
    """
    Compares *name* to multiple *patterns* and returns *True* in case of at least one match (*mode*
    = *any*, the default), or in case all patterns match (*mode* = *all*). Otherwise, *False* is
    returned. When *regex* is *True*, *re.match* is used instead of *fnmatch.fnmatch*.
    """
    patterns = make_list(patterns)
    if not regex:
        return mode(fnmatch.fnmatch(name, pattern) for pattern in patterns)
    else:
        return mode(re.match(pattern, name) for pattern in patterns)


def is_iterable(obj):
    """
    Returns *True* when an object *obj* is iterable and *False* otherwise.
    """
    try:
        iter(obj)
    except Exception:
        return False
    return True


lazy_iter_types = (
    types.GeneratorType,
    six.moves.collections_abc.MappingView,
    six.moves.range,
    six.moves.map,
    enumerate,
)


def is_lazy_iterable(obj):
    """
    Returns whether *obj* is iterable lazily, such as generators, range objects, maps, etc.
    """
    return isinstance(obj, lazy_iter_types)


def make_list(obj, cast=True):
    """
    Converts an object *obj* to a list and returns it. Objects of types *tuple* and *set* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new list.
    """
    if isinstance(obj, list):
        return list(obj)
    elif is_lazy_iterable(obj):
        return list(obj)
    elif isinstance(obj, (tuple, set)) and cast:
        return list(obj)
    else:
        return [obj]


def make_tuple(obj, cast=True):
    """
    Converts an object *obj* to a tuple and returns it. Objects of types *list* and *set* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new tuple.
    """
    if isinstance(obj, tuple):
        return obj
    elif is_lazy_iterable(obj):
        return tuple(obj)
    elif isinstance(obj, (list, set)) and cast:
        return tuple(obj)
    else:
        return (obj,)


def make_unique(obj):
    """
    Takes a list or tuple *obj*, removes duplicate elements in order of their appearance and returns
    the sequence of remaining, unique elements. The sequence type is preserved. When *obj* is
    neither a list nor a tuple, but iterable, a list is returned. Otherwise, a *TypeError* is
    raised.
    """
    if not isinstance(obj, (list, tuple)):
        if is_iterable(obj) or is_lazy_iterable(obj):
            obj = list(obj)
        else:
            raise TypeError("object is neither list, tuple, nor generic iterable")

    ret = sorted(obj.__class__(set(obj)), key=lambda elem: obj.index(elem))

    return obj.__class__(ret) if isinstance(obj, tuple) else ret


def is_nested(obj):
    """
    Takes a list or tuple *obj* and checks whether it only contains items of types list and tuple.
    """
    return isinstance(obj, (list, tuple)) and all(isinstance(item, (list, tuple)) for item in obj)


def flatten(*structs, **kwargs):
    """ flatten(*structs, flatten_dict=True, flatten_list=True, flatten_tuple=True, flatten_set=True)
    Takes one or multiple complex structured objects *structs*, flattens them, and returns a single
    list. *flatten_dict*, *flatten_list*, *flatten_tuple* and *flatten_set* configure if objects of
    the respective types are flattened (the default). If not, they are returned unchanged.
    """
    if len(structs) == 0:
        return []
    elif len(structs) > 1:
        return flatten(structs, **kwargs)
    else:
        struct = structs[0]

        flatten_seq = lambda seq: sum((flatten(obj, **kwargs) for obj in seq), [])
        if isinstance(struct, dict):
            if kwargs.get("flatten_dict", True):
                return flatten_seq(struct.values())
        elif isinstance(struct, list):
            if kwargs.get("flatten_list", True):
                return flatten_seq(struct)
        elif isinstance(struct, tuple):
            if kwargs.get("flatten_tuple", True):
                return flatten_seq(struct)
        elif isinstance(struct, set):
            if kwargs.get("flatten_set", True):
                return flatten_seq(struct)
        elif is_lazy_iterable(struct):
            return flatten_seq(struct)

        return [struct]


def merge_dicts(*dicts, **kwargs):
    """ merge_dicts(*dicts, cls=None, deep=False)
    Takes multiple *dicts* and returns a single merged dict. The merging takes place in order of the
    passed dicts and therefore, values of rear objects have precedence in case of field collisions.
    The class of the returned merged dict is configurable via *cls*. If it is *None*, the class is
    inferred from the first dict object in *dicts*.

    When *deep* is *True*, dictionary types within the dictionaries to merge are updated recursively
    such that their fields are merged. This is only possible when input dictionaries have a similar
    structure. Example:

    .. code-block:: python

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": {"c": 3}})
        # -> {"foo": 1, "bar": {"c": 3}}  # fully replaced "bar"

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": {"c": 3}}, deep=True)
        # -> {"foo": 1, "bar": {"a": 1, "b": 2, "c": 3}}  # inserted entry bar.c

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": 2}, deep=True)
        # -> {"foo": 1, "bar": 2}  # "bar" has a different type, so this just uses the rear value
    """
    # get or infer the class
    cls = kwargs.get("cls", None)
    if cls is None:
        for d in dicts:
            if isinstance(d, dict):
                cls = d.__class__
                break
        else:
            raise TypeError("cannot infer cls as none of the passed objects is of type dict")

    # start merging
    deep = kwargs.get("deep", False)
    merged_dict = cls()
    for d in dicts:
        if not isinstance(d, dict):
            continue

        if deep:
            for k, v in d.items():
                # just take the value as is when it is not a dict, or the field is either not
                # existing yet or not a dict in the merged dict
                if not isinstance(v, dict) or not isinstance(merged_dict.get(k), dict):
                    merged_dict[k] = v
                else:
                    # merge by recursion
                    merged_dict[k] = merge_dicts(merged_dict[k], v, cls=cls, deep=deep)
        else:
            merged_dict.update(d)

    return merged_dict


def which(prog):
    """
    Pythonic ``which`` implementation. Returns the path to an executable *prog* by searching in
    *PATH*, or *None* when it could not be found.
    """
    executable = lambda path: os.path.isfile(path) and os.access(path, os.X_OK)

    # prog can also be a path
    dirname, _ = os.path.split(prog)
    if dirname:
        if executable(prog):
            return prog
    elif "PATH" in os.environ:
        for search_path in os.environ["PATH"].split(os.pathsep):
            path = os.path.join(search_path.strip('"'), prog)
            if executable(path):
                return path

    return None


def map_verbose(func, seq, msg="{}", every=25, start=True, end=True, offset=0, callback=None):
    """
    Same as the built-in map function but prints a *msg* after chunks of size *every* iterations.
    When *start* (*stop*) is *True*, the *msg* is also printed after the first (last) iteration.
    Note that *msg* is supposed to be a template string that will be formatted with the current
    iteration number (starting at 0) plus *offset* using ``str.format``. When *callback* is
    callable, it is invoked instead of the default print method with the current iteration number
    (without *offset*) as the only argument. Example:

    .. code-block:: python

       func = lambda x: x ** 2
       msg = "computing square of {}"
       squares = map_verbose(func, range(7), msg, every=3)
       # ->
       # computing square of 0
       # computing square of 2
       # computing square of 5
       # computing square of 6
    """
    # default callable
    if not callable(callback):
        def callback(i):
            print(msg.format(i + offset))

    results = []
    for i, obj in enumerate(seq):
        results.append(func(obj))
        do_call = (start and i == 0) or (i + 1) % every == 0
        if do_call:
            callback(i)
    else:
        if end and results and not do_call:
            callback(i)

    return results


def map_struct(func, struct, map_dict=True, map_list=True, map_tuple=False, map_set=False,
        cls=None, custom_mappings=None):
    """
    Applies a function *func* to each value of a complex structured object *struct* and returns the
    output in the same structure. Example:

    .. code-block:: python

        struct = {"foo": [123, 456], "bar": [{"1": 1}, {"2": 2}]}
        def times_two(i):
            return i * 2

        map_struct(times_two, struct)
        # -> {"foo": [246, 912], "bar": [{"1": 2}, {"2": 4}]}

    *map_dict*, *map_list*, *map_tuple* and *map_set* configure if objects of the respective types
    are traversed or mapped as a whole. They can be booleans or integer values defining the depth of
    that setting in the struct. When *cls* is not *None*, it exclusively defines the class of
    objects that *func* is applied on. All other objects are unchanged. *custom_mappings* key be a
    dictionary that maps custom types to custom object traversal methods. The following example
    would tranverse lists backwards:

    .. code-block:: python

        def traverse_lists(func, l, **kwargs):
            return [map_struct(func, v, **kwargs) for v in l[::-1]]

        map_struct(times_two, struct, custom_mappings={list: traverse_lists})
        # -> {"foo": [912, 246], "bar": [{"1": 2}, {"2": 4}]}
    """
    # interpret generators and views as lists
    if is_lazy_iterable(struct):
        struct = list(struct)

    # determine valid types for struct traversal
    valid_types = tuple()
    if map_dict:
        valid_types += (dict,)
        if is_number(map_dict):
            map_dict -= 1
    if map_list:
        valid_types += (list,)
        if is_number(map_list):
            map_list -= 1
    if map_tuple:
        valid_types += (tuple,)
        if is_number(map_tuple):
            map_tuple -= 1
    if map_set:
        valid_types += (set,)
        if is_number(map_set):
            map_set -= 1

    # is an explicit cls set?
    if cls is not None:
        return func(struct) if isinstance(struct, cls) else struct

    # custom mapping?
    elif custom_mappings and isinstance(struct, tuple(flatten(custom_mappings.keys()))):
        # get the mapping function
        for mapping_types, mapping_func in six.iteritems(custom_mappings):
            if isinstance(struct, mapping_types):
                return mapping_func(func, struct, map_dict=map_dict, map_list=map_list,
                    map_tuple=map_tuple, map_set=map_set, cls=cls, custom_mappings=custom_mappings)

    # traverse?
    elif isinstance(struct, valid_types):
        # create a new struct, treat tuples as lists for itertative item appending
        new_struct = struct.__class__() if not isinstance(struct, tuple) else []

        # create type-dependent generator and addition callback
        if isinstance(struct, (list, tuple)):
            gen = enumerate(struct)
            add = lambda _, value: new_struct.append(value)
        elif isinstance(struct, set):
            gen = enumerate(struct)
            add = lambda _, value: new_struct.add(value)
        else:  # dict
            gen = six.iteritems(struct)
            add = lambda key, value: new_struct.__setitem__(key, value)

        # recursively fill the new struct
        for key, value in gen:
            value = map_struct(func, value, map_dict=map_dict, map_list=map_list,
                map_tuple=map_tuple, map_set=map_set, cls=cls, custom_mappings=custom_mappings)
            add(key, value)

        # convert tuples
        if isinstance(struct, tuple):
            new_struct = struct.__class__(new_struct)

        return new_struct

    # apply the mapping function on everything else
    else:
        return func(struct)


def mask_struct(mask, struct, replace=no_value):
    """
    Masks a complex structured object *struct* with a *mask* and returns the remaining values. When
    *replace* is set, masked values are replaced with that value instead of being removed. The
    *mask* can have a complex structure as well. Examples:

    .. code-block:: python

        struct = {"a": [1, 2], "b": [3, ["foo", "bar"]]}

        # simple example
        mask_struct({"a": [False, True], "b": False}, struct)
        # => {"a": [2]}

        # omitting mask information results in kept values
        mask_struct({"a": [False, True]}, struct)
        # => {"a": [2], "b": [3, ["foo", "bar"]]}
    """
    # interpret lazy iterables lists
    if is_lazy_iterable(struct):
        struct = list(struct)

    # when mask is a bool, or struct is not a dict or sequence, apply the mask immediately
    if isinstance(mask, bool) or not isinstance(struct, (list, tuple, dict)):
        return struct if mask else replace

    # check list and tuple types
    elif isinstance(struct, (list, tuple)) and isinstance(mask, (list, tuple)):
        new_struct = []
        for i, val in enumerate(struct):
            if i >= len(mask):
                new_struct.append(val)
            else:
                repl = replace
                if isinstance(replace, (list, tuple)) and len(replace) > i:
                    repl = replace[i]
                val = mask_struct(mask[i], val, replace=repl)
                if val != no_value:
                    new_struct.append(val)

        return struct.__class__(new_struct) if new_struct else replace

    # check dict types
    elif isinstance(struct, dict) and isinstance(mask, dict):
        new_struct = struct.__class__()
        for key, val in six.iteritems(struct):
            if key not in mask:
                new_struct[key] = val
            else:
                repl = replace
                if isinstance(replace, dict) and key in replace:
                    repl = replace[key]
                val = mask_struct(mask[key], val, replace=repl)
                if val != no_value:
                    new_struct[key] = val
        return new_struct or replace

    # when this point is reached, mask and struct have incompatible types
    raise TypeError("mask and struct must have the same type, got '{}' and '{}'".format(type(mask),
            type(struct)))


@contextlib.contextmanager
def tmp_file(*args, **kwargs):
    """
    Context manager that creates an empty, temporary file, yields the file descriptor number and
    temporary path, and eventually removes it. All *args* and *kwargs* are passed to
    :py:meth:`tempfile.mkstemp`. The behavior of this function is similar to
    ``tempfile.NamedTemporaryFile`` which, however, yields an already opened file object.
    """
    fileno, path = tempfile.mkstemp(*args, **kwargs)

    # create the file
    with open(path, "w") as f:
        f.write("")

    # yield it
    try:
        yield fileno, path
    finally:
        if os.path.exists(path):
            os.remove(path)


def interruptable_popen(*args, **kwargs):
    """ interruptable_popen(*args, interrupt_callback=None, kill_timeout=None, **kwargs)
    Shorthand to :py:class:`Popen` followed by :py:meth:`Popen.communicate` which can be interrupted
    by *KeyboardInterrupt*. The return code, standard output and standard error are returned in a
    3-tuple.

    *interrupt_callback* can be a function, accepting the process instance as an argument, that is
    called immediately after a *KeyboardInterrupt* occurs. After that, a SIGTERM signal is send to
    the subprocess to allow it to gracefully shutdown.

    When *kill_timeout* is set, and the process is still alive after that period (in seconds), a
    SIGKILL signal is sent to force the process termination.

    All other *args* and *kwargs* are forwarded to the :py:class:`Popen` constructor.
    """
    # get kwargs not being passed to Popen
    interrupt_callback = kwargs.pop("interrupt_callback", None)
    kill_timeout = kwargs.pop("kill_timeout", None)

    # start the subprocess in a new process group
    kwargs["preexec_fn"] = os.setsid
    p = subprocess.Popen(*args, **kwargs)

    # handle interrupts
    try:
        out, err = p.communicate()
    except KeyboardInterrupt:
        # allow the interrupt_callback to perform a custom process termination
        if callable(interrupt_callback):
            interrupt_callback(p)

        # when the process is still alive, send SIGTERM to gracefully terminate it
        pgid = os.getpgid(p.pid)
        if p.poll() is None:
            os.killpg(pgid, signal.SIGTERM)

        # when a kill_timeout is set, and the process is still running after that period,
        # send SIGKILL to force its termination
        if kill_timeout is not None:
            target_time = time.time() + kill_timeout
            while target_time > time.time():
                time.sleep(0.05)
                if p.poll() is not None:
                    # the process terminated, exit the loop
                    break
            else:
                # check the status again to avoid race conditions
                if p.poll() is None:
                    os.killpg(pgid, signal.SIGKILL)

        # transparently reraise
        raise

    if six.PY3:
        if out is not None:
            out = out.decode("utf-8")
        if err is not None:
            err = err.decode("utf-8")

    return p.returncode, out, err


def readable_popen(*args, **kwargs):
    """
    Creates a :py:class:`Popen` object and a generator function yielding the output line-by-line as
    it comes in. All *args* and *kwargs* are forwarded to the :py:class:`Popen` constructor.
    Example:

    .. code-block:: python

        # create the popen object and line generator
        p, lines = readable_popen(["some_executable", "--args"])

        # loop through output lines as they come in
        for line in lines:
            print(line)

        if p.returncode != 0:
            raise Exception("complain ...")

    ``communicate()`` is called automatically after the output iteration terminates which sets the
    subprocess' *returncode* member.
    """
    # force pipes
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.STDOUT

    p = subprocess.Popen(*args, **kwargs)

    def line_gen():
        for line in iter(lambda: p.stdout.readline(), ""):
            if six.PY3:
                line = line.decode("utf-8")
            yield line.rstrip()

        # communicate in the end
        p.communicate()

    return p, line_gen()


def create_hash(inp, l=10, algo="sha256", to_int=False):
    """
    Takes an arbitrary input *inp* and creates a hexadecimal string hash based on an algorithm
    *algo*. For valid algorithms, see python's hashlib. *l* corresponds to the maximum length of the
    returned hash and is limited by the length of the hexadecimal representation produced by the
    hashing algorithm. When *to_int* is *True*, the decimal integer representation is returned.
    """
    h = getattr(hashlib, algo)(six.b(str(inp))).hexdigest()[:l]
    return int(h, 16) if to_int else h


def create_random_string(prefix="", l=10):
    """
    Creates and returns a random string consisting of *l* characters using a uuid4 hash. When
    *prefix* is given, the string will have the format ``<prefix>_<random_string>``.
    """
    s = ""
    while len(s) < l:
        s += uuid.uuid4().hex
    s = s[:l]
    if prefix:
        s = "{}_{}".format(prefix, s)
    return s


def copy_no_perm(src, dst):
    """
    Copies a file from *src* to *dst* including meta data except for permission bits.
    """
    shutil.copyfile(src, dst)
    perm = os.stat(dst).st_mode
    shutil.copystat(src, dst)
    os.chmod(dst, perm)


def makedirs(path, perm=None):
    """
    Recursively creates directories up to *path*. No exception is raised if *path* refers to an
    existing directory. If *perm* is set, the permissions of all newly created directories are set
    to this value.
    """
    # nothing to do when the directory already exists
    if os.path.isdir(path):
        return

    # helper to silently create the directory, catching exceptions if it exists by now
    # (when dropping py2, just use the exist_ok flag of os.makedirs)
    def makedirs_safe(path, perm=None):
        try:
            if perm is None:
                os.makedirs(path)
            else:
                os.makedirs(path, perm)
        except Exception as e:
            if not is_file_exists_error(e):
                raise

    if perm is None:
        makedirs_safe(path)
    else:
        umask = os.umask(0)
        try:
            makedirs_safe(path, perm)
        finally:
            os.umask(umask)


def user_owns_file(path, uid=None):
    """
    Returns whether a file located at *path* is owned by the user with *uid*. When *uid* is *None*,
    the user id of the current process is used.
    """
    if uid is None:
        uid = os.getuid()
    path = os.path.expandvars(os.path.expanduser(path))
    return os.stat(path).st_uid == uid


def iter_chunks(l, size):
    """
    Returns a generator containing chunks of *size* of a list, integer or generator *l*. A *size*
    smaller than 1 results in no chunking at all.
    """
    if isinstance(l, six.integer_types):
        l = six.moves.range(l)

    if is_lazy_iterable(l):
        if size < 1:
            yield list(l)
        else:
            chunk = []
            for elem in l:
                if len(chunk) < size:
                    chunk.append(elem)
                else:
                    yield chunk
                    chunk = [elem]
            else:
                if chunk:
                    yield chunk

    else:
        if size < 1:
            yield l
        else:
            for i in six.moves.range(0, len(l), size):
                yield l[i:i + size]


byte_units = ["bytes", "kB", "MB", "GB", "TB", "PB", "EB"]


def human_bytes(n, unit=None, fmt=False):
    """
    Takes a number of bytes *n*, assigns the best matching unit and returns the respective number
    and unit string in a tuple. When *unit* is set, that unit is used. When *fmt* is set, it is
    expected to be a string template with two elements that are filled via *str.format*. It can also
    be a boolean value in which case the template defaults to ``"{:.1f} {}"`` when *True*. Example:

    .. code-block:: python

        human_bytes(3407872)
        # -> (3.25, "MB")

        human_bytes(3407872, "kB")
        # -> (3328.0, "kB")

        human_bytes(3407872, fmt="{:.2f} -- {}")
        # -> "3.25 -- MB"

        human_bytes(3407872, fmt=True)
        # -> "3.25 MB"
    """
    # check if the unit exists
    if unit and unit not in byte_units:
        raise ValueError("unknown unit '{}', valid values are {}".format(unit, byte_units))

    if n == 0:
        idx = 0
    elif unit:
        idx = byte_units.index(unit)
    else:
        idx = int(math.floor(math.log(abs(n), 1024)))
        idx = min(idx, len(byte_units))

    # get the value and the unit name
    value = n / 1024. ** idx
    unit = byte_units[idx]

    if fmt:
        if not isinstance(fmt, six.string_types):
            fmt = "{:.1f} {}"
        return fmt.format(value, unit)
    else:
        return value, unit


def parse_bytes(s, input_unit="bytes", unit="bytes"):
    """
    Takes a string *s*, interprets it as a size with an optional unit, and returns a float that
    represents that size in a given *unit*. When no unit is found in *s*, *input_unit* is used as a
    default. A *ValueError* is raised, when *s* cannot be successfully converted. Example:

    .. code-block:: python

        parse_bytes("100")
        # -> 100.

        parse_bytes("2048", unit="kB")
        # -> 2.

        parse_bytes("2048 kB", unit="kB")
        # -> 2048.

        parse_bytes("2048 kB", unit="MB")
        # -> 2.

        parse_bytes("2048", "kB", unit="MB")
        # -> 2.

        parse_bytes(2048, "kB", unit="MB")  # note the float type of the first argument
        # -> 2.
    """
    # check if the units exists
    if input_unit not in byte_units:
        raise ValueError("unknown input_unit '{}', valid values are {}".format(
            input_unit, byte_units))
    if unit not in byte_units:
        raise ValueError("unknown unit '{}', valid values are {}".format(
            unit, byte_units))

    # when s is a number, interpret it as bytes right away
    # otherwise parse it
    if isinstance(s, (float, six.integer_types)):
        input_value = float(s)
    else:
        m = re.match(r"^\s*(-?\d+\.?\d*)\s*(|{})\s*$".format("|".join(byte_units)), s)
        if not m:
            raise ValueError("cannot parse bytes from string '{}'".format(s))

        input_value, _input_unit = m.groups()
        input_value = float(input_value)
        if _input_unit:
            input_unit = _input_unit

    # convert the input value to bytes
    idx = byte_units.index(input_unit)
    size_bytes = input_value * 1024. ** idx

    # use human_bytes to convert the size
    return human_bytes(size_bytes, unit)[0]


time_units = collections.OrderedDict([
    ("week", 7 * 24 * 60 * 60),
    ("day", 24 * 60 * 60),
    ("hour", 60 * 60),
    ("minute", 60),
    ("second", 1),
])

time_unit_aliases = {
    "w": "week",
    "weeks": "week",
    "d": "day",
    "days": "day",
    "h": "hour",
    "hours": "hour",
    "m": "minute",
    "min": "minute",
    "mins": "minute",
    "minutes": "minute",
    "s": "second",
    "sec": "second",
    "secs": "second",
    "seconds": "second",
}


def human_duration(colon_format=False, plural=True, **kwargs):
    """ human_duration
    Returns a human readable duration. The largest unit is days. When *colon_format* is *True*, the
    return value has the format ``"[d-][hh:]mm:ss[.ms]"``. *colon_format* can also be a string value
    referring to a limiting  unit. In that case, the returned time string has no field above that
    unit, e.g. passing ``"m"`` results in a string ``"mm:ss[.ms]"`` where the minute field is
    potentially larger than 60. Passing ``"s"`` is a special case. Since the colon format always has
    a minute field (to mark it as colon format in the first place), the returned string will have
    the format ``"00:ss[.ms]"``. Unless *plural* is *False*, units corresponding to values other
    than **exactly** one are used in plural e.g. ``"1 second"`` but ``"1.5 seconds"``. All other
    *kwargs* are passed to ``datetime.timedelta`` to get the total duration in seconds. Example:

    .. code-block:: python

    human_duration(seconds=1233)
    # -> "20 minutes, 33 seconds"

    human_duration(seconds=90001)
    # -> "1 day, 1 hour, 1 second"

    human_duration(seconds=1233, colon_format=True)
    # -> "20:33"

    human_duration(seconds=-1233, colon_format=True)
    # -> "-20:33"

    human_duration(seconds=90001, colon_format=True)
    # -> "1-01:00:01"

    human_duration(seconds=90001, colon_format="h")
    # -> "25:00:01"

    human_duration(seconds=65, colon_format="s")
    # -> "00:65"

    human_duration(minutes=15, colon_format=True)
    # -> "15:00"

    human_duration(minutes=15)
    # -> "15 minutes"

    human_duration(minutes=15, plural=False)
    # -> "15 minute"

    human_duration(minutes=-15)
    # -> "minus 15 minutes"
    """
    _time_units = ["day", "hour", "minute", "second"]

    seconds = float(datetime.timedelta(**kwargs).total_seconds())
    sign = 1 if seconds >= 0 else -1
    seconds = abs(seconds)

    # when using colon_format, check if a limiting unit is set
    colon_unit_limit = None
    if isinstance(colon_format, six.string_types):
        colon_unit_limit = time_unit_aliases.get(colon_format, colon_format)
        if colon_unit_limit not in _time_units:
            raise ValueError("unknown colon_format unit '{}', valid values are {}".format(
                colon_unit_limit, ",".join(_time_units)))
        colon_unit_index = _time_units.index(colon_unit_limit)

    # start building the human readable string
    # loop through units, remove the fully dividable part and let the next unit handle the rest
    human_str = ""
    for i, unit in enumerate(_time_units):
        # skip this iteration when a colon unit limit is set
        if colon_unit_limit and i < colon_unit_index:
            continue

        # build the value for this unit
        if unit == "second":
            # try to round to 2 digits or convert to int
            value = try_int(round(seconds, 2))
        else:
            # get the integer divider and adjust the remaining number of seconds
            mul = time_units[unit]
            value = int(seconds // mul)
            seconds -= value * mul

        # keep zeros under certain conditions
        if value == 0:
            if colon_format:
                keep_zero = human_str or unit == "second" or colon_unit_limit
            else:
                keep_zero = not human_str and unit == "second"
            if not keep_zero:
                continue

        # build the human readable representation
        if colon_format:
            if unit == "second":
                # special case 1: force float formatting with optional leading 0
                fmt = "0{}" if value < 10 else "{}"
                # special case 2: when "minutes" are no there yet, prepend "00:"
                if not human_str:
                    fmt = "00:" + fmt
            elif unit in ["hour", "minute"]:
                fmt = "{:02d}:"
            else:  # day
                fmt = "{}-"
            human_str += fmt.format(value)
        else:
            if human_str:
                human_str += ", "
            human_str += "{} {}{}".format(value, unit, "" if (value == 1 or not plural) else "s")

    # sign
    if sign == -1:
        human_str = ("-" if colon_format else "minus ") + human_str

    return human_str


def parse_duration(s, input_unit="s", unit="s"):
    """
    Takes a string *s*, interprets it as a duration with an optional unit, and returns a float that
    represents that size in a given *unit*. When no unit is found in *s*, *input_unit* is used as a
    default. A *ValueError* is raised, when *s* cannot be successfully converted. Multiple input
    formats are parsed: Example:

    .. code-block:: python

        # plain number
        parse_duration(100)
        # -> 100.0

        parse_duration(100, unit="min")
        # -> 1.667

        parse_duration(100, input_unit="min")
        # -> 6000.0

        parse_duration(-100, input_unit="min")
        # -> -6000.0

        # strings in the format [d-][h:][m:]s[.ms] are interpreted with input_unit disregarded
        parse_duration("2:1")
        # -> 121.0

        parse_duration("04:02:01.1")
        # -> 14521.1

        parse_duration("04:02:01.1", unit="min")
        # -> 242.0183

        parse_duration("0-4:2:1.1")
        # -> 14521.1

        # human-readable string, optionally multiple of them separated by comma
        # missing units are interpreted as input_unit, unit works as above
        parse_duration("10 mins")
        # -> 600.0

        parse_duration("10 mins", unit="min")
        # -> 10.0

        parse_duration("10", unit="min")
        # -> 0.167

        parse_duration("10", input_unit="min", unit="min")
        # -> 10.0

        parse_duration("10 mins, 15 secs")
        # -> 615.0

        parse_duration("10 mins and 15 secs")
        # -> 615.0

        parse_duration("minus 10 mins and 15 secs")
        # -> -615.0
    """
    # consider unit aliases
    input_unit = time_unit_aliases.get(input_unit, input_unit)
    unit = time_unit_aliases.get(unit, unit)

    # check units
    if input_unit not in time_units:
        raise ValueError("unknown input_unit '{}', valid values are {}".format(
            input_unit, ",".join(time_units)))
    if unit not in time_units:
        raise ValueError("unknown unit '{}', valid values are {}".format(
            unit, ",".join(time_units)))

    sign = 1
    duration_seconds = 0.0

    # number or string?
    if isinstance(s, six.integer_types + (float,)):
        duration_seconds += s * time_units[input_unit]
    else:
        s = s.strip()

        # identify the format "[d-][h:][m:]s[.ms]" first
        m = re.match(r"^([+-])?((((((\d+)-)?(\d+)):)?(\d+)):)?(\d+)(\.(\d*))?$", s)
        if m:
            sgn, d, h, m, s, ms = [m.group(i) for i in [1, 7, 8, 9, 10, 11]]

            # interpret leading "-" or "+" as the sign of the duration
            if sgn == "-":
                sign = -1

            # add to seconds
            if d:
                duration_seconds += float(d) * time_units["day"]
            if h:
                duration_seconds += float(h) * time_units["hour"]
            if m:
                duration_seconds += float(m) * time_units["minute"]
            duration_seconds += float(s)
            if ms:
                duration_seconds += float(ms)

        else:
            # human readable format
            # interpret leading "+", "-", "plus" and "minus" as the sign of the duration
            m = re.match(r"^(\+|\-|plus\s|minus\s)\s*(.*)$", s)
            if m:
                sign = 1 if m.group(1) in ("plus ", "+") else -1
                s = m.group(2)

            # replace "and" with comma, replace multiple commas with one, then split
            s = re.sub(r"\,+", ",", s.replace("and", ","))
            parts = s.split(",")

            units = list(time_units.keys()) + list(time_unit_aliases.keys())
            cre = re.compile(r"^\s*(\d+|\d+\.|\.\d+|\d+\.\d+)\s*(|{})\s*$".format("|".join(units)))

            # convert each part
            for part in parts:
                part = part.strip()
                if not part:
                    continue

                m = cre.match(part)
                if not m:
                    raise ValueError("cannot parse duration string '{}'".format(s))

                d, u = m.groups()
                d = float(d)
                if not u:
                    u = input_unit
                u = time_unit_aliases.get(u, u)

                duration_seconds += d * time_units[u]

    # convert to output unit
    duration = sign * duration_seconds / time_units[unit]

    return duration


def is_file_exists_error(e):
    """
    Returns whether the exception *e* was raised due to an already existing file or directory.
    """
    if six.PY3:
        return isinstance(e, FileExistsError)  # noqa: F821
    else:
        return isinstance(e, OSError) and e.errno == 17


def send_mail(recipient, sender, subject="", content="", smtp_host="127.0.0.1", smtp_port=25):
    """
    Lightweight mail functionality. Sends an mail from *sender* to *recipient* with *subject* and
    *content*. *smtp_host* and *smtp_port* are forwarded to the ``smtplib.SMTP`` constructor. *True*
    is returned on success, *False* otherwise.
    """
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
    except Exception as e:
        logger.warning("cannot create SMTP server: {}".format(e))
        return False

    header = "From: {}\r\nTo: {}\r\nSubject: {}\r\n\r\n".format(sender, recipient, subject)
    server.sendmail(sender, recipient, header + content)

    return True


class DotDict(collections.OrderedDict):
    """
    Subclass of *OrderedDict* that provides read access for items via attributes by implementing
    ``__getattr__``. In case a item is accessed via attribute and it does not exist, an
    *AttriuteError* is raised rather than a *KeyError*. Example:

    .. code-block:: python

       d = DotDict()
       d["foo"] = 1

       print(d["foo"])
       # => 1

       print(d.foo)
       # => 1

       print(d["bar"])
       # => KeyError

       print(d.bar)
       # => AttributeError
    """

    def __getattr__(self, attr):
        if attr == "_OrderedDict__root":
            return super(DotDict, self).__getattr__(attr)
        else:
            try:
                return self[attr]
            except KeyError:
                raise AttributeError("'{}' object has no attribute '{}'".format(
                    self.__class__.__name__, attr))


class ShorthandDict(collections.OrderedDict):
    """
    Subclass of *OrderedDict* that implements ``__getattr__`` and ``__setattr__`` for a configurable
    list of attributes. Example:

    .. code-block:: python

        MyDict(ShorthandDict):
            attributes = {"foo": 1, "bar": 2}

        d = MyDict(foo=9)

        print(d.foo)
        # => 9

        print(d.bar)
        # => 2

        d.foo = 3
        print(d.foo)
        # => 3

    .. py:classattribute: attributes
       type: dict

       Mapping of attribute names to default values. ``__getattr__`` and ``__setattr__`` support is
       provided for these attributes.
    """

    attributes = {}

    def __init__(self, **kwargs):
        super(ShorthandDict, self).__init__()

        for attr, default in six.iteritems(self.attributes):
            self[attr] = kwargs.pop(attr, copy.deepcopy(default))

        self.update(kwargs)

    def copy(self):
        """"""
        kwargs = {key: copy.deepcopy(value) for key, value in six.iteritems(self)}
        return self.__class__(**kwargs)

    def __getattr__(self, attr):
        if attr in self.attributes:
            return self[attr]
        else:
            return super(ShorthandDict, self).__getattr__(attr)

    def __setattr__(self, attr, value):
        if attr in self.attributes:
            self[attr] = value
        else:
            super(ShorthandDict, self).__setattr__(attr, value)


class InsertableDict(collections.OrderedDict):
    """
    Subclass of *OrderedDict* that supports inserting elements before or after certain keys.
    Example:

    .. code-block:: python

        d = InsertableDict(foo=123, bar=456)

        d.insert_before("bar", "test", 999)
        print(d)  # -> InsertableDict([('foo', 123), ('test', 999), ('bar', 456)])

        d.insert_after("test", "foo", "new_value")
        print(d)  # -> InsertableDict([('test', 999), ('foo', 'new_value'), ('bar', 456)])
    """

    def _insert(self, search_key, key, value, offset):
        # when key is a list or dict and value is None, assume key refers to key-value pairs
        if isinstance(key, (list, dict)) and value is None:
            new_items = key.items() if isinstance(key, dict) else key
            new_keys = [k for k, v in new_items]
        else:
            new_items = [(key, value)]
            new_keys = [key]

        # if the search key is not present, insert the new pairs and finish
        if search_key not in self:
            self.update(new_items)
            return

        # create a copy if the index
        items = list(self.items())

        # find the position where to insert
        pos = items.index((search_key, self[search_key])) + offset

        # construct the new items without duplicates
        items = [
            (k, v) for k, v in items[:pos]
            if k not in new_keys
        ] + new_items + [
            (k, v) for k, v in items[pos:]
            if k not in new_keys
        ]

        # rebuild the index
        self.clear()
        self.update(items)

    def insert_before(self, before_key, key, value=None):
        """
        Inserts a *key* - *value* pair before the key *before_key*. When this key does not exist,
        the new pair is added to the end. When *key* is list or dictionary and value is *None*,
        multiple new values are inserted.
        """
        self._insert(before_key, key, value, 0)

    def insert_after(self, after_key, key, value=None):
        """
        Inserts a *key* - *value* pair after the key *after_key*. When this key does not exist, the
        new pair is added to the end. When *key* is list or dictionary and value is *None*,
        multiple new values are inserted.
        """
        self._insert(after_key, key, value, 1)


def open_compat(*args, **kwargs):
    """
    Polyfill for python's ``open`` factory, returning the plain ``open`` in python 3, and
    ``io.open`` in python 2 with a patched ``write`` method that internally handles unicode
    conversion of its first argument. All *args* and *kwargs* are forwarded.
    """
    if six.PY3:
        return open(*args, **kwargs)

    else:
        f = io.open(*args, **kwargs)

        if f.encoding and f.encoding.lower().replace("-", "") == "utf8":
            write_orig = f.write

            def write(data, *args, **kwargs):
                u = unicode  # noqa: F821
                if not isinstance(data, u):
                    data = u(data)
                return write_orig(data, *args, **kwargs)

            f.write = write

        return f


@contextlib.contextmanager
def patch_object(obj, attr, value, lock=False):
    """
    Context manager that temporarily patches an object *obj* by replacing its attribute *attr* with
    *value*. The original value is set again when the context is closed. When *lock* is *True*, the
    py:attr:`default_lock` object is used to ensure the patch is thread-safe. When *lock* is a lock
    instance, this object is used instead.
    """
    orig = getattr(obj, attr, no_value)

    if lock:
        if isinstance(lock, bool):
            lock = default_lock
        lock.acquire()

    try:
        setattr(obj, attr, value)

        yield obj
    finally:
        if lock and lock.locked():
            lock.release()

        try:
            if orig is no_value:
                delattr(obj, attr)
            else:
                setattr(obj, attr, orig)
        except:
            pass


def join_generators(*generators, **kwargs):
    """ join_generators(*generators, on_error=None)
    Joins multiple *generators* and returns a single generator for simplified iteration. Yielded
    objects are transparently sent back to ``yield`` assignments of the same generator. When
    *on_error* is callable, it is invoked in case an exception is raised while iterating, including
    *KeyboardInterrupt*'s. If its return value evaluates to *True*, the state is reset and
    iterations continue. Otherwise, the exception is raised.
    """
    on_error = kwargs.get("on_error")
    for gen in generators:
        last_result = no_value
        while True:
            try:
                if last_result == no_value:
                    last_result = yield six.next(gen)
                else:
                    last_result = yield gen.send(last_result)
            except StopIteration:
                break
            except (Exception, KeyboardInterrupt) as error:
                if callable(on_error) and on_error(error):
                    last_result = no_value
                else:
                    raise


def quote_cmd(cmd):
    """
    Takes a shell command *cmd* given as a list and returns a single string representation of that
    command with proper quoting. To denote nested commands (such as shown below), *cmd* can also
    contain nested lists. Example:

    .. code-block:: python

        print(quote_cmd(["bash", "-c", "echo", "foobar"]))
        # -> "bash -c echo foobar"

        print(quote_cmd(["bash", "-c", ["echo", "foobar"]]))
        # -> "bash -c 'echo foobar'"
    """
    # expand lists recursively
    cmd = [
        (quote_cmd(part) if isinstance(part, (list, tuple)) else str(part))
        for part in cmd
    ]

    # quote all parts and join
    return " ".join(six.moves.shlex_quote(part) for part in cmd)


def escape_markdown(s):
    """
    Escapes all characters in a string *s* that coupld be confused for markdown formatting strings
    and returns it.
    """
    return re.sub(r"([^\\]?)(\(|\)|=|\.|_|-)", r"\1\\\2", s)


class ClassPropertyDescriptor(object):
    """
    Generic descriptor class that is used by :py:func:`classproperty`. Setters are currently not
    supported.
    """

    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, obj, cls=None):
        if cls is None:
            cls = type(obj)

        return self.fget.__get__(obj, cls)()

    def __set__(self, obj, value):
        if not self.fset:
            raise AttributeError("can't set attribute")

        type_ = type(obj)

        return self.fset.__get__(obj, type_)(value)


def classproperty(func):
    """
    Propety decorator for class-level methods.
    """
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)

    return ClassPropertyDescriptor(func)


class BaseStream(object):

    FLUSH_AFTER_WRITE = True

    def __init__(self, flush_after_write=None):
        super(BaseStream, self).__init__()

        self.closed = False
        self.flush_after_write = flush_after_write

    @property
    def _flush_after_write(self):
        return self.FLUSH_AFTER_WRITE if self.flush_after_write is None else self.flush_after_write

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        if not self.closed:
            self.flush()

            self._close()
            self.closed = True

    def flush(self):
        if not self.closed:
            self._flush()

    def write(self, *args, **kwargs):
        if not self.closed:
            self._write(*args, **kwargs)

            if self._flush_after_write:
                self.flush()

    def _close(self):
        return

    def _flush(self):
        return

    def _write(self, *args, **kwargs):
        return


class TeeStream(BaseStream):
    """ __init__(*consumers, mode="w", **kwargs)
    Multi-stream object that forwards calls to :py:meth:`write` and :py:meth:`flush` to all
    registered *consumer* streams. When a *consumer* is a string, it is interpreted as a file which
    is opened for writing (similar to *tee* in bash). All *kwargs* are forwarded to the
    :py:class:`BaseStream` constructor.

    Example:

    .. code-block:: python

        tee = TeeStream("/path/to/log.txt", sys.__stdout__)
        sys.stdout = tee
    """

    def __init__(self, *consumers, **kwargs):
        mode = kwargs.pop("mode", "w")

        super(TeeStream, self).__init__(**kwargs)

        self.consumers = []
        self.open_files = []

        for consumer in consumers:
            # interpret strings as file paths
            if isinstance(consumer, six.string_types):
                consumer = open_compat(consumer, mode)
                self.open_files.append(consumer)
            self.consumers.append(consumer)

    def _close(self):
        """
        Closes opened files.
        """
        for f in self.open_files:
            f.close()

    def _flush(self):
        """
        Flushes all registered consumer streams.
        """
        for consumer in self.consumers:
            if not getattr(consumer, "closed", False):
                consumer.flush()

    def _write(self, *args, **kwargs):
        """
        Writes to all registered consumer streams, passing *args* and *kwargs*.
        """
        for consumer in self.consumers:
            consumer.write(*args, **kwargs)


class FilteredStream(BaseStream):
    """
    Stream object that accepts in input *stream* and a function *filter_fn* which is called upon
    every call to :py:meth:`write`. The payload is written when the returned value evaluates to
    *True*. All *kwargs* are forwarded to the :py:class:`BaseStream` constructor.
    """

    def __init__(self, stream, filter_fn, **kwargs):
        super(FilteredStream, self).__init__(**kwargs)
        self.stream = stream
        self.filter_fn = filter_fn

    def _close(self):
        """
        Closes the consumer stream.
        """
        self.stream.close()

    def _flush(self):
        """
        Flushes the consumer stream.
        """
        if not getattr(self.stream, "closed", False):
            self.stream.flush()

    def _write(self, *args, **kwargs):
        """
        Writes to the consumer stream when *filter_fn* evaluates to *True*, passing *args* and
        *kwargs*.
        """
        if self.filter_fn(*args, **kwargs):
            self.stream.write(*args, **kwargs)
