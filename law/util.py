# coding: utf-8

"""
Helpful utility functions.
"""

from __future__ import annotations

__all__ = [
    # singleton values
    "default_lock", "io_lock", "console_lock", "no_value",
    # path and task helpers
    "rel_path", "law_src_path", "law_home_path", "law_run", "common_task_params",
    # generic helpers
    "abort", "import_file", "get_terminal_width", "custom_context", "empty_context", "which",
    "create_hash", "create_random_string", "iter_chunks", "quote_cmd", "escape_markdown",
    "send_mail", "patch_object", "join_generators",
    # value identification and conversion
    "is_classmethod", "is_number", "is_float", "try_int", "round_discrete", "str_to_int",
    "flag_to_bool", "colored", "uncolored", "query_choice", "is_pattern",
    "human_bytes", "parse_bytes", "human_duration", "parse_duration",
    # sequence helpers
    "brace_expand", "range_expand", "range_join", "multi_match", "is_iterable",
    "is_lazy_iterable", "make_list", "make_tuple", "make_set", "make_unique", "is_nested",
    "flatten", "merge_dicts", "unzip", "map_verbose", "map_struct", "mask_struct",
    # io helpers
    "tmp_file", "interruptable_popen", "readable_popen", "copy_no_perm", "makedirs",
    "user_owns_file",
    # classes
    "DotDict", "ShorthandDict", "classproperty", "BaseStream", "TeeStream", "FilteredStream",
]

import os
import sys
import re
import math
import fnmatch
import itertools
import functools
import tempfile
import subprocess
import signal
import hashlib
import uuid
import shutil
import pathlib
import copy
import contextlib
import smtplib
import time
import datetime
import random
import threading
import shlex
import inspect
import logging

from law._types import (
    ModuleType, Any, Sequence, Callable, Iterable, GeneratorType, MappingView, T, Iterator,
    Generator, Hashable, AbstractContextManager, TracebackType, GenericAlias,
)

ipykernel: ModuleType | None = None
try:
    import ipykernel
    import ipykernel.iostream
except ImportError:
    pass

try:
    import google.colab  # type: ignore[import-untyped, import-not-found] # noqa
    ON_COLAB = True
except ImportError:
    ON_COLAB = False


logger = logging.getLogger(__name__)

# some globally usable thread locks
default_lock = threading.Lock()
io_lock = threading.Lock()
console_lock = threading.Lock()


class NoValue(object):

    _instance: NoValue | None = None

    def __new__(cls, *args, **kwargs) -> NoValue:
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, NoValue)

    def __bool__(self) -> bool:
        return False

    def __nonzero__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return f"{self.__module__}.no_value"

    def __str__(self) -> str:
        return "no_value"


#: Unique dummy value that is used to denote missing values and always evaluates to *False*.
no_value = NoValue()


def rel_path(anchor: str, *paths: Any) -> str:
    """
    Returns a path made of framgment *paths* relativ to an *anchor* path. When *anchor* is a file,
    its absolute directory is used instead.
    """
    anchor = os.path.abspath(os.path.expandvars(os.path.expanduser(str(anchor))))
    if os.path.exists(anchor) and os.path.isfile(anchor):
        anchor = os.path.dirname(anchor)
    return os.path.normpath(os.path.join(anchor, *map(str, paths)))


def law_src_path(*paths: Any) -> str:
    """
    Returns the law installation directory, optionally joined with *paths*.
    """
    return rel_path(__file__, paths)


def law_home_path(*paths: Any) -> str:
    """
    Returns the law home directory, optionally joined with *paths*.
    """
    from law.config import law_home_path
    return law_home_path(*paths)


def law_run(argv: Sequence[str], **kwargs) -> int:
    """
    Runs a task with certain parameters as defined in *argv*, which can be a string or a list of
    strings. It must start with the family of the task to run, followed by the desired parameters.
    All *kwargs* are forwarded to :py:func:`luigi.interface.run`. Example:

    .. code-block:: python

        law_run(["MyTask", "--param", "value"])
        law_run("MyTask --param value")
    """
    from luigi.interface import run as luigi_run  # type: ignore[import-untyped]
    from luigi.cmdline_parser import CmdlineParser  # type: ignore[import-untyped]
    from law.parser import _reset as reset_parser

    # ensure that argv is a list of strings
    if isinstance(argv, str):
        argv = shlex.split(argv)
    else:
        argv = [str(arg) for arg in argv]

    # luigi's pid locking must be disabled
    argv.append("--no-lock")

    # run with a patch to the ArgumentParser to overwrite the prog default
    _build_parser_orig = CmdlineParser._build_parser
    @functools.wraps(_build_parser_orig)
    def _build_parser(*args, **kwargs) -> CmdlineParser:
        parser = _build_parser_orig(*args, **kwargs)
        parser.prog = "law run"
        return parser

    ret = False
    try:
        with patch_object(
            CmdlineParser,
            "_build_parser",
            staticmethod(_build_parser),
            orig=staticmethod(_build_parser_orig),
        ):
            ret = luigi_run(argv, **kwargs)
    finally:
        # reset parser objects
        reset_parser()

    return ret


def abort(msg: str | None = None, exitcode: int = 1, color: bool = True) -> int:
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
            print(msg, file=sys.stderr)
    sys.exit(exitcode)
    return exitcode


def import_file(path: str | pathlib.Path, attr: str | None = None) -> ModuleType | Any:
    """
    Loads the content of a python file located at *path* and returns its package content as a
    dictionary. When *attr* is set, only the attribute with that name is returned.

    The file is not required to be importable as its content is loaded directly into the
    interpreter. While this approach is not necessarily clean, it can be useful in places where
    custom code must be loaded.
    """
    # load the package contents
    path = os.path.expandvars(os.path.expanduser(str(path)))
    pkg = DotDict()
    with open(path, "r") as f:
        exec(f.read(), pkg)

    # extract a particular attribute
    if attr:
        if attr not in pkg:
            raise AttributeError(f"no local member '{attr}' found in file {path}")
        return pkg[attr]

    return pkg


def get_terminal_width(fallback: bool = False) -> int | None:
    """
    Returns the terminal width when possible, and *None* otherwise. By default, the width is
    obtained through ``os.get_terminal_size``, querying the *sys.__stdout__* which might fail in
    case no valid output device is connected. However, when *fallback* is *True*,
    ``shutil.get_terminal_size`` is used instead, which priotizes the *COLUMNS* variable if set.
    """
    width = None
    func = getattr(shutil if fallback else os, "get_terminal_size", None)
    if callable(func):
        try:
            width = func().columns
        except OSError:
            pass

    return width


def is_classmethod(func: Any, cls: type | None = None) -> bool:
    """
    Returns *True* if *func* is a classmethod of *cls*, and *False* otherwise. When *cls* is *None*,
    it is extracted from the function's qualified name and module name.
    """
    # when no cls is given, try to lookup it up in its associated module
    _hasattr = lambda attr: getattr(func, attr, None) is not None
    if cls is None:
        if _hasattr("__qualname__") and _hasattr("__module__") and "." in func.__qualname__:
            cls_name = func.__qualname__.rsplit(".", 1)[0]
            cls = getattr(sys.modules.get(func.__module__), cls_name, None)

    # when no class exists at this point, func cannot be a classmethod
    if cls is None:
        return False

    # func requires a __name__
    if not _hasattr("__name__"):
        raise AttributeError(f"func '{func}' has not attribute __name__")

    # func must be the class attribute with that name
    if getattr(cls, func.__name__, None) != func:
        return False

    # finally, find the attribute in the __dict__ of cls or its super classes and check the type
    try:
        for _cls in inspect.getmro(cls):
            if func.__name__ not in _cls.__dict__:
                continue
            return cls.__dict__[func.__name__].__class__.__name__ == "classmethod"
    except AttributeError:
        return False

    return False


def is_number(n: Any) -> bool:
    """
    Returns *True* if *n* is a number, i.e., integer or float, and in particular no boolean.
    """
    return isinstance(n, (int, float)) and not isinstance(n, bool)


def is_float(v: Any) -> bool:
    """
    Takes any value *v* and tries to convert it to a float. Returns *True* success, and *False*
    otherwise.
    """
    try:
        float(v)
        return True
    except:
        return False


def try_int(n: int | float) -> int | float:
    """
    Takes a number *n* and tries to convert it to an integer. When *n* has no decimals, an integer
    is returned with the same value as *n*. Otherwise, a float is returned.
    """
    n_int = int(n)
    return n_int if n == n_int else n


def round_discrete(
    n: int | float,
    base: int | float = 1.0,
    round_fn: Callable[[int | float], float] | str = round,
) -> float:
    """ round_discrete(n, base=1.0, round_fn="round")
    Rounds a number *n* to a discrete *base*. *round_fn* can be a function used for rounding and
    defaults to the built-in ``round`` function. It also accepts string values ``"round"``,
    ``"floor"`` and ``"ceil"`` which are resolved to the corresponding math functions. Example:

    .. code-block:: python

        round_discrete(17, 5)
        # -> 15.0

        round_discrete(17, 2.5)
        # -> 17.5

        round_discrete(17, 2.5)
        # -> 17.5

        round_discrete(17, 2.5, math.floor)
        round_discrete(17, 2.5, "floor")
        # -> 15.0
    """
    if isinstance(round_fn, str):
        if round_fn == "round":
            round_fn = round
        elif round_fn == "floor":
            round_fn = math.floor
        elif round_fn == "ceil":
            round_fn = math.ceil
        else:
            raise ValueError("unknown round function '{}'".format(round_fn))

    return base * round_fn(float(n) / base)


def str_to_int(s: str) -> int:
    """
    Converts a string *s* into an integer under consideration of binary, octal, decimal and
    hexadecimal representations, such as ``"0o0660"``.
    """
    s = str(s).lower()
    m = re.match(r"^0(b|o|d|x)\d+$", s)
    base = {"b": 2, "o": 8, "d": 10, "x": 16}[m.group(1)] if m else 10
    return int(s, base=base)


def flag_to_bool(s: str | bool, silent: bool = False) -> bool | None:
    """
    Takes a string flag *s* and returns whether it evaluates to *True* (values ``"1"``, ``"true"``
    ``"yes"``, ``"y"``, ``"on"``, case-insensitive) or *False* (values ``"0"``, ``"false"``,
    `"no"``, ``"n"``, ``"off"``, case-insensitive). When *s* is already a boolean, it is returned
    unchanged. An error is thrown when *s* is neither of the allowed values and *silent* is *False*.
    Otherwise, *None* is returned.
    """
    if isinstance(s, bool):
        return s

    if isinstance(s, str):
        if s.lower() in ("true", "1", "yes", "y", "on"):
            return True
        if s.lower() in ("false", "0", "no", "n", "off"):
            return False

    if silent:
        return None

    raise ValueError(f"cannot convert to bool: {s}")


def custom_context(obj: T) -> Callable[[], AbstractContextManager[T]]:
    """
    Yields an empty context that can be used in case of dynamically choosing context managers while
    maintaining code structure.
    """
    @contextlib.contextmanager
    def context() -> Iterator[T]:
        yield obj

    return context


@contextlib.contextmanager
def empty_context() -> Iterator[None]:
    """
    Yields an empty context that can be used in case of dynamically choosing context managers while
    maintaining code structure.
    """
    yield


def common_task_params(task_instance, task_cls) -> dict[str, Any]:
    """
    Returns the parameters that are common between a *task_instance* and a *task_cls* in a
    dictionary with values taken directly from the task instance. The difference with respect to
    ``luigi.util.common_params`` is that the values are not parsed using the parameter objects of
    the task class, which might be faster for some purposes.
    """
    task_cls_param_names = set(name for name, _ in task_cls.get_params())
    common_param_names = [
        name
        for name, _ in task_instance.get_params()
        if name in task_cls_param_names
    ]
    return {name: getattr(task_instance, name) for name in common_param_names}


colors: dict[str, int] = {
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

backgrounds: dict[str, int] = {
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

styles: dict[str, int] = {
    "default": 0,
    "bright": 1,
    "dim": 2,
    "underlined": 4,
    "blink": 5,
    "inverted": 7,
    "hidden": 8,
}

uncolor_cre = re.compile(r"(\x1B\[[0-?]*[ -/]*[@-~])")


def colored(
    msg: Any,
    color: str | int | None = None,
    background: str | int | None = None,
    style: str | int | Sequence[str | int] | None = None,
    force: bool = False,
) -> str:
    """
    Return the colored version of a string *msg*. For *color*, *background* and *style* options, see
    https://misc.flogisoft.com/bash/tip_colors_and_formatting. They can also be explicitely set to
    ``"random"`` to get a random value. *style* also accepts a sequence of styles that will be
    stacked. Unless *force* is *True*, the *msg* string is returned unchanged in case the output is
    neither a tty nor an IPython output stream.
    """
    msg = str(msg)

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

    if isinstance(color, str):
        if color == "random":
            color = random.choice(list(colors.values()))
        else:
            colors.get(color, colors["default"])
    elif not isinstance(color, int):
        color = colors["default"]

    if isinstance(background, str):
        if background == "random":
            background = random.choice(list(backgrounds.values()))
        else:
            background = backgrounds.get(background, backgrounds["default"])
    elif not isinstance(background, int):
        background = backgrounds["default"]

    _styles = []
    for s in make_list(style):
        if isinstance(s, str):
            if s == "random":
                s = random.choice(list(styles.values()))
            else:
                s = styles.get(s, styles["default"])
        elif not isinstance(s, int):
            s = styles["default"]
        _styles.append(s)
    style = ";".join(map(str, _styles))

    return f"\033[{style};{background};{color}m{msg}\033[0m"


def uncolored(s: str) -> str:
    """
    Removes all color codes from a string *s* and returns it.
    """
    return uncolor_cre.sub("", s)


def query_choice(
    msg: str,
    choices: Sequence[Any],
    default: str | None = None,
    descriptions: Sequence[str] | None = None,
    lower: bool = True,
) -> str:
    """
    Interactively query a choice from the prompt until the input matches one of the *choices*. The
    prompt can be configured using *msg* and *descriptions*, which, if set, must have the same
    length as *choices*. When *default* is not *None* it must be one of the choices and is used when
    the input is empty. When *lower* is *True*, the input is compared to the choices in lower case.
    """
    choices: list[str] = [str(c) for c in choices]  # type: ignore[assignment]
    _choices = [c.lower() for c in choices] if lower else choices

    if default is not None:
        if default not in choices:
            raise Exception("default must be one of the choices")

    hints = [(choice if choice != default else choice + "*") for choice in choices]
    if descriptions is not None:
        if len(descriptions) != len(choices):
            raise ValueError("length of descriptions must match length of choices")
        hints = [f"{h}({d})" for h, d in zip(hints, descriptions)]
    msg += f" [{', '.join(hints)}] "

    not_set = "__law_str_not_str__"
    choice = not_set
    while choice not in _choices:
        if choice != not_set:
            print(f"invalid choice: '{choice}'")
        choice = input(msg)
        if default is not None and choice == "":
            choice = default
        if lower:
            choice = choice.lower()

    return choice


def is_pattern(s: str) -> bool:
    """
    Returns *True* if the string *s* represents a pattern, i.e., if it contains characters such as
    ``"*"`` or ``"?"``.
    """
    return "*" in s or "?" in s


def brace_expand(s: str, split_csv: bool = False, escape_csv_sep: bool = True) -> list[str]:
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
        # split by real csv separators except escaped ones when requested
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
        raise ValueError(
            f"the number of sequences ({','.join(sequences)}) and the number of fixed entities "
            f"({','.join(entities)}) are not compatible",
        )

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


def range_expand(
    s: str | Sequence[str] | Sequence[tuple[int] | tuple[int, int]],
    include_end: bool = False,
    min_value: int | None = None,
    max_value: int | None = None,
    sep: str = ":",
) -> list[int]:
    """
    Takes a string, or a sequence of strings in the format ``"1:3"``, or a sequence of tuples
    containing start and stop values of a range and returns a list of all intermediate values. When
    *include_end* is *True*, the end value is included.

    One sided range expressions such as ``":4"`` or ``"4:"`` for strings and ``(None, 4)`` or
    ``(4, None)`` for tuples are also expanded but they require *min_value* and *max_value* to be
    set (an exception is raised otherwise), with *max_value* being either included or not, depending
    on *include_end*.

    Also, when a *min_value* (*max_value*) is set, the minimum (maximum) of expanded range is
    limited at this value.

    Example:

    .. code-block:: python

        range_expand("5:8")
        # -> [5, 6, 7]

        range_expand((6, 9))
        # -> [6, 7, 8]

        range_expand("5:8", include_end=True)
        # -> [5, 6, 7, 8]

        range_expand(["5-8", "10"])
        # -> [5, 6, 7, 10]

        range_expand(["5-8", "10-"])
        # -> Exception, no max_value set

        range_expand(["5-8", "10-"], max_value=12)
        # -> [5, 6, 7, 10, 11]

        range_expand(["5-8", "10-"], max_value=12, include_end=True)
        # -> [5, 6, 7, 8, 10, 11, 12]
    """
    def to_int(v: Any, s: Any | None = None) -> int:
        try:
            return int(v)
        except ValueError:
            raise ValueError(f"invalid number or range '{v if s is None else s}'")

    # make_list is used below, but we need to distinguish between lists and tuples
    numbers = []
    for v in ([s] if isinstance(s, tuple) else make_list(s)):
        start, stop, value = None, None, None
        single_value = False

        if isinstance(v, (tuple, list)):
            # parse tuple
            if len(v) == 1:
                value = v[0]
                single_value = True
            elif len(v) == 2:
                start, stop = v
            else:
                raise ValueError(f"invalid range tuple length: {v}")

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
                    raise Exception(
                        f"range '{s}' with missing start value requires min_value to be set",
                    )
                start = min_value
            if stop is None:
                if max_value is None:
                    raise Exception(
                        f"range '{s}' with missing stop value requires max_value to be set",
                    )
                stop = max_value

            # convert to integers and potentially swap
            start = to_int(start)
            stop = to_int(stop)
            if start > stop:
                start, stop = stop, start

            # add numbers
            numbers.extend(range(start, stop + int(bool(include_end))))

    # remove duplicates preserving the order
    unique_numbers = list(make_unique(numbers))
    del numbers

    # apply limits
    if min_value is not None:
        unique_numbers = [num for num in unique_numbers if num >= min_value]
    if max_value is not None:
        py_max_value = (max_value + 1) if include_end else max_value
        unique_numbers = [num for num in unique_numbers if num < py_max_value]

    return unique_numbers


def range_join(
    numbers: Sequence[int | str],
    to_str: bool = False,
    include_end: bool = False,
    sep: str = ",",
    range_sep: str = ":",
) -> list[tuple[int] | tuple[int, int]] | str:
    """
    Takes a sequence of positive integer numbers given either as integer or string types, and
    returns a sequence 1- and 2-tuples, denoting either single numbers or start and end values of
    possible ranges. Unless *include_end* is *True*, end values are not included. When *to_str* is
    *True*, a string is returned in a format consistent to :py:func:`range_expand` with ranges
    constructed by *range_sep* and merged with *sep*. Example:

    .. code-block:: python

        range_join([1, 2, 3, 5])
        # -> [(1, 4), (5,)]

        range_join([1, 2, 3, 5], include_end=True)
        # -> [(1, 3), (5,)]

        range_join([1, 2, 3, 5, 7, 8, 9])
        # -> [(1, 4), (5,), (7, 10)]

        range_join([1, 2, 3, 5, 7, 8, 9], to_str=True)
        # -> "1:4,5,7:10"
    """
    if not numbers:
        return "" if to_str else []

    # check type, convert, make unique and sort
    _numbers: list[int] = []
    for n in numbers:
        if isinstance(n, str):
            try:
                n = int(n)
            except ValueError:
                raise ValueError(f"invalid number format '{n}'")
        if not isinstance(n, int):
            raise TypeError(f"cannot handle non-integer value '{n}' in numbers to join")
        _numbers.append(n)
    del numbers
    _numbers = sorted(set(_numbers))

    # iterate through numbers, keep track of last starts and stops and fill a list of range tuples
    ranges: list[tuple[int] | tuple[int, int]] = []
    start = stop = _numbers[0]
    for n in _numbers[1:]:
        if n == stop + 1:
            stop += 1
        else:
            ranges.append((start,) if start == stop else (start, stop + int(bool(not include_end))))
            start = stop = n
    # add the last one
    ranges.append((start,) if start == stop else (start, stop + int(bool(not include_end))))

    # return if not converting to string
    if not to_str:
        return ranges

    # convert to string representation
    return sep.join(
        str(r[0]) if len(r) == 1 else "{1}{0}{2}".format(range_sep, *r)
        for r in ranges
    )


def multi_match(
    name: str,
    patterns: str | Iterable[str],
    mode: Callable[[Iterable], bool] = any,
    regex: bool = False,
) -> bool:
    """
    Compares *name* to multiple *patterns* and returns *True* in case of at least one match (*mode*
    = *any*, the default), or in case all patterns match (*mode* = *all*). Otherwise, *False* is
    returned. When *regex* is *True*, *re.match* is used instead of *fnmatch.fnmatch*.
    """
    patterns = make_list(patterns)
    if regex:
        return mode(re.match(pattern, name) for pattern in patterns)
    return mode(fnmatch.fnmatch(name, pattern) for pattern in patterns)


def is_iterable(obj: Any) -> bool:
    """
    Returns *True* when an object *obj* is iterable and *False* otherwise.
    """
    try:
        iter(obj)
        return True
    except Exception:
        return False


lazy_iter_types = (
    GeneratorType,
    MappingView,
    range,
    map,
    enumerate,
)


def is_lazy_iterable(obj: Any) -> bool:
    """
    Returns whether *obj* is iterable lazily, such as generators, range objects, maps, etc.
    """
    return isinstance(obj, lazy_iter_types)


def make_list(obj: Any, cast: bool = True) -> list[Any]:
    """
    Converts an object *obj* to a list and returns it. Objects of types *tuple* and *set* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new list.
    """
    if isinstance(obj, list):
        return list(obj)
    if is_lazy_iterable(obj):
        return list(obj)
    if isinstance(obj, (tuple, set)) and cast:
        return list(obj)
    return [obj]


def make_tuple(obj: Any, cast: bool = True) -> tuple[Any]:
    """
    Converts an object *obj* to a tuple and returns it. Objects of types *list* and *set* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new tuple.
    """
    if isinstance(obj, tuple):
        return obj
    if is_lazy_iterable(obj):
        return tuple(obj)
    if isinstance(obj, (list, set)) and cast:
        return tuple(obj)
    return (obj,)


def make_set(obj: Any, cast: bool = True) -> set[Any]:
    """
    Converts an object *obj* to a set and returns it. Objects of types *list* and *tuple* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new set.
    """
    if isinstance(obj, set):
        return obj
    if is_lazy_iterable(obj):
        return set(obj)
    if isinstance(obj, (list, tuple)) and cast:
        return set(obj)
    return {obj}


def make_unique(obj: Iterable[T]) -> Iterable[T]:
    """
    Takes a list or tuple *obj*, removes duplicate elements in order of their appearance and returns
    the sequence of remaining, unique elements. The sequence type is preserved. When *obj* is
    neither a list nor a tuple, but iterable, a list is returned. Otherwise, a *TypeError* is
    raised.
    """
    if not isinstance(obj, (list, tuple)):
        if not is_iterable(obj) and not is_lazy_iterable(obj):
            raise TypeError("object is neither list, tuple, nor generic iterable")
        obj = list(obj)

    ret = sorted(obj.__class__(set(obj)), key=lambda elem: obj.index(elem))

    return obj.__class__(ret) if isinstance(obj, tuple) else ret


def is_nested(obj: Any) -> bool:
    """
    Takes a list or tuple *obj* and checks whether it only contains items of types list and tuple.
    """
    return isinstance(obj, (list, tuple)) and all(isinstance(item, (list, tuple)) for item in obj)


def flatten(
    *structs: Any,
    flatten_dict: bool = True,
    flatten_list: bool = True,
    flatten_tuple: bool = True,
    flatten_set: bool = True,
) -> list[Any]:
    """ flatten(*structs, flatten_dict=True, flatten_list=True, flatten_tuple=True, flatten_set=True)
    Takes one or multiple complex structured objects *structs*, flattens them, and returns a single
    list. *flatten_dict*, *flatten_list*, *flatten_tuple* and *flatten_set* configure if objects of
    the respective types are flattened (the default). If not, they are returned unchanged.
    """
    if len(structs) == 0:
        return []

    kwargs = dict(
        flatten_dict=flatten_dict,
        flatten_list=flatten_list,
        flatten_tuple=flatten_tuple,
        flatten_set=flatten_set,
    )
    if len(structs) > 1:
        return flatten(structs, **kwargs)

    struct = structs[0]

    flatten_seq = lambda seq: sum((flatten(obj, **kwargs) for obj in seq), [])
    if isinstance(struct, dict):
        if flatten_dict:
            return flatten_seq(struct.values())
    elif isinstance(struct, list):
        if flatten_list:
            return flatten_seq(struct)
    elif isinstance(struct, tuple):
        if flatten_tuple:
            return flatten_seq(struct)
    elif isinstance(struct, set):
        if flatten_set:
            return flatten_seq(struct)
    elif is_lazy_iterable(struct):
        return flatten_seq(struct)

    return [struct]


def merge_dicts(*dicts, **kwargs):
    """ merge_dicts(*dicts, inplace=False, cls=None, deep=False)
    Takes multiple *dicts* and returns a single merged dict. The merging takes place in order of the
    passed dicts and therefore, values of rear objects have precedence in case of field collisions.

    By default, a new dictionary is returned. However, when *inplace* is *True*, all update
    operations are performed inplace on the first object in *dicts*.

    When not inplace, the class of the returned merged dict is configurable via *cls*. If it is
    *None*, the class is inferred from the first dict object in *dicts*.

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
    if not dicts:
        raise ValueError("cannot merge empty sequence of dictionaries")

    inplace = kwargs.get("inplace", False)
    if inplace:
        merged_dict = dicts[0]
    else:
        # get or infer the class
        cls = kwargs.get("cls", None)
        if cls is None:
            for d in dicts:
                if isinstance(d, dict):
                    cls = d.__class__
                    break
            else:
                raise TypeError("cannot infer cls as none of the passed objects is of type dict")
        # create a new instance
        merged_dict = cls()

    # start merging
    deep = kwargs.get("deep", False)
    for d in dicts[(1 if inplace else 0):]:
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
                    merge_dicts(merged_dict[k], v, inplace=True, deep=deep)
        else:
            merged_dict.update(d)

    return merged_dict


def unzip(struct: Iterable, fill_none: bool = False) -> tuple[list[Any], ...] | None:
    """
    Unzips a *struct* consisting of sequences with equal lengths and returns lists with 1st, 2nd,
    etc elements. This function can be thought of as the opposite of the ``zip`` builtin.

    The number of elements per returned list is determined by the length of the first sequence in
    *struct*. In case a sequence does contain fewer items an exception is raised. However, if
    *fill_none* is *True*, *None* is inserted instead.

    .. code-block:: python

        unzip([(1, 2), (3, 4)])
        # -> ([1, 3], [2, 4])

        unzip([(1, 2), (3,)])
        # -> ValueError

        unzip([(1, 2), (3,)], fill_none=True)
        # -> ([1, 3], [2, None])
    """
    lists: tuple | None = None
    for i, obj in enumerate(struct):
        # determine the number of lists to return
        if lists is None:
            lists = tuple([] for _ in range(len(obj)))

        # fill them
        for j, l in enumerate(lists):
            if len(obj) > j:
                l.append(obj[j])
            elif fill_none:
                l.append(None)
            else:
                raise ValueError(
                    f"insufficient length {j} of sequence at index {len(lists)} to unzip",
                )

    return lists


def which(prog: str) -> str | None:
    """
    Pythonic ``which`` implementation. Returns the path to an executable *prog* by searching in
    *PATH*, or *None* when it could not be found.
    """
    executable = lambda path: os.path.isfile(path) and os.access(path, os.X_OK)

    # prog can also be a path
    dirname, _ = os.path.split(str(prog))
    if dirname:
        if executable(str(prog)):
            return prog

    elif "PATH" in os.environ:
        for search_path in os.environ["PATH"].split(os.pathsep):
            path = os.path.join(search_path.strip('"'), prog)
            if executable(path):
                return path

    return None


def map_verbose(
    func: Callable[[T], Any],
    seq: Iterable[T],
    msg: str = "{}",
    every: int = 25,
    start: bool = True,
    end: bool = True,
    offset: int = 0,
    callback: Callable[[int], Any] | None = None,
) -> list[T]:
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


def map_struct(
    func: Callable[[Any], Any],
    struct: Any,
    map_dict: int | bool = True,
    map_list: int | bool = True,
    map_tuple: int | bool = False,
    map_set: int | bool = False,
    cls=None,
    custom_mappings: dict[type | tuple[type, ...], Callable[..., Any]] | None = None,
) -> Any:
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
    valid_types: tuple[type, ...] = tuple()
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
    if custom_mappings and isinstance(struct, tuple(flatten(custom_mappings.keys()))):
        # get the mapping function
        for mapping_types, mapping_func in custom_mappings.items():
            if isinstance(struct, mapping_types):
                return mapping_func(
                    func,
                    struct,
                    map_dict=map_dict,
                    map_list=map_list,
                    map_tuple=map_tuple,
                    map_set=map_set,
                    cls=cls,
                    custom_mappings=custom_mappings,
                )
        raise RuntimeError(f"no custom mapping function found for struct '{struct}'")

    # traverse?
    if isinstance(struct, valid_types):
        # create a new struct, treat tuples as lists for itertative item appending
        new_struct = struct.__class__() if not isinstance(struct, tuple) else []

        # create type-dependent generator and addition callback
        if isinstance(struct, (list, tuple)):
            gen = enumerate(struct)
            add = lambda _, value: new_struct.append(value)  # type: ignore
        elif isinstance(struct, set):
            gen = enumerate(struct)
            add = lambda _, value: new_struct.add(value)  # type: ignore
        elif isinstance(struct, dict):
            gen = struct.items()  # type: ignore
            add = lambda key, value: new_struct.__setitem__(key, value)  # type: ignore
        else:
            raise TypeError(f"invalid struct type '{type(struct)}'")

        # recursively fill the new struct
        for key, value in gen:
            value = map_struct(
                func,
                value,
                map_dict=map_dict,
                map_list=map_list,
                map_tuple=map_tuple,
                map_set=map_set,
                cls=cls,
                custom_mappings=custom_mappings,
            )
            add(key, value)

        # convert tuples
        if isinstance(struct, tuple):
            new_struct = struct.__class__(new_struct)  # type: ignore

        return new_struct

    # apply the mapping function on everything else
    return func(struct)


def mask_struct(
    mask: bool | Iterable[bool],
    struct: T | Iterable[T],
    replace: Any | NoValue = no_value,
    convert_types: dict[type | tuple[type, ...], Callable[[Any], Any]] | None = None,
):
    """
    Masks a complex structured object *struct* with a *mask* and returns the remaining values. When
    *replace* is set, masked values are replaced with that value instead of being removed. The
    *mask* can have a complex structure as well.

    *convert_types* can be a dictionary containing conversion functions mapped to types (or tuples)
    thereof that is applied to objects during the struct traversal if their types match.

    Examples:

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
        struct = list(struct)  # type: ignore

    # cast convert types
    if convert_types and isinstance(struct, tuple(flatten(convert_types.keys()))):
        # get the mapping function
        for _types, convert in convert_types.items():
            if isinstance(struct, _types):
                struct = convert(struct)
                break

    # when mask is a bool, or struct is not a dict or sequence, apply the mask immediately
    if isinstance(mask, bool) or not isinstance(struct, (list, tuple, dict)):
        return struct if mask else replace

    # check list and tuple types
    if isinstance(struct, (list, tuple)) and isinstance(mask, (list, tuple)):
        new_struct = []
        for i, val in enumerate(struct):
            if i >= len(mask):
                new_struct.append(val)
            else:
                repl = replace
                if isinstance(replace, (list, tuple)) and len(replace) > i:
                    repl = replace[i]
                val = mask_struct(mask[i], val, replace=repl, convert_types=convert_types)
                if val != no_value:
                    new_struct.append(val)

        return struct.__class__(new_struct) if new_struct else replace

    # check dict types
    if isinstance(struct, dict) and isinstance(mask, dict):
        new_struct: dict = struct.__class__()
        for key, val in struct.items():
            if key not in mask:
                new_struct[key] = val
            else:
                repl = replace
                if isinstance(replace, dict) and key in replace:
                    repl = replace[key]
                val = mask_struct(mask[key], val, replace=repl, convert_types=convert_types)
                if val != no_value:
                    new_struct[key] = val
        return new_struct or replace

    # when this point is reached, mask and struct have incompatible types
    raise TypeError(
        f"mask and struct must have the same type, got '{type(mask)}' and '{type(struct)}'",
    )


@contextlib.contextmanager
def tmp_file(*args, **kwargs) -> Iterator[tuple[int, str]]:
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


def interruptable_popen(
    *args,
    stdin_callback: Callable[[], Any] | None = None,
    stdin_delay: int | float = 0,
    interrupt_callback: Callable[[subprocess.Popen], Any] | None = None,
    kill_timeout: int | float | None = None,
    **kwargs,
) -> tuple[int, str | None, str | None]:
    """
    Shorthand to :py:class:`Popen` followed by :py:meth:`Popen.communicate` which can be interrupted
    by *KeyboardInterrupt*. The return code, standard output and standard error are returned in a
    3-tuple.

    *stdin_callback* can be a function accepting no arguments and whose return value is passed to
    ``communicate`` after a delay of *stdin_delay* to feed data input to the subprocess.

    *interrupt_callback* can be a function, accepting the process instance as an argument, that is
    called immediately after a *KeyboardInterrupt* occurs. After that, a SIGTERM signal is send to
    the subprocess to allow it to gracefully shutdown.

    When *kill_timeout* is set, and the process is still alive after that period (in seconds), a
    SIGKILL signal is sent to force the process termination.

    All other *args* and *kwargs* are forwarded to the :py:class:`Popen` constructor.
    """
    # start the subprocess in a new process group
    kwargs["preexec_fn"] = os.setsid
    p = subprocess.Popen(*args, **kwargs)

    # get stdin
    stdin_data = None
    if callable(stdin_callback):
        if stdin_delay > 0:
            time.sleep(stdin_delay)
        stdin_data = stdin_callback()
        if isinstance(stdin_data, str):
            stdin_data = (stdin_data + "\n").encode("utf-8")

    # handle interrupts
    try:
        out, err = p.communicate(stdin_data)
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
            target_time = time.perf_counter() + kill_timeout
            while target_time > time.perf_counter():
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

    # decode
    if out is not None:
        out = out.decode("utf-8")
    if err is not None:
        err = err.decode("utf-8")

    return p.returncode, out, err


def readable_popen(*args, **kwargs) -> tuple[subprocess.Popen, Iterable[str]]:
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
        for line in p.stdout:
            yield line.decode("utf-8").rstrip()

        # communicate in the end
        p.communicate()

    return p, line_gen()


def create_hash(inp: Any, l: int = 10, algo: str = "sha256", to_int: bool = False) -> str | int:
    """
    Takes an arbitrary input *inp* and creates a hexadecimal string hash based on an algorithm
    *algo*. For valid algorithms, see python's hashlib. *l* corresponds to the maximum length of the
    returned hash and is limited by the length of the hexadecimal representation produced by the
    hashing algorithm. When *to_int* is *True*, the decimal integer representation is returned.
    """
    h = getattr(hashlib, algo)(str(inp).encode("utf-8")).hexdigest()[:l]
    return int(h, 16) if to_int else h


def create_random_string(prefix: str = "", l: int = 10) -> str:
    """
    Creates and returns a random string consisting of *l* characters using a uuid4 hash. When
    *prefix* is given, the string will have the format ``<prefix>_<random_string>``.
    """
    s = ""
    while len(s) < l:
        s += uuid.uuid4().hex
    s = s[:l]
    if prefix:
        s = f"{prefix}_{s}"
    return s


def copy_no_perm(src: str | pathlib.Path, dst: str | pathlib.Path) -> None:
    """
    Copies a file from *src* to *dst* including meta data except for permission bits.
    """
    src, dst = str(src), str(dst)
    shutil.copyfile(src, dst)
    perm = os.stat(dst).st_mode
    shutil.copystat(src, dst)
    os.chmod(dst, perm)


def makedirs(path: str | pathlib.Path, perm: int | None = None) -> None:
    """
    Recursively creates directories up to *path*. No exception is raised if *path* refers to an
    existing directory. If *perm* is set, the permissions of all newly created directories are set
    to this value.
    """
    # nothing to do when the directory already exists
    path = str(path)
    if os.path.isdir(path):
        return

    # helper to silently create the directory, catching exceptions if it exists by now
    # (when dropping py2, just use the exist_ok flag of os.makedirs)
    def makedirs_safe(path: str, perm: int | None = None) -> None:
        try:
            if perm is None:
                os.makedirs(path)
            else:
                os.makedirs(path, perm)
        except Exception as e:
            if not isinstance(e, FileExistsError):
                raise

    if perm is None:
        makedirs_safe(path)
    else:
        umask = os.umask(0)
        try:
            makedirs_safe(path, perm)
        finally:
            os.umask(umask)


def user_owns_file(path: str | pathlib.Path, uid: int | None = None) -> bool:
    """
    Returns whether a file located at *path* is owned by the user with *uid*. When *uid* is *None*,
    the user id of the current process is used.
    """
    if uid is None:
        uid = os.getuid()
    path = os.path.expandvars(os.path.expanduser(str(path)))
    return os.stat(path).st_uid == uid


def iter_chunks(l: int | Iterable[Any], size: int) -> Iterator[list[int | Any]]:
    """
    Returns a generator containing chunks of *size* of a list, integer or generator *l*. A *size*
    smaller than 1 results in no chunking at all.
    """
    _l: Iterable = range(l) if isinstance(l, int) else l

    if is_lazy_iterable(_l):
        # non-positive size means no chunking
        if size < 1:
            yield list(_l)
            return

        # traverse and divide into chunks
        chunk: list = []
        for elem in _l:
            if len(chunk) < size:
                chunk.append(elem)
            else:
                yield chunk
                chunk = [elem]
        if chunk:
            yield chunk
        return

    _l = list(_l)
    if size < 1:
        yield _l
        return

    for i in range(0, len(_l), size):
        yield _l[i:i + size]


byte_units = ["bytes", "kB", "MB", "GB", "TB", "PB", "EB"]
byte_units_lower = [u.lower() for u in byte_units]


def human_bytes(
    n: int | float,
    unit: str | None = None,
    fmt: Callable[[str, str], str] | Any = None,
) -> tuple[float, str] | str:
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
        raise ValueError(f"unknown unit '{unit}', valid values are {byte_units}")

    if n == 0:
        idx = 0
    elif unit:
        idx = byte_units.index(unit)
    else:
        idx = int(math.floor(math.log(abs(n), 1024)))
        idx = min(idx, len(byte_units))

    # get the value and the unit name
    value = n / 1024.0 ** idx
    unit = byte_units[idx]

    # vast value to int when the unit is bytes
    if idx == 0:
        value = int(round(value))

    if fmt:
        if not isinstance(fmt, str):
            fmt = "{} {}" if idx == 0 else "{:.1f} {}"
        return fmt.format(value, unit)

    return value, unit


def parse_bytes(s: str | int | float, input_unit: str = "bytes", unit: str = "bytes") -> float:
    """
    Takes a string *s*, interprets it as a size with an optional unit, and returns a float that
    represents that size in a given *unit*. When no unit is found in *s*, *input_unit* is used as a
    default. A *ValueError* is raised, when *s* cannot be successfully converted. Example:

    .. code-block:: python

        parse_bytes("100")
        # -> 100.0

        parse_bytes("2048", unit="kB")
        # -> 2.0

        parse_bytes("2048 kB", unit="kB")
        # -> 2048.0

        parse_bytes("2048 kB", unit="MB")
        # -> 2.0

        parse_bytes("2048", "kB", unit="MB")
        # -> 2.0

        parse_bytes(2048, "kB", unit="MB")  # note the float type of the first argument
        # -> 2.0
    """
    # check if the units exists
    if input_unit.lower() not in byte_units_lower:
        raise ValueError(f"unknown input_unit '{input_unit}', valid values are {byte_units}")
    if unit.lower() not in byte_units_lower:
        raise ValueError(f"unknown unit '{unit}', valid values are {byte_units}")

    # when s is a number, interpret it as bytes right away
    # otherwise parse it
    if isinstance(s, int):
        input_value = float(s)
    elif not isinstance(s, float):
        m = re.match(r"^\s*(-?\d+\.?\d*)\s*(|{})\s*$".format("|".join(byte_units_lower)), s.lower())
        if not m:
            raise ValueError("cannot parse bytes from string '{}'".format(s))

        input_value = float(m.group(1))
        _input_unit = m.group(2)
        if _input_unit:
            input_unit = _input_unit

    # convert the input value to bytes
    idx = byte_units_lower.index(input_unit.lower())
    size_bytes = input_value * 1024.0 ** idx

    # use human_bytes to convert the size
    return human_bytes(size_bytes, unit=unit)[0]  # type: ignore


time_units: dict[str, int] = dict([
    ("week", 7 * 24 * 60 * 60),
    ("day", 24 * 60 * 60),
    ("hour", 60 * 60),
    ("minute", 60),
    ("second", 1),
])

time_unit_aliases: dict[str, str] = {
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


def human_duration(colon_format: bool | str = False, plural: bool = True, **kwargs) -> str:
    """
    Returns a human readable duration. The largest unit is days. When *colon_format* is *True*, the
    return value has the format ``"[d-][hh:]mm:ss[.ms]"``. *colon_format* can also be a string value
    referring to a limiting  unit. In that case, the returned time string has no field above that
    unit, e.g. passing ``"m"`` results in a string ``"mm:ss[.ms]"`` where the minute field is
    potentially larger than 60. Passing ``"s"`` is a special case. Since the colon format always has
    a minute field (to mark it as colon format in the first place), the returned string will have
    the format ``"00:ss[.ms]"``. Unless *plural* is *False*, units corresponding to values other
    than **exactly** one are used in plural e.g. ``"1 second"`` but ``"1.5 seconds"``.

    All other *kwargs* are passed to ``datetime.timedelta`` to get the total duration in seconds.

    Example:

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
    if isinstance(colon_format, str):
        colon_unit_limit = time_unit_aliases.get(colon_format, colon_format)
        if colon_unit_limit not in _time_units:
            raise ValueError(
                f"unknown colon_format unit '{colon_unit_limit}', valid values are "
                f"{','.join(_time_units)}",
            )
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
                keep_zero = bool(human_str) or unit == "second" or bool(colon_unit_limit)
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
            human_str += f"{value} {unit}{'' if (value == 1 or not plural) else 's'}"

    # sign
    if sign == -1:
        human_str = ("-" if colon_format else "minus ") + human_str

    return human_str


def parse_duration(s: int | float | str, input_unit: str = "s", unit: str = "s") -> float:
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
        raise ValueError(
            f"unknown input_unit '{input_unit}', valid values are {','.join(time_units)}",
        )
    if unit not in time_units:
        raise ValueError(f"unknown unit '{unit}', valid values are {','.join(time_units)}")

    sign = 1
    duration_seconds = 0.0

    # number or string?
    if isinstance(s, (int, float)) or is_float(s):
        duration_seconds += float(s) * time_units[input_unit]
    else:
        s = s.strip()

        # identify the format "[d-][h:][m:]s[.ms]" first
        _m = re.match(r"^([+-])?((((((\d+)-)?(\d+)):)?(\d+)):)?(\d+)(\.(\d*))?$", s)
        if _m:
            sgn, d, h, m, s, ms = [_m.group(i) for i in [1, 7, 8, 9, 10, 11]]

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
                    raise ValueError(f"cannot parse duration string '{s}'")

                d, u = m.groups()
                d = float(d)
                if not u:
                    u = input_unit
                u = time_unit_aliases.get(u, u)

                duration_seconds += d * time_units[u]

    # convert to output unit
    duration = sign * duration_seconds / time_units[unit]

    return duration


def send_mail(
    recipient: str,
    sender: str,
    subject: str = "",
    content: str = "",
    smtp_host: str = "127.0.0.1",
    smtp_port: int = 25,
) -> bool:
    """
    Lightweight mail functionality. Sends an mail from *sender* to *recipient* with *subject* and
    *content*. *smtp_host* and *smtp_port* are forwarded to the ``smtplib.SMTP`` constructor. *True*
    is returned on success, *False* otherwise.
    """
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
    except Exception as e:
        logger.warning(f"cannot create SMTP server: {e}")
        return False

    header = f"From: {sender}\r\nTo: {recipient}\r\nSubject: {subject}\r\n\r\n"
    server.sendmail(sender, recipient, header + content)

    return True


class DotDict(dict):
    """
    Dictionary subclass that provides read access for items via attributes by implementing
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

    def __class_getitem__(cls, types: tuple[type, type]) -> GenericAlias:
        # python <3.9
        if GenericAlias is str:
            key_type, value_type = types
            return f"{cls.__name__}[{key_type.__name__}, {value_type.__name__}]"  # type: ignore[return-value] # noqa

        return GenericAlias(cls, types)  # type: ignore[call-overload, return-value]

    @classmethod
    def wrap(cls, *args, **kwargs) -> DotDict:
        """
        Takes a dictionary *d* and recursively replaces it and all other nested dictionary types
        with :py:class:`DotDict`'s for deep attribute-style access.
        """
        wrap = lambda d: cls((k, wrap(v)) for k, v in d.items()) if isinstance(d, dict) else d  # type: ignore # noqa
        return wrap(dict(*args, **kwargs))

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def copy(self) -> DotDict:
        """"""
        return copy.deepcopy(self)


class ShorthandDict(dict):
    """
    Dictionary subclass that implements ``__getattr__`` and ``__setattr__`` for a configurable list
    of attributes. Example:

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

    attributes: dict[str, Any] = {}

    def __init__(self, **kwargs) -> None:
        super().__init__()

        for attr, default in self.attributes.items():
            self[attr] = kwargs.pop(attr, copy.deepcopy(default))

        self.update(kwargs)

    def copy(self) -> ShorthandDict:
        """"""
        kwargs = {key: copy.deepcopy(value) for key, value in self.items()}
        return self.__class__(**kwargs)

    def __getattr__(self, attr: str) -> Any:
        if attr in self.attributes:
            return self[attr]
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'")

    def __setattr__(self, attr: str, value: Any) -> None:
        if attr in self.attributes:
            self[attr] = value
        else:
            super().__setattr__(attr, value)


class InsertableDict(dict):
    """
    Dictionary subclass that supports inserting elements before or after certain keys.
    Example:

    .. code-block:: python

        d = InsertableDict(foo=123, bar=456)

        d.insert_before("bar", "test", 999)
        print(d)  # -> InsertableDict([('foo', 123), ('test', 999), ('bar', 456)])

        d.insert_after("test", "foo", "new_value")
        print(d)  # -> InsertableDict([('test', 999), ('foo', 'new_value'), ('bar', 456)])
    """

    def _insert(self, search_key: Hashable, key: Hashable, value: Any, offset: int) -> None:
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

    def insert_before(self, before_key: Hashable, key: Hashable, value: Any = None) -> None:
        """
        Inserts a *key* - *value* pair before the key *before_key*. When this key does not exist,
        the new pair is added to the end. When *key* is list or dictionary and value is *None*,
        multiple new values are inserted.
        """
        self._insert(before_key, key, value, 0)

    def insert_after(self, after_key: Hashable, key: Hashable, value: Any = None) -> None:
        """
        Inserts a *key* - *value* pair after the key *after_key*. When this key does not exist, the
        new pair is added to the end. When *key* is list or dictionary and value is *None*,
        multiple new values are inserted.
        """
        self._insert(after_key, key, value, 1)


@contextlib.contextmanager
def patch_object(
    obj: T,
    attr: str,
    value: Any,
    reset: bool = True,
    orig: Any | NoValue = no_value,
    lock: bool | AbstractContextManager = False,
) -> Generator[T, None, None]:
    """
    Context manager that temporarily patches an object *obj* by replacing its attribute *attr* with
    *value*. The original value is set again when the context is closed unless *reset* is *False*.
    The original value is obtained through ``getattr`` or taken from *orig* if set. When *lock* is
    *True*, the py:attr:`default_lock` object is used to ensure the patch is thread-safe.
    When *lock* is a lock instance, this object is used instead.
    """
    if orig is no_value:
        # get the original value
        orig = getattr(obj, attr, no_value)

    # handle thread locks
    if lock:
        if isinstance(lock, bool):
            lock = default_lock
    else:
        lock = empty_context()

    with lock:
        try:
            setattr(obj, attr, value)

            yield obj
        finally:
            try:
                if reset:
                    if orig is no_value:
                        delattr(obj, attr)
                    else:
                        setattr(obj, attr, orig)
            except:
                pass


def join_generators(
    *generators: GeneratorType,
    on_error: Callable[[Exception | KeyboardInterrupt], None] | None = None,
) -> Generator[Any, None, None]:
    """
    Joins multiple *generators* and returns a single generator for simplified iteration. Yielded
    objects are transparently sent back to ``yield`` assignments of the same generator. When
    *on_error* is callable, it is invoked in case an exception is raised while iterating, including
    *KeyboardInterrupt*'s. If its return value evaluates to *True*, the state is reset and
    iterations continue. Otherwise, the exception is raised.
    """
    for gen in generators:
        last_result: Any = no_value
        while True:
            try:
                if last_result == no_value:
                    last_result = yield next(gen)
                else:
                    last_result = yield gen.send(last_result)
            except StopIteration:
                break
            except (Exception, KeyboardInterrupt) as error:
                if not callable(on_error) or not on_error(error):
                    raise
                last_result = no_value


def quote_cmd(cmd: str | Sequence[str]) -> str:
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
    parts = (
        (quote_cmd(part) if isinstance(part, (list, tuple)) else str(part))
        for part in cmd
    )

    # quote all parts and join
    return " ".join(shlex.quote(part) for part in parts)


def escape_markdown(s: str) -> str:
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

    def __init__(self, fget: Callable, fset: Callable | None = None) -> None:
        super().__init__()

        self.fget = fget
        self.fset = fset

    def __get__(self, obj: object, cls: type | None = None) -> Any:
        if cls is None:
            cls = type(obj)

        return self.fget.__get__(obj, cls)()

    def __set__(self, obj: object, value: Any) -> Any:
        if not self.fset:
            raise AttributeError("can't set attribute")

        type_ = type(obj)

        return self.fset.__get__(obj, type_)(value)


def classproperty(func: Callable) -> ClassPropertyDescriptor:
    """
    Propety decorator for class-level methods.
    """
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)  # type: ignore

    return ClassPropertyDescriptor(func)


class BaseStream(object):

    FLUSH_AFTER_WRITE: bool = True

    def __init__(self, flush_after_write: bool | None = None) -> None:
        super().__init__()

        self.closed = False
        self.flush_after_write = flush_after_write

    @property
    def _flush_after_write(self) -> bool:
        return self.FLUSH_AFTER_WRITE if self.flush_after_write is None else self.flush_after_write

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> BaseStream:
        return self

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> None:
        self.close()

    def close(self) -> None:
        if self.closed:
            return
        self.flush()
        self._close()
        self.closed = True

    def flush(self) -> None:
        if self.closed:
            return
        self._flush()

    def write(self, *args, **kwargs) -> None:
        if self.closed:
            return

        self._write(*args, **kwargs)
        if self._flush_after_write:
            self.flush()

    def _close(self) -> None:
        return

    def _flush(self) -> None:
        return

    def _write(self, *args, **kwargs) -> None:
        return


class TeeStream(BaseStream):
    """
    Multi-stream object that forwards calls to :py:meth:`write` and :py:meth:`flush` to all
    registered *consumer* streams. When a *consumer* is a string, it is interpreted as a file which
    is opened for writing (similar to *tee* in bash). All *kwargs* are forwarded to the
    :py:class:`BaseStream` constructor.

    Example:

    .. code-block:: python

        tee = TeeStream("/path/to/log.txt", sys.__stdout__)
        sys.stdout = tee
    """

    def __init__(self, *consumers, mode="w", **kwargs) -> None:
        super().__init__(**kwargs)

        self.consumers = []
        self.open_files = []

        for consumer in consumers:
            # interpret strings as file paths
            if isinstance(consumer, str):
                consumer = open(consumer, mode)
                self.open_files.append(consumer)
            self.consumers.append(consumer)

    def _close(self) -> None:
        """
        Closes opened files.
        """
        for f in self.open_files:
            f.close()

    def _flush(self) -> None:
        """
        Flushes all registered consumer streams.
        """
        for consumer in self.consumers:
            if not getattr(consumer, "closed", False):
                consumer.flush()

    def _write(self, *args, **kwargs) -> None:
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

    def __init__(self, stream: Any, filter_fn: Callable[..., bool], **kwargs) -> None:
        super().__init__(**kwargs)

        self.stream = stream
        self.filter_fn = filter_fn

    def _close(self) -> None:
        """
        Closes the consumer stream.
        """
        self.stream.close()

    def _flush(self) -> None:
        """
        Flushes the consumer stream.
        """
        if getattr(self.stream, "closed", False):
            return
        self.stream.flush()

    def _write(self, *args, **kwargs) -> None:
        """
        Writes to the consumer stream when *filter_fn* evaluates to *True*, passing *args* and
        *kwargs*.
        """
        if self.filter_fn(*args, **kwargs):
            self.stream.write(*args, **kwargs)
