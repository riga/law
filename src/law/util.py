"""
Helpful utility functions.
"""

from __future__ import annotations

__all__ = [  # noqa: RUF022
    # singleton values
    "default_lock",
    "io_lock",
    "console_lock",
    "mp_manager",
    "no_value",
    # path and task helpers
    "common_task_params",
    "increment_path",
    "law_home_path",
    "law_run",
    "law_src_path",
    "rel_path",
    # generic helpers
    "abort",
    "chunk_slice_ranges",
    "create_hash",
    "create_random_string",
    "custom_context",
    "empty_context",
    "escape_markdown",
    "get_terminal_width",
    "import_file",
    "iter_chunks",
    "join_generators",
    "patch_object",
    "quote_cmd",
    "send_mail",
    "which",
    # value identification and conversion
    "colored",
    "flag_to_bool",
    "human_bytes",
    "human_duration",
    "is_classmethod",
    "is_float",
    "is_number",
    "is_pattern",
    "parse_bytes",
    "parse_duration",
    "query_choice",
    "round_discrete",
    "str_to_int",
    "try_int",
    "uncolored",
    # sequence helpers
    "brace_expand",
    "flatten",
    "is_iterable",
    "is_lazy_iterable",
    "is_nested",
    "make_list",
    "make_set",
    "make_tuple",
    "make_unique",
    "map_struct",
    "map_verbose",
    "mask_struct",
    "merge_dicts",
    "multi_match",
    "range_expand",
    "range_join",
    "unzip",
    # parallelization helpers
    "get_subprocess_pids",
    "interruptable_popen",
    "kill_process",
    "readable_popen",
    "send_signal_silent",
    # io helpers
    "copy_no_perm",
    "makedirs",
    "tmp_file",
    "user_owns_file",
    # classes
    "DotDict",
    "ShorthandDict",
    "classproperty",
    "BaseStream",
    "TeeStream",
    "FilteredStream",
]

import collections
import contextlib
import copy
import datetime
import fnmatch
import functools
import hashlib
import inspect
import itertools
import logging
import math
import multiprocessing
import multiprocessing.managers
import os
import pathlib
import random
import re
import shlex
import shutil
import signal
import smtplib
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from law._types import (
    AbstractContextManager,
    Any,
    Callable,
    Generator,
    GeneratorType,
    GenericAlias,
    Hashable,
    Iterable,
    Iterator,
    MappingView,
    ModuleType,
    Sequence,
    Sized,
    T,
    TracebackType,
    Union,
)

ipykernel: ModuleType | None = None
try:
    import ipykernel
    import ipykernel.iostream
except ImportError:
    pass

try:
    import google.colab  # noqa: F401
    ON_COLAB = True
except ImportError:
    ON_COLAB = False


logger = logging.getLogger(__name__)

# some globally usable thread locks
default_lock = threading.Lock()
io_lock = threading.Lock()
console_lock = threading.Lock()


class NoValue:

    __hash: int = hash(object())
    _instance: NoValue | None = None

    def __new__(cls, *args, **kwargs) -> NoValue:
        if cls._instance is None:
            cls._instance = super().__new__(cls, *args, **kwargs)
        return cls._instance

    def __hash__(self) -> int:
        return self.__hash

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


def MPManager(**kwargs) -> multiprocessing.managers.SyncManager:
    """
    Factory function identical to :py:func:`multiprocessing.Manager` but allows for additional
    arguments to be forwarded to the underlying :py:class:`multiprocessing.managers.SyncManager`.

    :param kwargs: Keyword arguments forwarded to the
        :py:class:`multiprocessing.managers.SyncManager` constructor.
    :return: The started manager.
    """
    kwargs.setdefault("ctx", multiprocessing.context._default_context.get_context())
    manager = multiprocessing.managers.SyncManager(**kwargs)
    manager.start()
    return manager


_mp_managed_objects = {
    "list", "dict", "Namespace", "Lock", "RLock", "Semaphore", "BoundedSemaphore", "Condition", "Event", "Barrier",
    "Queue", "Value", "Array",
}


class DeferredManager:
    """
    Wrapper for a :py:class:`multiprocessing.managers.SyncManager` created by the
    :py:func:`MPManager` factory that is started lazily once a synchronization attribute is
    accessed. In addition, it provides a cache for these attributes.
    """

    def __init__(self, **kwargs) -> None:
        super().__init__()

        self.kwargs = kwargs
        self._manager: multiprocessing.managers.SyncManager | None = None
        self._objects: dict[str, Any] = {}

    def _start(self) -> None:
        if self._manager is None:
            self._manager = MPManager(**self.kwargs)

    def get(self, name: str, obj_type: str, *args, **kwargs) -> Any:
        if name not in self._objects:
            self._objects[name] = getattr(self, obj_type)(*args, **kwargs)
        return self._objects[name]

    def __getattr__(self, attr: str) -> Any:
        if attr in _mp_managed_objects:
            self._start()
        return getattr(self._manager, attr)


# globally usable manager for mp objects
mp_manager = DeferredManager(address=("localhost", 0))


def rel_path(anchor: str, *paths: Any) -> str:
    """
    Returns a path made of fragment *paths* relative to an *anchor* path.

    :param anchor: The anchor path. When it is a file, its absolute directory is used instead.
    :param paths: Path fragments to join.
    :return: The joined path.
    """
    anchor = os.path.abspath(os.path.expandvars(os.path.expanduser(str(anchor))))
    if os.path.exists(anchor) and os.path.isfile(anchor):
        anchor = os.path.dirname(anchor)
    return os.path.normpath(os.path.join(anchor, *map(str, paths)))


def law_src_path(*paths: Any) -> str:
    """
    Returns the law installation directory, optionally joined with *paths*.

    :param paths: Path fragments to join.
    :return: The path.
    """
    return rel_path(__file__, *paths)


def law_home_path(*paths: Any) -> str:
    """
    Returns the law home directory, optionally joined with *paths*.

    :param paths: Path fragments to join.
    :return: The path.
    """
    from law.config import law_home_path
    return law_home_path(*paths)


def law_run(argv: Sequence[str], **kwargs) -> int:
    """
    Runs a task with certain parameters as defined in *argv*. Example:

    .. code-block:: python

        law_run(["MyTask", "--param", "value"])
        law_run("MyTask --param value")

    :param argv: A string or a list of strings that starts with the family of the task to run,
        followed by the desired parameters.
    :param kwargs: Keyword arguments forwarded to :py:func:`luigi.interface.run`.
    :return: The exit code.
    """
    from luigi.cmdline_parser import CmdlineParser
    from luigi.interface import run as luigi_run

    from law.parser import _reset as reset_parser

    # ensure that argv is a list of strings
    argv = shlex.split(argv) if isinstance(argv, str) else [str(arg) for arg in argv]

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
    Aborts the process (*sys.exit*) with an *exitcode*.

    :param msg: Message to print first, to stdout when *exitcode* is 0 or *None*, and to stderr
        otherwise.
    :param exitcode: The exit code.
    :param color: Whether the message is printed in red when *exitcode* is not 0 or *None*.
    :return: The exit code, although this function never actually returns.
    """
    if msg is not None:
        if exitcode in (None, 0):
            print(msg)
        else:
            if color:
                msg = colored(msg, color="red")
            print(msg, file=sys.stderr)
    sys.exit(exitcode)
    return exitcode  # type: ignore[unreachable]


def import_file(path: str | pathlib.Path, attr: str | None = None) -> ModuleType | Any:
    """
    Loads the content of a python file located at *path* and returns its package content as a
    dictionary.

    The file is not required to be importable as its content is loaded directly into the
    interpreter. While this approach is not necessarily clean, it can be useful in places where
    custom code must be loaded.

    :param path: The path of the file.
    :param attr: When set, only the attribute with that name is returned.
    :raises AttributeError: When *attr* is set but the file does not contain it.
    :return: The package content as a dictionary, or the attribute *attr* when set.
    """
    # load the package contents
    path = os.path.expandvars(os.path.expanduser(str(path)))
    pkg = DotDict()
    with open(path, encoding="utf-8") as f:
        exec(f.read(), pkg)

    # extract a particular attribute
    if attr:
        if attr not in pkg:
            raise AttributeError(f"no local member '{attr}' found in file {path}")
        return pkg[attr]

    return pkg


def get_terminal_width(fallback: bool = False) -> int | None:
    """
    Returns the terminal width when possible.

    :param fallback: By default, the width is obtained through ``os.get_terminal_size``, querying
        the *sys.__stdout__* which might fail in case no valid output device is connected. When
        *True*, ``shutil.get_terminal_size`` is used instead, which priotizes the *COLUMNS* variable
        if set.
    :return: The terminal width, or *None* when it could not be determined.
    """
    width = None
    func = getattr(shutil if fallback else os, "get_terminal_size", None)
    if callable(func):
        with contextlib.suppress(OSError):
            width = func().columns

    return width


def is_classmethod(func: Any, cls: type | None = None) -> bool:
    """
    Returns whether *func* is a classmethod of *cls*.

    :param func: The function to check.
    :param cls: The class. When *None*, it is extracted from the function's qualified name and
        module name.
    :raises AttributeError: When *func* has no ``__name__`` attribute.
    :return: Whether *func* is a classmethod.
    """
    # when no cls is given, try to lookup it up in its associated module
    _hasattr = lambda attr: getattr(func, attr, None) is not None
    if cls is None and _hasattr("__qualname__") and _hasattr("__module__") and "." in func.__qualname__:
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
            return _cls.__dict__[func.__name__].__class__.__name__ == "classmethod"
    except AttributeError:
        return False

    return False


def is_number(n: Any) -> bool:
    """
    Returns whether *n* is a number, i.e., integer or float, and in particular no boolean.

    :param n: The value to check.
    :return: Whether *n* is a number.
    """
    return isinstance(n, (int, float)) and not isinstance(n, bool)


def is_float(v: Any) -> bool:
    """
    Takes any value *v* and tries to convert it to a float.

    :param v: The value to check.
    :return: Whether the conversion succeeded.
    """
    try:
        float(v)
        return True
    except Exception:
        return False


def try_int(n: int | float) -> int | float:
    """
    Takes a number *n* and tries to convert it to an integer.

    :param n: The number.
    :return: An integer with the same value as *n* when it has no decimals, and *n* as a float
        otherwise.
    """
    n_int = int(n)
    return n_int if n == n_int else n


def round_discrete(
    n: int | float,
    base: int | float = 1.0,
    round_fn: Callable[[int | float], float] | str = round,
) -> float:
    """ round_discrete(n, base=1.0, round_fn="round")
    Rounds a number *n* to a discrete *base*. Example:

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

    :param n: The number to round.
    :param base: The discrete base.
    :param round_fn: The function used for rounding, defaulting to the built-in ``round`` function.
        The string values ``"round"``, ``"floor"`` and ``"ceil"`` are resolved to the corresponding
        math functions.
    :raises ValueError: When *round_fn* is an unknown string.
    :return: The rounded number.
    """
    if isinstance(round_fn, str):
        if round_fn == "round":
            round_fn = round
        elif round_fn == "floor":
            round_fn = math.floor
        elif round_fn == "ceil":
            round_fn = math.ceil
        else:
            raise ValueError(f"unknown round function '{round_fn}'")

    return base * round_fn(float(n) / base)


def str_to_int(s: str) -> int:
    """
    Converts a string *s* into an integer under consideration of binary, octal, decimal and
    hexadecimal representations, such as ``"0o0660"``.

    :param s: The string to convert.
    :return: The integer.
    """
    s = str(s).strip().lower()
    m = re.match(r"^([+-]?)0([bodx])([0-9a-f_]+)$", s)
    if not m:
        return int(s, base=10)
    sign, prefix, digits = m.groups()
    return int(sign + digits, base={"b": 2, "o": 8, "d": 10, "x": 16}[prefix])


def flag_to_bool(s: str | bool, silent: bool = False) -> bool | None:
    """
    Takes a string flag *s* and returns whether it evaluates to *True* (values ``"1"``, ``"true"``
    ``"yes"``, ``"y"``, ``"on"``, case-insensitive) or *False* (values ``"0"``, ``"false"``,
    ``"no"``, ``"n"``, ``"off"``, case-insensitive).

    :param s: The flag. When it is already a boolean, it is returned unchanged.
    :param silent: When *True*, *None* is returned instead of raising an error when *s* is neither
        of the allowed values.
    :raises ValueError: When *s* is neither of the allowed values and *silent* is *False*.
    :return: The boolean value, or *None*.
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
    Returns a function that creates an empty context that yields *obj*, which can be used in case of
    dynamically choosing context managers while maintaining code structure.

    :param obj: The object to yield.
    :return: A function that creates the context manager.
    """
    @contextlib.contextmanager
    def context() -> Iterator[T]:
        yield obj

    return context


@contextlib.contextmanager
def empty_context(obj: Any | None = None) -> Iterator[T | None]:
    """
    Yields an empty context that can be used in case of dynamically choosing context managers while
    maintaining code structure.

    :param obj: The object to yield.
    :return: A context manager that yields *obj*.
    """
    yield obj


def common_task_params(task_instance, task_cls) -> dict[str, Any]:
    """
    Returns the parameters that are common between a *task_instance* and a *task_cls* with values
    taken directly from the task instance. The difference with respect to
    ``luigi.util.common_params`` is that the values are not parsed using the parameter objects of
    the task class, which might be faster for some purposes.

    :param task_instance: The task instance.
    :param task_cls: The task class.
    :return: A dictionary mapping parameter names to values.
    """
    task_cls_param_names = {name for name, _ in task_cls.get_params()}
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
    Returns the colored version of a string *msg*. For *color*, *background* and *style* options,
    see https://misc.flogisoft.com/bash/tip_colors_and_formatting.

    :param msg: The message to color.
    :param color: The text color. ``"random"`` results in a random color.
    :param background: The background color. ``"random"`` results in a random color.
    :param style: The style. ``"random"`` results in a random style. A sequence of styles is
        stacked.
    :param force: Unless *True*, *msg* is returned unchanged in case the output is neither a tty nor
        an IPython output stream.
    :return: The colored message.
    """
    msg = str(msg)

    if not force:
        tty = False
        ipy = False

        with contextlib.suppress(Exception):
            tty = os.isatty(sys.stdout.fileno())

        if not tty and ipykernel is not None:
            ipy = isinstance(sys.stdout, ipykernel.iostream.OutStream)

        if not tty and not ipy:
            return msg

    if isinstance(color, str):
        color = random.choice(list(colors.values())) if color == "random" else colors.get(color, colors["default"])
    elif not isinstance(color, int):
        color = colors["default"]

    if isinstance(background, str):
        background = (
            random.choice(list(backgrounds.values()))
            if background == "random"
            else backgrounds.get(background, backgrounds["default"])
        )
    elif not isinstance(background, int):
        background = backgrounds["default"]

    _styles = []
    for s in make_list(style):
        if isinstance(s, str):
            s = random.choice(list(styles.values())) if s == "random" else styles.get(s, styles["default"])
        elif not isinstance(s, int):
            s = styles["default"]
        _styles.append(s)
    style = ";".join(map(str, _styles))

    return f"\033[{style};{background};{color}m{msg}\033[0m"


def uncolored(s: str) -> str:
    """
    Removes all color codes from a string *s*.

    :param s: The string.
    :return: The string without color codes.
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
    Interactively queries a choice from the prompt until the input matches one of the *choices*.

    :param msg: The message to show.
    :param choices: The allowed choices.
    :param default: When not *None*, the choice used when the input is empty. Must be one of the
        *choices*.
    :param descriptions: Optional descriptions of the choices to show. Must have the same length as
        *choices*.
    :param lower: When *True*, the input is compared to the choices in lower case.
    :raises ValueError: When the length of *descriptions* does not match the length of *choices*, or
        when *default* is not one of the *choices*.
    :return: The chosen value.
    """
    choices: list[str] = [str(c) for c in choices]
    _choices = [c.lower() for c in choices] if lower else choices

    if default is not None and default not in choices:
        raise ValueError("default must be one of the choices")

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
        if default is not None and not choice:
            choice = default
        if lower:
            choice = choice.lower()

    return choice


def is_pattern(s: str) -> bool:
    """
    Returns whether the string *s* represents a pattern, i.e., if it contains characters such as
    ``"*"`` or ``"?"``.

    :param s: The string to check.
    :return: Whether *s* is a pattern.
    """
    return "*" in s or "?" in s


def brace_expand(s: str, split_csv: bool = False, escape_csv_sep: bool = True) -> list[str]:
    """
    Expands brace statements in a string *s* and returns a list containing all possible string
    combinations. Example:

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

    :param s: The string to expand.
    :param split_csv: When *True*, the input string is split by all comma characters located outside
        braces and the expansion is performed sequentially on all elements.
    :param escape_csv_sep: When *True*, escaped commas are not considered for splitting when
        *split_csv* is *True*.
    :raises ValueError: When the brace statements cannot be parsed.
    :return: The list of expanded strings.
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


#: Type of range tuples, either a single value or start and stop values, which might be open (*None*).
RangeTuple = Union[tuple[int], tuple[Union[int, None], Union[int, None]]]


def range_expand(
    s: str | Sequence[str] | RangeTuple | Sequence[RangeTuple],
    include_end: bool = False,
    min_value: int | None = None,
    max_value: int | None = None,
    sep: str = ":",
) -> list[int]:
    """
    Takes a string, or a sequence of strings in the format ``"1:3"``, or a sequence of tuples
    containing start and stop values of a range and returns a list of all intermediate values.

    One sided range expressions such as ``":4"`` or ``"4:"`` for strings and ``(None, 4)`` or ``(4,
    None)`` for tuples are also expanded but they require *min_value* and *max_value* to be set,
    with *max_value* being either included or not, depending on *include_end*.

    Example:

    .. code-block:: python

        range_expand("5:8")
        # -> [5, 6, 7]

        range_expand((6, 9))
        # -> [6, 7, 8]

        range_expand("5:8", include_end=True)
        # -> [5, 6, 7, 8]

        range_expand(["5:8", "10"])
        # -> [5, 6, 7, 10]

        range_expand(["5-8", "10"], sep="-")
        # -> [5, 6, 7, 10]

        range_expand(["5:8", "10:"])
        # -> Exception, no max_value set

        range_expand(["5:8", "10:"], max_value=12)
        # -> [5, 6, 7, 10, 11]

        range_expand(["5:8", "10:"], max_value=12, include_end=True)
        # -> [5, 6, 7, 8, 10, 11, 12]

    :param s: The range expression(s) to expand.
    :param include_end: Whether end values are included.
    :param min_value: The minimum value, used for ranges with missing start values and to limit the
        expanded ranges.
    :param max_value: The maximum value, used for ranges with missing stop values and to limit the
        expanded ranges.
    :param sep: The separator between start and stop values in strings.
    :raises ValueError: When a range expression is invalid, or when a one sided range is used
        without *min_value* or *max_value* being set.
    :return: The list of expanded values.
    """
    def to_int(v: Any, s: Any | None = None) -> int:
        try:
            return int(v)
        except ValueError as e:
            raise ValueError(f"invalid number or range '{v if s is None else s}'") from e

    # make_list is used below, but need to distinguish between single range tuples and tuples of them
    numbers = []
    for v in ([s] if isinstance(s, tuple) and not is_nested(s) else make_list(s)):
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
            v = str(v)
            if sep in v:
                parts = v.split(sep, 1)
                start = parts[0] or None
                stop = parts[1] or None
            else:
                value = v
                single_value = True

        if single_value:
            # add a single value
            numbers.append(to_int(value))

        else:
            # build the range
            if start is None:
                if min_value is None:
                    raise ValueError(f"range '{v}' with missing start value requires min_value to be set")
                start = min_value
            if stop is None:
                if max_value is None:
                    raise ValueError(f"range '{v}' with missing stop value requires max_value to be set")
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
    Takes a sequence of positive integer numbers and returns a sequence 1- and 2-tuples, denoting
    either single numbers or start and end values of possible ranges. Example:

    .. code-block:: python

        range_join([1, 2, 3, 5])
        # -> [(1, 4), (5,)]

        range_join([1, 2, 3, 5], include_end=True)
        # -> [(1, 3), (5,)]

        range_join([1, 2, 3, 5, 7, 8, 9])
        # -> [(1, 4), (5,), (7, 10)]

        range_join([1, 2, 3, 5, 7, 8, 9], to_str=True)
        # -> "1:4,5,7:10"

    :param numbers: The numbers, given either as integers or strings.
    :param to_str: When *True*, a string is returned in a format consistent with
        :py:func:`range_expand`.
    :param include_end: Whether end values are included.
    :param sep: The separator between ranges when *to_str* is *True*.
    :param range_sep: The separator between start and end values when *to_str* is *True*.
    :raises ValueError: When a string cannot be converted to an integer.
    :raises TypeError: When a number is not an integer.
    :return: The list of tuples, or a string when *to_str* is *True*.
    """
    if not numbers:
        return "" if to_str else []

    # check type, convert, make unique and sort
    _numbers: list[int] = []
    for n in numbers:
        if isinstance(n, str):
            try:
                n = int(n)
            except ValueError as e:
                raise ValueError(f"invalid number format '{n}'") from e
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
    regex: bool | None = None,
    skip_negation: bool = False,
) -> bool:
    """
    Compares *name* to multiple *patterns*.

    :param name: The name to compare.
    :param patterns: One or multiple patterns. Patterns starting with ``"!"`` are negated unless
        *skip_negation* is *True*.
    :param mode: Either :py:func:`any` to require at least one match, or :py:func:`all` to require
        all patterns to match.
    :param regex: When *True*, :py:func:`re.match` is used instead of :py:func:`fnmatch.fnmatch`.
        When *None*, the matching function is chosen per pattern: when containing both ``"^"`` and
        ``"$"``, regex matching is used, and fnmatch otherwise.
    :param skip_negation: Whether to disable the negation of patterns starting with ``"!"``.
    :return: Whether *name* matches according to *mode*.
    """
    patterns = make_list(patterns)

    # negation helper
    if skip_negation:
        negate = lambda pattern: (True, pattern)
    else:
        negate = lambda pattern: (False, pattern[1:]) if pattern.startswith("!") else (True, pattern)

    # generic matching functions with identical signature
    def match_func_fn(pattern):
        state, pattern = negate(pattern)
        return bool(fnmatch.fnmatch(name, pattern)) is state

    def match_func_re(pattern):
        state, pattern = negate(pattern)
        return bool(re.match(pattern, name)) is state

    # determine the matching function
    match_func = match_func_fn
    if regex is None:
        match_func = lambda pattern: (
            match_func_re(pattern)
            if "^" in pattern and "$" in pattern
            else match_func_fn(pattern)
        )
    elif regex:
        match_func = match_func_re

    # perform the matching
    return mode(match_func(pattern) for pattern in patterns)


def is_iterable(obj: Any) -> bool:
    """
    Returns whether an object *obj* is iterable.

    :param obj: The object to check.
    :return: Whether *obj* is iterable.
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

    :param obj: The object to check.
    :return: Whether *obj* is a lazy iterable.
    """
    return isinstance(obj, lazy_iter_types)


def make_list(obj: Any, cast: bool = True) -> list[Any]:
    """
    Converts an object *obj* to a list.

    :param obj: The object to convert.
    :param cast: Whether objects of types *tuple* and *set* are converted. Otherwise, and for all
        other types, *obj* is put in a new list.
    :return: The list.
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
    Converts an object *obj* to a tuple.

    :param obj: The object to convert.
    :param cast: Whether objects of types *list* and *set* are converted. Otherwise, and for all
        other types, *obj* is put in a new tuple.
    :return: The tuple.
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
    Converts an object *obj* to a set.

    :param obj: The object to convert.
    :param cast: Whether objects of types *list* and *tuple* are converted. Otherwise, and for all
        other types, *obj* is put in a new set.
    :return: The set.
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
    Takes a list or tuple *obj* and removes duplicate elements in order of their appearance.

    :param obj: The list, tuple or other iterable.
    :raises TypeError: When *obj* is not iterable.
    :return: The sequence of unique elements with the same type as *obj*, or a list when *obj* is
        neither a list nor a tuple.
    """
    if not isinstance(obj, (list, tuple)):
        if not is_iterable(obj) and not is_lazy_iterable(obj):
            raise TypeError("object is neither list, tuple, nor generic iterable")
        obj = list(obj)

    ret = sorted(obj.__class__(set(obj)), key=obj.index)

    return obj.__class__(ret) if isinstance(obj, tuple) else ret


def is_nested(obj: Any) -> bool:
    """
    Takes a list or tuple *obj* and checks whether it only contains items of types list and tuple.

    :param obj: The object to check.
    :return: Whether *obj* is nested.
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
    Takes one or multiple complex structured objects *structs* and flattens them.

    :param structs: The objects to flatten.
    :param flatten_dict: Whether dictionaries are flattened. If not, they are returned unchanged.
    :param flatten_list: Whether lists are flattened. If not, they are returned unchanged.
    :param flatten_tuple: Whether tuples are flattened. If not, they are returned unchanged.
    :param flatten_set: Whether sets are flattened. If not, they are returned unchanged.
    :return: A single flat list.
    """
    if len(structs) == 0:
        return []

    kwargs = {
        "flatten_dict": flatten_dict,
        "flatten_list": flatten_list,
        "flatten_tuple": flatten_tuple,
        "flatten_set": flatten_set,
    }
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
    Example:

    .. code-block:: python

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": {"c": 3}})
        # -> {"foo": 1, "bar": {"c": 3}}  # fully replaced "bar"

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": {"c": 3}}, deep=True)
        # -> {"foo": 1, "bar": {"a": 1, "b": 2, "c": 3}}  # inserted entry bar.c

        merge_dicts({"foo": 1, "bar": {"a": 1, "b": 2}}, {"bar": 2}, deep=True)
        # -> {"foo": 1, "bar": 2}  # "bar" has a different type, so this just uses the rear value

    :param dicts: The dictionaries to merge.
    :param inplace: When *True*, all update operations are performed inplace on the first object in
        *dicts* instead of returning a new dictionary.
    :param cls: The class of the returned dictionary when not inplace. When *None*, it is inferred
        from the first dict object in *dicts*.
    :param deep: When *True*, dictionary types within the dictionaries to merge are updated
        recursively such that their fields are merged, which is only possible when input
        dictionaries have a similar structure.
    :raises ValueError: When *dicts* is empty.
    :raises TypeError: When *cls* cannot be inferred as none of the passed objects is a dictionary.
    :return: The merged dictionary.
    """
    if not dicts:
        raise ValueError("cannot merge empty sequence of dictionaries")

    inplace = kwargs.get("inplace", False)
    if inplace:
        merged_dict = dicts[0]
    else:
        # get or infer the class
        cls = kwargs.get("cls")
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
                # just take the value as is when it is not a dict, and when the field is either not
                # existing yet or not a dict in the merged dict, use a (deep) copy so that subsequent
                # merges do not alter the input dict
                if not isinstance(v, dict):
                    merged_dict[k] = v
                elif not isinstance(merged_dict.get(k), dict):
                    merged_dict[k] = merge_dicts(v, deep=True)
                else:
                    # merge by recursion
                    merge_dicts(merged_dict[k], v, inplace=True, deep=deep)
        else:
            merged_dict.update(d)

    return merged_dict


def unzip(struct: Iterable, fill_none: bool = False) -> tuple[list[Any], ...] | None:
    """
    Unzips a *struct* consisting of sequences with equal lengths and returns lists with 1st, 2nd,
    etc elements. This function can be thought of as the opposite of the ``zip`` builtin. The number
    of elements per returned list is determined by the length of the first sequence in *struct*.

    .. code-block:: python

        unzip([(1, 2), (3, 4)])
        # -> ([1, 3], [2, 4])

        unzip([(1, 2), (3,)])
        # -> ValueError

        unzip([(1, 2), (3,)], fill_none=True)
        # -> ([1, 3], [2, None])

    :param struct: The sequences to unzip.
    :param fill_none: When *True*, *None* is inserted for missing items of shorter sequences.
    :raises ValueError: When a sequence contains fewer items than the first one and *fill_none* is
        *False*.
    :return: A tuple of lists.
    """
    lists: tuple | None = None
    for j, obj in enumerate(struct):
        # determine the number of lists to return
        if lists is None:
            lists = tuple([] for _ in range(len(obj)))

        # fill them
        for i, _list in enumerate(lists):
            if len(obj) > i:
                _list.append(obj[i])
            elif fill_none:
                _list.append(None)
            else:
                raise ValueError(
                    f"insufficient length {len(obj)} of sequence at index {j} to unzip, expected "
                    f"{len(lists)}",
                )

    return lists


def which(prog: str) -> str | None:
    """
    Pythonic ``which`` implementation that searches for an executable *prog* in *PATH*.

    :param prog: The name of the executable.
    :return: The path to the executable, or *None* when it could not be found.
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
    Example:

    .. code-block:: python

        func = lambda x: x ** 2
        msg = "computing square of {}"
        squares = map_verbose(func, range(7), msg, every=3)
        # ->
        # computing square of 0
        # computing square of 2
        # computing square of 5
        # computing square of 6

    :param func: The function to apply.
    :param seq: The sequence to iterate over.
    :param msg: A template string that is formatted with the current iteration number (starting at
        0) plus *offset* using ``str.format``.
    :param every: The number of iterations after which *msg* is printed.
    :param start: Whether *msg* is also printed after the first iteration.
    :param end: Whether *msg* is also printed after the last iteration.
    :param offset: An offset added to the iteration number in *msg*.
    :param callback: When callable, it is invoked instead of the default print method with the
        current iteration number (without *offset*) as the only argument.
    :return: The list of results.
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

    The following example would traverse lists backwards using *custom_mappings*:

    .. code-block:: python

        def traverse_lists(func, l, **kwargs):
            return [map_struct(func, v, **kwargs) for v in l[::-1]]

        map_struct(times_two, struct, custom_mappings={list: traverse_lists})
        # -> {"foo": [912, 246], "bar": [{"1": 2}, {"2": 4}]}

    :param func: The function to apply.
    :param struct: The structure to traverse.
    :param map_dict: Whether dictionaries are traversed or mapped as a whole. An integer value
        defines the depth of that setting in the struct.
    :param map_list: Whether lists are traversed or mapped as a whole. An integer value defines the
        depth of that setting in the struct.
    :param map_tuple: Whether tuples are traversed or mapped as a whole. An integer value defines
        the depth of that setting in the struct.
    :param map_set: Whether sets are traversed or mapped as a whole. An integer value defines the
        depth of that setting in the struct.
    :param cls: When not *None*, it exclusively defines the class of objects that *func* is applied
        on. All other objects are unchanged.
    :param custom_mappings: A dictionary that maps custom types to custom object traversal methods.
    :return: The mapped structure.
    """
    # interpret generators and views as lists
    if is_lazy_iterable(struct):
        struct = list(struct)

    # determine valid types for struct traversal
    valid_types: tuple[type, ...] = ()
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
            add = lambda _, value: new_struct.append(value)  # type: ignore[attr-defined]
        elif isinstance(struct, set):
            gen = enumerate(struct)
            add = lambda _, value: new_struct.add(value)  # type: ignore[attr-defined]
        elif isinstance(struct, dict):
            gen = struct.items()  # type: ignore[assignment]
            add = new_struct.__setitem__  # type: ignore[index]
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
            new_struct = struct.__class__(new_struct)  # type: ignore[arg-type]

        return new_struct

    # apply the mapping function on everything else
    return func(struct)


def mask_struct(
    mask: bool | Sequence[Any] | dict[Any, Any],
    struct: Any,
    replace: Any | NoValue = no_value,
    keep_missing: bool = True,
    convert_types: dict[type | tuple[type, ...], Callable[[Any], Any]] | None = None,
) -> Any:
    """
    Masks a complex structured object *struct* with a *mask* and returns the remaining values.
    Examples:

    .. code-block:: python

        struct = {"a": [1, 2], "b": [3, ["foo", "bar"]]}

        # simple example
        mask_struct({"a": [False, True], "b": False}, struct)
        # => {"a": [2]}

        # omitting mask information results in kept values
        mask_struct({"a": [False, True]}, struct)
        # => {"a": [2], "b": [3, ["foo", "bar"]]}

    :param mask: The mask, which can have a complex structure as well.
    :param struct: The structure to mask.
    :param replace: When set, masked values are replaced with that value instead of being removed.
    :param keep_missing: Whether items in *struct* that are not matched by a value in *mask* are
        kept.
    :param convert_types: A dictionary containing conversion functions mapped to types (or tuples
        thereof) that is applied to objects during the struct traversal if their types match.
    :raises TypeError: When *mask* and *struct* have incompatible types.
    :return: The masked structure.
    """
    # interpret lazy iterables lists
    if is_lazy_iterable(struct):
        struct = list(struct)

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
                if keep_missing:
                    new_struct.append(val)
            else:
                repl = replace
                if isinstance(replace, (list, tuple)) and len(replace) > i:
                    repl = replace[i]
                val = mask_struct(
                    mask[i],
                    val,
                    replace=repl,
                    keep_missing=keep_missing,
                    convert_types=convert_types,
                )
                if val != no_value:
                    new_struct.append(val)

        return struct.__class__(new_struct) if new_struct else replace

    # check dict types
    if isinstance(struct, dict) and isinstance(mask, dict):
        new_struct: dict = struct.__class__()
        for key, val in struct.items():
            if key not in mask:
                if keep_missing:
                    new_struct[key] = val
            else:
                repl = replace
                if isinstance(replace, dict) and key in replace:
                    repl = replace[key]
                val = mask_struct(
                    mask[key],
                    val,
                    replace=repl,
                    keep_missing=keep_missing,
                    convert_types=convert_types,
                )
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
    temporary path, and eventually removes it. The behavior of this function is similar to
    ``tempfile.NamedTemporaryFile`` which, however, yields an already opened file object.

    :param args: Arguments forwarded to :py:func:`tempfile.mkstemp`.
    :param kwargs: Keyword arguments forwarded to :py:func:`tempfile.mkstemp`.
    :return: A context manager that yields a 2-tuple with the file descriptor number and the path.
    """
    fileno, path = tempfile.mkstemp(*args, **kwargs)

    # create the file
    with open(path, "w", encoding="utf-8") as f:
        f.write("")

    # yield it
    try:
        yield fileno, path
    finally:
        if os.path.exists(path):
            os.remove(path)


def interruptable_popen(
    cmd: str | list[str] | os.PathLike,
    *args,
    stdin_callback: Callable[[], Any] | None = None,
    stdin_delay: int | float = 0,
    interrupt_callback: Callable[[subprocess.Popen], Any] | None = None,
    kill_timeout: int | float | None = None,
    processes: list | None = None,
    **kwargs,
) -> tuple[int, str | None, str | None]:
    """
    Shorthand to :py:class:`Popen` followed by :py:meth:`Popen.communicate` which can be interrupted
    by *KeyboardInterrupt*.

    The default value of *stdin* depends on whether a *stdin_callback* is provided. It is set to
    ``subprocess.PIPE`` if *stdin_callback* is set, and to ``subprocess.DEVNULL`` otherwise. In case
    the subprocess should "inherit" the standard input of the parent process, *stdin* should be
    manually set to ``None``.

    :param cmd: The command, forwarded to the :py:class:`Popen` constructor.
    :param args: Arguments forwarded to the :py:class:`Popen` constructor.
    :param stdin_callback: A function accepting no arguments and whose return value is passed to
        ``communicate`` after a delay of *stdin_delay* to feed data input to the subprocess.
    :param stdin_delay: The delay in seconds before *stdin_callback* is invoked.
    :param interrupt_callback: A function, accepting the process instance as an argument, that is
        called immediately after a *KeyboardInterrupt* occurs. After that, a SIGTERM signal is sent
        to the subprocess to allow it to gracefully shutdown.
    :param kill_timeout: When set, and the process is still alive after that period (in seconds)
        after an interrupt, a SIGKILL signal is sent to force the process termination.
    :param processes: When set, the process is appended to it right after it was created. This can
        be useful to keep track of multiple processes and sending signals to them from an outer
        context.
    :param kwargs: Keyword arguments forwarded to the :py:class:`Popen` constructor.
    :return: A 3-tuple with the return code, standard output and standard error.
    """
    # default stdin setting
    kwargs.setdefault("stdin", subprocess.PIPE if callable(stdin_callback) else subprocess.DEVNULL)

    # transform the command depending on the shell setting
    shell = kwargs.get("shell", False)
    if shell and isinstance(cmd, (list, tuple)):
        cmd = quote_cmd(cmd)
    elif not shell and isinstance(cmd, str):
        cmd = shlex.split(cmd)

    # start the subprocess
    p = subprocess.Popen(cmd, *args, **kwargs)

    # add to processes list
    if processes is not None:
        processes.append(p)

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

        # kill it
        kill_process(p, kill_timeout=kill_timeout)

        # transparently reraise
        raise

    # decode
    if out is not None:
        out = out.decode("utf-8")
    if err is not None:
        err = err.decode("utf-8")

    return p.returncode, out, err


def send_signal_silent(pid: int, sig: int) -> bool:
    """
    Sends a signal *sig* to a process with id *pid*.

    :param pid: The process id.
    :param sig: The signal.
    :return: Whether the signal was sent, i.e., the process existed.
    """
    try:
        os.kill(pid, sig)
    except ProcessLookupError:
        return False
    return True


def get_subprocess_pids(pid: int, recursive: bool = False, use_psutil: bool = True) -> list[int]:
    """
    Given the *pid* of a process, returns a list of ids for all subprocesses.

    :param pid: The process id.
    :param recursive: When *True*, the list includes all descendant processes as well.
    :param use_psutil: Unless *False*, the implementation uses the psutil library if installed.
    :raises TypeError: When *pid* is not an integer.
    :raises RuntimeError: When the subprocess ids could not be determined without psutil.
    :return: The list of process ids.
    """
    if not isinstance(pid, int):
        raise TypeError(f"pid must be an integer, got '{pid}'")

    # check for psutil
    if use_psutil:
        try:
            import psutil
        except ImportError:
            use_psutil = False

    # psutil implementation
    if use_psutil:
        try:
            p = psutil.Process(pid)
            p_time = p.create_time()
        except psutil.NoSuchProcess:
            return []

        pids = []
        for sub in p.children(recursive=recursive):
            sub_time = sub.create_time()
            if sub_time >= p_time:
                pids.append(sub.pid)

        return pids

    # fallback to cross-platform 'ps' lookup with 1s resolution
    def _get_subprocess_pids(pid):
        # get process info
        cmd = f"ps -eo ppid=,pid=,lstart= | awk '$1 == {pid} || $2 == {pid} {{ print $0 }}'"
        out: str
        code, out, err = interruptable_popen(  # type: ignore[assignment]
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if code != 0:
            raise RuntimeError(f"failed to get subprocess pids for pid {pid}: {err}")
        # parse output into pid -> start time mapping
        pids_times = {}
        for line in out.strip().splitlines():
            line = line.strip()
            if line:
                pid_str, time_str = line.split(None, 2)[1:]
                pids_times[int(pid_str)] = datetime.datetime.strptime(time_str, r"%a %b %d %H:%M:%S %Y").timestamp()
        if not pids_times:
            return []
        if pid not in pids_times:
            logger.error(f"pid {pid} not found in 'ps' output:\n{out}")
            return []
        # return pids of existing subprocesses created after the main process
        # (to protect against pid reuse)
        p_time = pids_times.pop(pid)
        return [
            _pid for _pid, sub_time in pids_times.items()
            if sub_time >= p_time and send_signal_silent(_pid, 0)
        ]

    # potentially recursive lookup with depth-first ordering
    pids = []
    q = collections.deque(_get_subprocess_pids(pid))
    while q:
        _pid = q.popleft()
        pids.append(_pid)
        if recursive:
            q.extendleft(_get_subprocess_pids(_pid)[::-1])

    return pids


def kill_process(
    p: subprocess.Popen,
    recursive: bool = True,
    kill_timeout: int | float | None = None,
) -> None:
    """
    Terminates a running process *p* with SIGTERM.

    :param p: The process.
    :param recursive: When *True*, the termination of all subprocesses is enforced as well (if not
        already triggered by the main process termination).
    :param kill_timeout: When set, and the process is still running after that period (in seconds),
        a SIGKILL signal is sent to force the termination.
    """
    # do nothing when the process does no longer exist
    if not send_signal_silent(p.pid, 0):
        return

    # helper to check if the process is still alive
    def alive():
        return p.poll() is None

    # helper to perform the actual, potentially recursive process termination
    def kill(sig):
        # nothing to do when already terminated
        if not alive():
            return
        # gather pids to send the signal to
        pids = [p.pid]
        if recursive:
            pids += get_subprocess_pids(p.pid, recursive=True)
        # send signal in order
        for pid in pids:
            send_signal_silent(pid, sig)

    # start with SIGTERM to allow graceful shutdown
    kill(signal.SIGTERM)

    # when still alive and a timeout is set, send SIGKILL after that time
    if kill_timeout is not None and alive():
        target_time = time.perf_counter() + kill_timeout
        while time.perf_counter() < target_time:
            time.sleep(0.05)
            if not alive():
                break
        else:
            kill(signal.SIGKILL)


def readable_popen(*args, **kwargs) -> tuple[subprocess.Popen, Iterable[str]]:
    """
    Creates a :py:class:`Popen` object and a generator function yielding the output line-by-line as
    it comes in. Example:

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

    :param args: Arguments forwarded to the :py:class:`Popen` constructor.
    :param kwargs: Keyword arguments forwarded to the :py:class:`Popen` constructor. *stdout* and
        *stderr* are overwritten.
    :return: A 2-tuple with the :py:class:`Popen` object and the line generator.
    """
    # force pipes
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.STDOUT

    p = subprocess.Popen(*args, **kwargs)

    def line_gen():
        for line in p.stdout:  # type: ignore[union-attr]
            yield line.decode("utf-8").rstrip()

        # communicate in the end
        p.communicate()

    return p, line_gen()


def create_hash(inp: Any, length: int = 10, algo: str = "sha256", to_int: bool = False) -> str | int:
    """
    Takes an arbitrary input *inp* and creates a hexadecimal string hash.

    :param inp: The input, which is converted to a string first.
    :param length: The maximum length of the returned hash, limited by the length of the hexadecimal
        representation produced by the hashing algorithm.
    :param algo: The name of the algorithm. For valid algorithms, see python's hashlib.
    :param to_int: When *True*, the decimal integer representation is returned.
    :return: The hash.
    """
    h = getattr(hashlib, algo)(str(inp).encode("utf-8")).hexdigest()[:length]
    return int(h, 16) if to_int else h


def compute_sha1_hash(path: str | pathlib.Path, to_int: bool = False) -> str | int:
    """
    Computes the SHA1 hash of a file located at *path*.

    :param path: The path of the file.
    :param to_int: When *True*, the decimal integer representation is returned.
    :raises RuntimeError: When the hash computation failed.
    :return: The hash as a hexadecimal string, or as an integer when *to_int* is *True*.
    """
    path = os.path.abspath(os.path.expandvars(os.path.expanduser(str(path))))
    cmd = ["sha1sum", path]
    code, out, _ = interruptable_popen(
        cmd,
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
    )
    if code != 0:
        raise RuntimeError(f"failed to compute sha1 hash for file '{path}'")

    h = out.split()[0]  # type: ignore[union-attr]
    return int(h, 16) if to_int else h


def create_random_string(length: int = 10, prefix: str = "") -> str:
    """
    Creates a random string using a uuid4 hash.

    :param length: The number of random characters.
    :param prefix: When given, the string will have the format ``<prefix>_<random_string>``.
    :return: The random string.
    """
    s = ""
    while len(s) < length:
        s += uuid.uuid4().hex
    s = s[:length]
    if prefix:
        s = f"{prefix}_{s}"
    return s


def copy_no_perm(src: str | pathlib.Path, dst: str | pathlib.Path) -> None:
    """
    Copies a file from *src* to *dst* including meta data except for permission bits.

    :param src: The source path.
    :param dst: The destination path.
    """
    src, dst = str(src), str(dst)
    shutil.copyfile(src, dst)
    perm = os.stat(dst).st_mode
    shutil.copystat(src, dst)
    os.chmod(dst, perm)


def makedirs(path: str | pathlib.Path, perm: int | None = None) -> None:
    """
    Recursively creates directories up to *path*. No exception is raised if *path* refers to an
    existing directory.

    :param path: The path of the directory.
    :param perm: When set, the permissions of all newly created directories are set to this value.
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
    Returns whether a file located at *path* is owned by the user with *uid*.

    :param path: The path of the file.
    :param uid: The user id. When *None*, the user id of the current process is used.
    :return: Whether the user owns the file.
    """
    if uid is None:
        uid = os.getuid()
    path = os.path.expandvars(os.path.expanduser(str(path)))
    return os.stat(path).st_uid == uid


def increment_path(path: str | pathlib.Path, n: int | None = None) -> str:
    """
    Takes a file path *path* and returns a new path with a counter appended to the basename.

    :param path: The path.
    :param n: When a number, the counter is increased by that number. When *None*, a new counter is
        determined by checking the directory for existing files with the same basename.
    :return: The new path.
    """
    path = os.path.abspath(os.path.expandvars(os.path.expanduser(str(path))))
    dirname, basename = os.path.split(path)
    basename, ext = os.path.splitext(basename)

    # check if basename already contains a trailing counter
    m = re.match(r"^(.+)_(\d+)$", basename)
    counter = 0
    if m:
        basename = m.group(1)
        counter = int(m.group(2))

    # helper to determine the incremented path
    next_path = lambda i: os.path.join(dirname, f"{basename}_{counter + i}{ext}")

    # when a number is given in n, just increase the counter by that number
    if n is not None:
        return next_path(n)

    # when n is none, perform a full collision handling in the directory
    _path = path
    i = 0
    while True:
        if not os.path.exists(_path):
            return _path
        i += 1
        _path = next_path(i)


def iter_chunks(obj: int | Iterable[Any], size: int) -> Iterator[list[int | Any]]:
    """
    Returns a generator containing chunks of *size* of a list, integer or generator *obj*.

    :param obj: The object to chunk. An integer is interpreted as a range.
    :param size: The size of chunks. A size smaller than 1 results in no chunking at all.
    :return: Generator that yields lists of chunked elements.
    """
    _l: Iterable = range(obj) if isinstance(obj, int) else obj

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


def chunk_slice_ranges(
    sizes: Sequence[int | Sized],
    start: int | None = None,
    stop: int | None = None,
) -> list[tuple[int, int] | None]:
    """
    Takes a list of chunk *sizes* and desired *start* and *stop* indices to return a list of
    2-tuples marking the slice indices for each size so that the total *start* and *stop* indices
    are covered. Example:

    .. :code-block:: python

        slice_ranges([10, 10, 10], 5, 15)
        # -> [(5, 10), (0, 5), None]

        slice_ranges([10, 10, 10], 15, 25)
        # -> [None, (5, 10), (0, 5)]

        slice_ranges([10, 10, 10], 5, -5)
        # -> [(5, 10), (0, 10), (0, 5)]

    :param sizes: The chunk sizes, which can also be iterables whose sizes are used instead.
    :param start: The total start index.
    :param stop: The total stop index. It is allowed to be negative, using the total size as a
        reference.
    :raises ValueError: When *start* and *stop* are invalid for the total size.
    :return: A list with the same length as *sizes*, containing 2-tuples or *None* in case a chunk
        is not covered.
    """
    # convert sizes to integers
    _sizes = [(s if isinstance(s, int) else len(s)) for s in sizes]
    total_size = sum(_sizes)

    # boundary checks
    if start is None:
        start = 0
    if stop is None:
        stop = total_size
    stop_orig = stop
    if stop < 0:
        stop += total_size
    if not (0 <= start <= stop <= total_size):
        raise ValueError(f"invalid start and stop indices {start} and {stop_orig} for total size {total_size}")

    # slicing algorithm
    indices: list[tuple[int, int] | None] = len(_sizes) * [None]  # type: ignore[assignment]
    for i, size in enumerate(_sizes):
        if start >= size:
            start -= size
            stop -= size
        elif stop > size:
            indices[i] = (start, size)
            start = 0
            stop -= size
        else:
            indices[i] = (start, stop)
            break

    return indices


byte_units = ["bytes", "kB", "MB", "GB", "TB", "PB", "EB"]
byte_units_lower = [u.lower() for u in byte_units]


def human_bytes(
    n: int | float,
    unit: str | None = None,
    fmt: Callable[[str, str], str] | Any = None,
) -> tuple[float, str] | str:
    """
    Takes a number of bytes *n*, assigns the best matching unit and returns the respective number
    and unit string. Example:

    .. code-block:: python

        human_bytes(3407872)
        # -> (3.25, "MB")

        human_bytes(3407872, "kB")
        # -> (3328.0, "kB")

        human_bytes(3407872, fmt="{:.2f} -- {}")
        # -> "3.25 -- MB"

        human_bytes(3407872, fmt=True)
        # -> "3.2 MB"

    :param n: The number of bytes.
    :param unit: When set, that unit is used.
    :param fmt: When set, a string template with two elements that are filled via *str.format*. It
        can also be a boolean value in which case the template defaults to ``"{:.1f} {}"`` when
        *True*.
    :raises ValueError: When *unit* is unknown.
    :return: A 2-tuple with the number and the unit, or a formatted string when *fmt* is set.
    """
    # check if the unit exists
    if unit and unit not in byte_units:
        raise ValueError(f"unknown unit '{unit}', valid values are {byte_units}")

    if unit:
        idx = byte_units.index(unit)
    elif n == 0:
        idx = 0
    else:
        idx = math.floor(math.log(abs(n), 1024))
        idx = min(max(idx, 0), len(byte_units) - 1)

    # get the value and the unit name
    value = n / 1024.0 ** idx
    unit = byte_units[idx]

    # vast value to int when the unit is bytes
    if idx == 0:
        value = round(value)

    if fmt:
        if not isinstance(fmt, str):
            fmt = "{} {}" if idx == 0 else "{:.1f} {}"
        return fmt.format(value, unit)

    return value, unit


def parse_bytes(s: str | int | float, input_unit: str = "bytes", unit: str = "bytes") -> float:
    """
    Takes a string *s*, interprets it as a size with an optional unit, and returns a float that
    represents that size in a given *unit*. Example:

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

    :param s: The size to parse.
    :param input_unit: The unit used when no unit is found in *s*.
    :param unit: The unit of the returned size.
    :raises ValueError: When *s* cannot be parsed, or when *unit* or *input_unit* is unknown.
    :return: The size in *unit*.
    """
    # check if the units exists
    if input_unit.lower() not in byte_units_lower:
        raise ValueError(f"unknown input_unit '{input_unit}', valid values are {byte_units}")
    if unit.lower() not in byte_units_lower:
        raise ValueError(f"unknown unit '{unit}', valid values are {byte_units}")

    # when s is a number, interpret it as bytes right away
    # otherwise parse it
    if isinstance(s, (int, float)):
        input_value = float(s)
    else:
        m = re.match(r"^\s*(-?\d+\.?\d*)\s*(|{})\s*$".format("|".join(byte_units_lower)), s.lower())
        if not m:
            raise ValueError(f"cannot parse bytes from string '{s}'")

        input_value = float(m.group(1))
        _input_unit = m.group(2)
        if _input_unit:
            input_unit = _input_unit

    # convert the input value to bytes
    idx = byte_units_lower.index(input_unit.lower())
    size_bytes = input_value * 1024.0 ** idx

    # convert to the output unit
    return size_bytes / 1024.0 ** byte_units_lower.index(unit.lower())


time_units: dict[str, int] = {
    "week": 7 * 24 * 60 * 60,
    "day": 24 * 60 * 60,
    "hour": 60 * 60,
    "minute": 60,
    "second": 1,
}

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
    Returns a human readable duration. The largest unit is days. Example:

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

    :param colon_format: When *True*, the return value has the format ``"[d-][hh:]mm:ss[.ms]"``. It
        can also be a string value referring to a limiting unit. In that case, the returned time
        string has no field above that unit, e.g. passing ``"m"`` results in a string
        ``"mm:ss[.ms]"`` where the minute field is potentially larger than 60. Passing ``"s"`` is a
        special case. Since the colon format always has a minute field (to mark it as colon format
        in the first place), the returned string will have the format ``"00:ss[.ms]"``.
    :param plural: Unless *False*, units corresponding to values other than **exactly** one are used
        in plural e.g. ``"1 second"`` but ``"1.5 seconds"``.
    :param kwargs: Keyword arguments forwarded to ``datetime.timedelta`` to get the total duration
        in seconds.
    :raises ValueError: When *colon_format* refers to an unknown unit.
    :return: The human readable duration.
    """
    _time_units = ["day", "hour", "minute", "second"]

    seconds = float(datetime.timedelta(**kwargs).total_seconds())
    sign = 1 if seconds >= 0 else -1
    # round to 2 digits before splitting into units so that rounding carries over to larger units
    seconds = round(abs(seconds), 2)

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
    represents that duration in a given *unit*. Multiple input formats are parsed. Example:

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

    :param s: The duration to parse.
    :param input_unit: The unit used when no unit is found in *s*.
    :param unit: The unit of the returned duration.
    :raises ValueError: When *s* cannot be parsed, or when *unit* or *input_unit* is unknown.
    :return: The duration in *unit*.
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
    Lightweight mail functionality that sends a mail from *sender* to *recipient*.

    :param recipient: The recipient address.
    :param sender: The sender address.
    :param subject: The subject.
    :param content: The content.
    :param smtp_host: The host forwarded to the ``smtplib.SMTP`` constructor.
    :param smtp_port: The port forwarded to the ``smtplib.SMTP`` constructor.
    :return: Whether the mail was sent successfully.
    """
    try:
        server = smtplib.SMTP(smtp_host, smtp_port)
    except Exception as e:
        logger.warning(f"cannot create SMTP server {smtp_host}:{smtp_port}: {e}")
        return False

    header = f"From: {sender}\r\nTo: {recipient}\r\nSubject: {subject}\r\n\r\n"
    server.sendmail(sender, recipient, header + content)

    return True


class DotDict(collections.OrderedDict):
    """
    OrderedDict subclass that provides read access for items via attributes by implementing
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
        return GenericAlias(cls, types)

    @classmethod
    def wrap(cls, *args, **kwargs) -> DotDict:
        """
        Creates a dictionary and recursively replaces it and all other nested dictionary types with
        :py:class:`DotDict`'s for deep attribute-style access.

        :param args: Arguments forwarded to the :py:class:`dict` constructor.
        :param kwargs: Keyword arguments forwarded to the :py:class:`dict` constructor.
        :return: The wrapped dictionary.
        """
        wrap: Callable[[Any], DotDict]
        wrap = lambda d: cls((k, wrap(v)) for k, v in d.items()) if isinstance(d, dict) else d
        return wrap(dict(*args, **kwargs))

    def __getattr__(self, attr: str) -> Any:
        try:
            return self[attr]
        except KeyError as e:
            raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{attr}'") from e

    def __setattr__(self, attr: str, value: Any) -> None:
        self[attr] = value

    def copy(self) -> DotDict:
        """
        Returns a deep copy of this dictionary.

        :return: The copy.
        """
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
        """
        Returns a deep copy of this dictionary.

        :return: The copy.
        """
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

        d.append("test")
        print(d)  # -> InsertableDict([('foo', 'new_value'), ('bar', 456), ('test', 999)])
    """

    def _insert(self, search_key: Hashable, key: Hashable | list | dict, value: Any, offset: int) -> None:
        # when key is a list or dict and value is no_value, assume key refers to key-value pairs
        if isinstance(key, (list, dict)) and value == no_value:
            new_items = list(key.items()) if isinstance(key, dict) else key
            new_keys = [k for k, v in new_items]
        else:
            new_items = [(key, value)]
            new_keys = [key]

        # if the search key is not present, insert the new pairs and finish
        if search_key == no_value or search_key not in self:
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

    def insert_before(self, before_key: Hashable, key: Hashable | list | dict, value: Any = None) -> None:
        """
        Inserts a *key* - *value* pair before the key *before_key*.

        :param before_key: The key before which the pair is inserted. If it does not exist, the new
            pair is added at the end.
        :param key: The key. When it is a list of item pairs or a dictionary, and *value* is
            :py:attr:`no_value`, multiple new values are inserted.
        :param value: The value.
        """
        self._insert(before_key, key, value, 0)

    def insert_after(self, after_key: Hashable, key: Hashable | list | dict, value: Any = None) -> None:
        """
        Inserts a *key* - *value* pair after the key *after_key*.

        :param after_key: The key after which the pair is inserted. If it does not exist, the new
            pair is added at the end.
        :param key: The key. When it is a list of item pairs or a dictionary, and *value* is
            :py:attr:`no_value`, multiple new values are inserted.
        :param value: The value.
        """
        self._insert(after_key, key, value, 1)

    def prepend(self, key: Hashable | list | dict, value: Any = no_value) -> None:
        """
        Adds a new *key* - *value* pair at the beginning of the dictionary.

        :param key: The key. When it is a list of item pairs or a dictionary, and *value* is
            :py:attr:`no_value`, multiple new values are prepended (in the given order).
        :param value: The value. When :py:attr:`no_value`, *key* is assumed to exist already in the
            dictionary and moved to the beginning.
        """
        first_key = next(iter(self)) if self else no_value
        if value is no_value and not isinstance(key, (list, dict)):
            value = self.get(key, None)
        self.insert_before(first_key, key, value=value)

    def append(self, key: Hashable | list | dict, value: Any = no_value) -> None:
        """
        Adds a new *key* - *value* pair at the end of the dictionary.

        :param key: The key. When it is a list of item pairs or a dictionary, and *value* is
            :py:attr:`no_value`, multiple new values are appended (in the given order).
        :param value: The value. When :py:attr:`no_value`, *key* is assumed to exist already in the
            dictionary and moved to the end.
        """
        last_key = list(self)[-1] if self else no_value
        if value is no_value and not isinstance(key, (list, dict)):
            value = self.get(key, None)
        self.insert_after(last_key, key, value=value)


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
    *value*.

    :param obj: The object to patch.
    :param attr: The name of the attribute.
    :param value: The temporary value.
    :param reset: Whether the original value is set again when the context is closed.
    :param orig: The original value. When not set, it is obtained through ``getattr``.
    :param lock: When *True*, the :py:attr:`default_lock` object is used to ensure the patch is
        thread-safe. When it is a lock instance, this object is used instead.
    :return: A context manager that yields *obj*.
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
            with contextlib.suppress(Exception):
                if reset:
                    if orig is no_value:
                        delattr(obj, attr)
                    else:
                        setattr(obj, attr, orig)


def join_generators(
    *generators: GeneratorType,
    on_error: Callable[[Exception | KeyboardInterrupt], Any] | None = None,
) -> Generator[Any, None, None]:
    """
    Joins multiple *generators* into a single generator for simplified iteration. Yielded objects
    are transparently sent back to ``yield`` assignments of the same generator.

    :param generators: The generators to join.
    :param on_error: When callable, it is invoked in case an exception is raised while iterating,
        including *KeyboardInterrupt*'s. If its return value evaluates to *True*, the state is reset
        and iterations continue. Otherwise, the exception is raised.
    :return: The joined generator.
    """
    for gen in generators:
        last_result: Any = no_value
        while True:
            try:
                last_result = yield (next(gen) if last_result == no_value else gen.send(last_result))
            except StopIteration:
                break
            except (Exception, KeyboardInterrupt) as e:
                last_result = no_value
                if not callable(on_error) or not on_error(e):
                    raise


def quote_cmd(cmd: str | Sequence[str | Sequence[str]]) -> str:
    """
    Takes a shell command *cmd* given as a list and returns a single string representation of that
    command with proper quoting. Example:

    .. code-block:: python

        print(quote_cmd(["bash", "-c", "echo", "foobar"]))
        # -> "bash -c echo foobar"

        print(quote_cmd(["bash", "-c", ["echo", "foobar"]]))
        # -> "bash -c 'echo foobar'"

    :param cmd: The command. Nested lists denote nested commands (such as shown above).
    :return: The quoted command string.
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
    Escapes all characters in a string *s* that could be confused for markdown formatting strings.

    :param s: The string.
    :return: The escaped string.
    """
    return re.sub(r"([^\\]?)(\(|\)|=|\.|_|-)", r"\1\\\2", s)


class ClassPropertyDescriptor:
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
    Property decorator for class-level methods.

    :param func: The method to decorate.
    :return: The property descriptor.
    """
    if not isinstance(func, (classmethod, staticmethod)):
        func = classmethod(func)  # type: ignore[assignment]

    return ClassPropertyDescriptor(func)


class BaseStream:

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
                consumer = open(consumer, mode)  # noqa: SIM115
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
        Writes to all registered consumer streams.

        :param args: Arguments forwarded to the ``write`` method of each stream.
        :param kwargs: Keyword arguments forwarded to the ``write`` method of each stream.
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
        Writes to the consumer stream when *filter_fn* evaluates to *True*.

        :param args: Arguments forwarded to the ``write`` method of the stream and to *filter_fn*.
        :param kwargs: Keyword arguments forwarded to the ``write`` method of the stream and to
            *filter_fn*.
        """
        if self.filter_fn(*args, **kwargs):
            self.stream.write(*args, **kwargs)
