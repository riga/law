# coding: utf-8

"""
Helpful utility functions.
"""


__all__ = [
    "default_lock", "io_lock", "console_lock", "no_value", "rel_path", "law_src_path",
    "law_home_path", "law_run", "print_err", "abort", "is_number", "try_int", "str_to_int",
    "flag_to_bool", "common_task_params", "colored", "uncolored", "query_choice", "is_pattern",
    "brace_expand", "multi_match", "is_iterable", "is_lazy_iterable", "make_list", "make_tuple",
    "make_unique", "flatten", "merge_dicts", "which", "map_verbose", "map_struct", "mask_struct",
    "tmp_file", "interruptable_popen", "readable_popen", "create_hash", "copy_no_perm",
    "makedirs_perm", "user_owns_file", "iter_chunks", "human_bytes", "parse_bytes",
    "human_duration", "human_time_diff", "parse_duration", "is_file_exists_error", "send_mail",
    "ShorthandDict", "open_compat", "patch_object", "join_generators", "quote_cmd", "BaseStream",
    "TeeStream", "FilteredStream",
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

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False


#: Unique dummy value that evaluates to *False*.
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


def law_run(argv):
    """
    Runs a task with certain parameters as defined in *argv*, which can be a string or a list of
    strings. It must start with the family of the task to run, followed by the desired parameters.
    Example:

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

    # luigis pid locking must be disabled
    argv.append("--no-lock")

    return luigi_run(argv)


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
    ``"yes"``, ``"on"``, case-insensitive) or *False* (values ``"0"``, ``"false"``, `"no"``,
    ``"off"``, case-insensitive). When *s* is already a boolean, it is returned unchanged. An error
    is thrown when *s* is neither of the allowed values and *silent* is *False*. Otherwise, *None*
    is returned.
    """
    if isinstance(s, bool):
        return s
    elif isinstance(s, six.string_types):
        if s.lower() in ("true", "1", "yes", "on"):
            return True
        elif s.lower() in ("false", "0", "no", "off"):
            return False
    elif silent:
        return None
    else:
        raise ValueError("cannot convert to bool: {}".format(s))


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
            print("invalid choice: '{}'\n".format(choice))
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


def brace_expand(s, split_csv=False):
    """
    Expands brace statements in a string *s* and returns a list containing all possible string
    combinations. When *split_csv* is *True*, the input string is split by all ``","`` characters
    located outside braces and the expansion is performed sequentially on all elements. Example:

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
    br_open = "__LAW_BRACE_OPEN__"
    br_close = "__LAW_BRACE_CLOSE__"
    s = s.replace(r"\{", br_open).replace(r"\}", br_close)

    # compile the expression that finds brace statements
    cre = re.compile(r"\{[^\{]*\}")

    # take into account csv splitting
    if split_csv:
        # replace commas in brace statements to avoid splitting
        br_comma = "__LAW_BRACE_COMMA__"
        _s = cre.sub(lambda m: m.group(0).replace(",", br_comma), s)
        # split by real csv commas and start recursion when a comma was found, otherwise continue
        parts = _s.split(",")
        if len(parts) > 1:
            # replace commas in braces again and recurse
            parts = [part.replace(br_comma, ",") for part in parts]
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


def multi_match(name, patterns, mode=any, regex=False):
    """
    Compares *name* to multiple *patterns* and returns *True* in case of at least one match (*mode*
    = *any*, the default), or in case all patterns matched (*mode* = *all*). Otherwise, *False* is
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


def is_lazy_iterable(obj):
    """
    Returns whether *obj* is iterable lazily, such as generators, range objects, maps, etc.
    """
    iter_types = (
        types.GeneratorType, collections.MappingView, six.moves.range, six.moves.map, enumerate,
    )
    return isinstance(obj, iter_types)


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
        if is_iterable(obj):
            obj = list(obj)
        else:
            raise TypeError("object is neither list, tuple, nor generic iterable")

    ret = sorted(obj.__class__(set(obj)), key=lambda elem: obj.index(elem))

    return obj.__class__(ret) if isinstance(obj, tuple) else ret


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
    """ merge_dicts(*dicts, cls=None)
    Takes multiple *dicts* and returns a single merged dict. The merging takes place in order of the
    passed dicts and therefore, values of rear objects have precedence in case of field collisions.
    The class of the returned merged dict is configurable via *cls*. If it is *None*, the class is
    inferred from the first dict object in *dicts*.
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
    merged_dict = cls()
    for d in dicts:
        if isinstance(d, dict):
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

        # omitting mask information results in keeping values
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
    Shorthand to :py:class:`Popen` which yields the output live line-by-line. All *args* and
    *kwargs* are forwatded to the :py:class:`Popen` constructor. When EOF is reached,
    ``communicate()`` is called on the subprocess and it is yielded. Example:

    .. code-block:: python

        for line in readable_popen(["some_executable", "--args"]):
            if isinstance(line, str):
                print(line)
            else:
                process = line
                if process.returncode != 0:
                    raise Exception("complain ...")
    """
    # force pipes
    kwargs["stdout"] = subprocess.PIPE
    kwargs["stderr"] = subprocess.STDOUT

    p = subprocess.Popen(*args, **kwargs)

    for line in iter(lambda: p.stdout.readline(), ""):
        if six.PY3:
            line = line.decode("utf-8")
        yield line.rstrip()

    # yield the process itself in the end
    p.communicate()
    yield p


def create_hash(inp, l=10, algo="sha256"):
    """
    Takes an input *inp* and creates a hash based on an algorithm *algo*. For valid algorithms, see
    python's hashlib. *l* corresponds to the maximum length of the returned hash. Internally, the
    string representation of *inp* is used.
    """
    return getattr(hashlib, algo)(six.b(str(inp))).hexdigest()[:l]


def copy_no_perm(src, dst):
    """
    Copies a file from *src* to *dst* including meta data except for permission bits.
    """
    shutil.copyfile(src, dst)
    perm = os.stat(dst).st_mode
    shutil.copystat(src, dst)
    os.chmod(dst, perm)


def makedirs_perm(path, perm=None):
    """
    Recursively creates directory up to *path*. If *perm* is set, the permissions of all newly
    created directories are set to its value.
    """
    if not os.path.exists(path):
        if perm is None:
            os.makedirs(path)
        else:
            umask = os.umask(0)
            try:
                os.makedirs(path, perm)
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
    ("day", 86400),
    ("hour", 3600),
    ("minute", 60),
    ("second", 1),
])

time_unit_aliases = {
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
    return value has the format ``"[d:][hh:]mm:ss[.ms]"``. *colon_format* can also be a string value
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
    # -> "1:01:00:01"

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
    seconds = float(datetime.timedelta(**kwargs).total_seconds())
    sign = 1 if seconds >= 0 else -1
    seconds = abs(seconds)

    # when using colon_format, check if a limiting unit is set
    colon_unit_limit = None
    if isinstance(colon_format, six.string_types):
        colon_unit_limit = time_unit_aliases.get(colon_format, colon_format)
        if colon_unit_limit not in time_units:
            raise ValueError("unknown colon_format unit '{}', valid values are {}".format(
                colon_unit_limit, ",".join(time_units)))
        colon_unit_index = list(time_units.keys()).index(colon_unit_limit)

    parts = []
    for i, (unit, mul) in enumerate(six.iteritems(time_units)):
        # skip this iteration when a colon unit limit is set
        if colon_unit_limit and i < colon_unit_index:
            continue

        # build the value for this unit
        if unit == "second":
            # try to round to 2 digits or convert to int
            value = try_int(round(seconds, 2))
        else:
            # get the integer divider and adjust the remaining number of seconds
            value = int(seconds // mul)
            seconds -= value * mul

        # skip zeros under certain conditions
        if not value:
            leading = not parts
            if colon_format:
                # skip zeros when leading but not referring to minutes or seconds
                if leading and unit not in ("minute", "second"):
                    continue
            else:
                # skip zeros always, except when leading and the unit is seconds
                if not leading or unit != "second":
                    continue

        # build the human readable representation
        if colon_format:
            if unit == "second" and value < 10:
                # special case for seconds to format floating points properly
                fmt = "0{}"
            elif unit in ["hour", "minute"]:
                fmt = "{:02d}"
            else:
                fmt = "{}"
            parts.append(fmt.format(value))
        else:
            plural_postfix = "" if (not plural or value == 1) else "s"
            parts.append("{} {}{}".format(value, unit, plural_postfix))

    # special case: the minute field is mandatory for colon_format in any case
    if colon_format and len(parts) == 1:
        parts.insert(0, "00")

    # denote negative values
    sign_prefix = ""
    if sign == -1:
        sign_prefix = "-" if colon_format else "minus "

    return sign_prefix + (":" if colon_format else ", ").join(parts)


def human_time_diff(*args, **kwargs):
    """
    Deprecated. Use :py:func:`human_duration` instead.
    """
    # deprecation warning until v0.1
    logger.warning("law.util.human_time_diff is deprecated, use law.util.human_duration instead")

    return human_duration(*args, **kwargs)


def parse_duration(s, input_unit="s", unit="s"):
    """
    Takes a string *s*, interprets it as a duration with an optional unit, and returns a float that
    represents that size in a given *unit*. When no unit is found in *s*, *input_unit* is used as a
    default. A *ValueError* is raised, when *s* cannot be successfully converted. Multiple input
    formats are parsed: Example:

    .. code-block:: python

        # plain number
        parse_duration(100)
        # -> 100.

        parse_duration(100, unit="min")
        # -> 1.667

        parse_duration(100, input_unit="min")
        # -> 6000.

        parse_duration(-100, input_unit="min")
        # -> -6000.

        # string separated with ":", interpreted from the back as seconds, minutes, etc.
        # input_unit is disregarded, unit works as above
        parse_duration("2:1")
        # -> 121.

        parse_duration("04:02:01.1")
        # -> 14521.1

        parse_duration("04:02:01.1", unit="min")
        # -> 242.0183

        # human-readable string, optionally multiple of them
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
    duration_seconds = 0.

    # number or string?
    if isinstance(s, six.integer_types + (float,)):
        duration_seconds += s * time_units[input_unit]
    else:
        s = s.strip()

        # check the format
        if "," not in s and ":" in s:
            # colon format, "[d:][h:][m:]s"
            unit_order = ["second", "minute", "hour", "day"]

            # interpret leading "-" or "+" as the sign of the duration
            if s[0] in "+-":
                sign = 1 if s[0] == "+" else -1
                s = s[1:]

            # split and check the number of parts
            parts = s.split(":")
            if len(parts) > len(unit_order):
                raise ValueError("cannot parse duration string '{}', too many ':'".format(s))

            # convert each part, starting from the back to match unit_order
            for i, part in enumerate(parts[::-1]):
                u = unit_order[i]
                try:
                    d = float(part.strip())
                except ValueError as e:
                    raise ValueError("cannot parse duration string '{}', {}".format(s, e))

                duration_seconds += d * time_units[u]

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


class BaseStream(object):

    FLUSH_AFTER_WRITE = True

    def __init__(self):
        super(BaseStream, self).__init__()
        self.closed = False

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
            if self.FLUSH_AFTER_WRITE:
                self.flush()

    def _close(self):
        return

    def _flush(self):
        return

    def _write(self, *args, **kwargs):
        return


class TeeStream(BaseStream):
    """
    Multi-stream object that forwards calls to :py:meth:`write` and :py:meth:`flush` to all
    registered *consumer* streams. When a *consumer* is a string, it is interpreted as a file which
    is opened for writing (similar to *tee* in bash). Example:

    .. code-block:: python

        tee = TeeStream("/path/to/log.txt", sys.__stdout__)
        sys.stdout = tee
    """

    def __init__(self, *consumers, **kwargs):
        """ __init__(*consumers, mode="w")
        """
        super(TeeStream, self).__init__()

        mode = kwargs.get("mode", "w")

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
    *True*.
    """

    def __init__(self, stream, filter_fn):
        super(FilteredStream, self).__init__()
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
