# -*- coding: utf-8 -*-

"""
Helpful utility functions.
"""


__all__ = ["rel_path", "law_base", "printerr", "abort", "colored", "uncolored", "query_choice",
           "multi_match", "make_list", "flatten", "which", "map_struct", "mask_struct", "tmp_file",
           "interruptable_popen", "create_hash", "copy_no_perm", "iter_chunks"]


import os
import sys
import types
import re
import fnmatch
import tempfile
import subprocess
import signal
import hashlib
import shutil
from contextlib import contextmanager

import six


class NoValue(object):

    def __nonzero__(self):
        return False


_no_value = NoValue()


def rel_path(anchor, *paths):
    """
    Returns a path made of framgment *paths* relativ to an *anchor* path. When *anchor* is a file,
    its absolute directory is used instead.
    """
    anchor = os.path.expandvars(os.path.expanduser(os.path.abspath(anchor)))
    if os.path.exists(anchor) and os.path.isfile(anchor):
        anchor = os.path.dirname(anchor)
    return os.path.normpath(os.path.join(anchor, *paths))


def law_base(*paths):
    """
    Returns the law installation directory, optionally joined with *paths*.
    """
    return rel_path(__file__, *paths)


def printerr(*args, **kwargs):
    """ printerr(*args, flush=False)
    Same as *print*, but outputs to stderr. If *flush* is *True*, stderr is flushed after printing.
    """
    sys.stderr.write(str(args) + "\n")
    if kwargs.get("flush", False):
        sys.stderr.flush()


def abort(msg=None, exitcode=1):
    """
    Aborts the process (*sys.exit*) with an *exitcode*. If *msg* is not *None*, it is printed first
    to stdout if *exitcode* is 0 or *None*, and to stderr otherwise.
    """
    if msg is not None:
        if exitcode in (None, 0):
            print(msg)
        else:
            printerr(msg)
    sys.exit(exitcode)


colors = {
    "red": 31,
    "green": 32,
    "yellow": 33,
    "blue": 34,
    "pink": 35,
    "cyan": 36,
    "white": 37,
}

backgrounds = {
    "none": 49,
    "red": 41,
    "green": 42,
    "yellow": 43,
    "blue": 44,
    "pink": 45,
    "cyan": 46,
    "grey": 47,
}

styles = {
    "none": 0,
    "bright": 1,
    "underline": 4,
}

uncolor_cre = re.compile(r"\x1B\[[0-?]*[ -/]*[@-~]")


def colored(msg, color=None, background=None, style=None, force=False):
    """
    Return the colored version of a string *msg*. *color*'s: red, green, yellow, blue, pink,
    cyan, white. *background*'s: none, red, green, yellow, blue, pink, cyan, grey. *style*'s: none,
    bright, underline. Unless *force* is *True*, the *msg* string is returned unchanged in case the
    output is not a tty.
    """
    try:
        if not force and not os.isatty(sys.stdout.fileno()):
            return msg
    except:
        return msg

    color = colors.get(color, colors["white"])
    background = backgrounds.get(background, backgrounds["none"])

    if not isinstance(style, (tuple, list, set)):
        style = (style,)
    style = ";".join(str(styles.get(s, styles["none"])) for s in style)

    return "\033[{};{};{}m{}\033[0m".format(style, background, color, msg)


def uncolored(msg):
    return uncolor_cre.sub("", msg)


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


def multi_match(name, patterns, mode=any, regex=False):
    """
    Compares *name* to multiple *patterns* and returns *True* in case of at least one match (*mode*
    = *any*, the default), or in case all patterns matched (*mode* = *all*). Otherwise, *False* is
    returned. When *regex* is *True*, *re.match* is used instead of *fnmatch.fnmatch*.
    """
    if not regex:
        return mode(fnmatch.fnmatch(name, pattern) for pattern in patterns)
    else:
        return mode(re.match(pattern, name) for pattern in patterns)


def make_list(obj, cast=True):
    """
    Converts an object *obj* to a list and returns it. Objects of types *tuple* and *set* are
    converted if *cast* is *True*. Otherwise, and for all other types, *obj* is put in a new list.
    """
    if isinstance(obj, list):
        return list(obj)
    if isinstance(obj, types.GeneratorType):
        return list(obj)
    if isinstance(obj, (tuple, set)) and cast:
        return list(obj)
    else:
        return [obj]


def flatten(struct):
    """
    Flattens and returns a complex structured object *struct*.
    """
    if isinstance(struct, types.GeneratorType):
        return flatten(list(struct))
    elif isinstance(struct, dict):
        return flatten(struct.values())
    elif isinstance(struct, (list, tuple, set)):
        objs = []
        for obj in struct:
            objs.extend(flatten(obj))
        return objs
    else:
        return [struct]


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


def map_struct(func, struct, cls=None, map_dict=True, map_list=True, map_tuple=False,
        map_set=False):
    """
    Applies a function *func* to each value of a complex structured object *struct* and returns the
    output in the same structure. Example:

    .. code-block:: python

       struct = {"foo": [123, 456], "bar": [{"1": 1}, {"2": 2}]}
       def times_two(i):
           return i * 2

       map_struct(struct, times_two)
       # -> {"foo": [246, 912], "bar": [{"1": 2}, {"2": 4}]}

    When *cls* is not *None*, it exclusively defines the class of objects that *func* is applied on.
    All other objects are unchanged. *map_dict*, *map_list*, *map_tuple* and *map_set* configure if
    objects of the respective types are traversed or mapped. The can be booleans or integer values
    that define the depth of that setting in the struct.
    """
    types = tuple()
    if map_dict:
        types = types + (dict,)
        if isinstance(map_dict, int) and not isinstance(map_dict, bool):
            map_dict -= 1
    if map_list:
        types = types + (list,)
        if isinstance(map_list, int) and not isinstance(map_list, bool):
            map_list -= 1
    if map_tuple:
        types = types + (tuple,)
        if isinstance(map_tuple, int) and not isinstance(map_tuple, bool):
            map_tuple -= 1
    if map_set:
        types = types + (set,)
        if isinstance(map_set, int) and not isinstance(map_set, bool):
            map_set -= 1

    # is an instance of cls?
    if cls is not None and isinstance(struct, cls):
        return func(struct)

    # traverse?
    elif isinstance(struct, types):
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
            value = map_struct(func, value, cls=cls, map_dict=map_dict, map_list=map_list,
                    map_tuple=map_tuple, map_set=map_set)
            add(key, value)

        # convert tuples
        if isinstance(struct, tuple):
            new_struct = struct.__class__(new_struct)

        return new_struct

    # when cls is set, just return
    elif cls is not None:
        return struct

    # apply func
    else:
        return func(struct)


def mask_struct(mask, struct, replace=_no_value):
    # interpret genrators as lists
    if isinstance(struct, types.GeneratorType):
        return mask_struct(mask, list(struct), replace=replace)

    # when mask is a bool, or struct is not a dict or sequence, apply the mask immediately
    elif isinstance(mask, bool) or not isinstance(struct, (list, tuple, dict)):
        return struct if mask else replace

    # check list and tuple types
    elif isinstance(struct, (list, tuple)) and isinstance(mask, (list, tuple)):
        new_struct = []
        for i, val in enumerate(struct):
            if i >= len(mask):
                new_struct.append(val)
            else:
                repl = _no_value
                if isinstance(replace, (list, tuple)) and len(replace) > i:
                    repl = replace[i]
                val = mask_struct(mask[i], val, replace=repl)
                if val != _no_value:
                    new_struct.append(val)

        return struct.__class__(new_struct) if new_struct else replace

    # check dict types
    elif isinstance(struct, dict) and isinstance(mask, dict):
        new_struct = struct.__class__()
        for key, val in six.iteritems(struct):
            if key not in mask:
                new_struct[key] = val
            else:
                repl = _no_value
                if isinstance(replace, dict) and key in replace:
                    repl = replace[key]
                val = mask_struct(mask[key], val, replace=repl)
                if val != _no_value:
                    new_struct[key] = val
        return new_struct or replace

    # when this point is reached, mask and struct have incompatible types
    raise TypeError("mask and struct must have the same type, got '{}' and '{}'".format(type(mask),
            type(struct)))


@contextmanager
def tmp_file(*args, **kwargs):
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
    kwargs["preexec_fn"] = os.setsid
    p = subprocess.Popen(*args, **kwargs)

    try:
        out, err = p.communicate()
    except KeyboardInterrupt:
        os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        raise

    return p.returncode, out, err


def create_hash(inp, l=10, algo="sha256"):
    """
    Takes an input *inp* and creates a hash based on an algorithm *algo*. For valid algorithms, see
    python's hashlib. *l* corresponds to the maximum length of the returned hash. Internally, the
    string representation of *inp* is used.
    """
    return getattr(hashlib, algo)(str(inp)).hexdigest()[:l]


def copy_no_perm(src, dst):
    """
    Copies a file from *src* to *dst* including meta data except for permission bits.
    """
    shutil.copy(src, dst)
    perm = os.stat(dst).st_mode
    shutil.copystat(src, dst)
    os.chmod(dst, perm)


def iter_chunks(l, size):
    """
    Returns a generator containing chunks of *size* of a list, integer or generator *l*. A *size*
    smaller than 1 results in no chunking at all.
    """
    if isinstance(l, six.integer_types):
        l = six.moves.range(l)

    if isinstance(l, types.GeneratorType):
        if size < 1:
            yield list(l)
        else:
            chunk = []
            for elem in l:
                if len(chunk) < size:
                    chunk.append(elem)
                else:
                    yield chunk
                    chunk = []
            if chunk:
                yield chunk

    else:
        if size < 1:
            yield l
        else:
            for i in six.moves.range(0, len(l), size):
                yield l[i:i + size]
