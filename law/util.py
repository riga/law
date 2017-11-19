# -*- coding: utf-8 -*-

"""
Helpful utility functions.
"""


__all__ = ["law_base", "printerr", "abort", "colored", "query_choice", "multi_match", "make_list",
           "flatten", "which"]


import os
import sys
import types
import re
import fnmatch

import six


def law_base(*paths):
    """
    Returns the law installation directory, optionally joined with *paths*.
    """
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, *paths)


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
    "red"   : 31,
    "green" : 32,
    "yellow": 33,
    "blue"  : 34,
    "pink"  : 35,
    "cyan"  : 36,
    "white" : 37
}

backgrounds = {
    "none"  : 49,
    "red"   : 41,
    "green" : 42,
    "yellow": 43,
    "blue"  : 44,
    "pink"  : 45,
    "cyan"  : 46,
    "grey"  : 47
}

styles = {
    "none"     : 0,
    "bright"   : 1,
    "underline": 4
}

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
        return mode(re.match(pattern, name) for pattern in patternss)


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
    dirname, basename = os.path.split(prog)
    if dirname:
        if executable(prog):
            return prog
    elif "PATH" in os.environ:
        for search_path in os.environ["PATH"].split(os.pathsep):
            path = os.path.join(search_path.strip('"'), prog)
            if executable(path):
                return path

    return None
