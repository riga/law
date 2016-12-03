# -*- coding: utf-8 -*-

"""
Helpful utility functions.
"""

__all__ = ["abort", "colored"]


import os
import sys


def abort(msg=None, exitCode=1):
    """
    Aborts the process (*sys.exit*) with an *exitCode*. If *msg* is not *None*, it is printed first.
    """
    if msg is not None:
        print(msg)
    print(colored("abort", "red"))
    sys.exit(exitCode)


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
    "none"  : 40,
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

    return "\033[%s;%s;%sm%s\033[0m" % (style, background, color, msg)
