# -*- coding: utf-8 -*-

"""
Custom base target definition.
"""


__all__ = ["Target", "split_transfer_kwargs"]


from abc import abstractmethod, abstractproperty

import luigi

from law.util import colored, make_list


class Target(luigi.target.Target):

    def __init__(self, *args, **kwargs):
        self.optional = kwargs.pop("optional", False)

        luigi.target.Target.__init__(self, *args, **kwargs)

    def __repr__(self):
        return self.colored_repr(color=False)

    def colored_repr(self, color=True):
        class_name = self._repr_class_name(self.__class__.__name__, color=color)

        parts = [self._repr_pair(*pair, color=color) for pair in self._repr_pairs(color=color)]
        parts += [self._repr_flag(flag, color=color) for flag in self._repr_flags(color=color)]

        return "{}({})".format(class_name, ", ".join(parts))

    def _repr_pairs(self, color=True):
        return []

    def _repr_flags(self, color=True):
        flags = []
        if self.optional:
            flags.append(self.optional_text())
        return flags

    @classmethod
    def _repr_class_name(cls, name, color=True):
        return colored(name, "cyan") if color else name

    @classmethod
    def _repr_pair(cls, key, value, color=True):
        return "{}={}".format(colored(key, color="blue", style="bright") if color else key, value)

    @classmethod
    def _repr_flag(cls, name, color=True):
        return colored(name, color="magenta") if color else name

    def status_text(self, max_depth=0, flags=None, color=True):
        if self.exists():
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red" if not self.optional else "grey"

        return colored(text, _color, style="bright") if color else text

    def optional_text(self):
        return "optional" if self.optional else "non-optional"

    @abstractmethod
    def exists(self):
        return

    @abstractmethod
    def remove(self, silent=True):
        return

    @abstractproperty
    def hash(self):
        return


def split_transfer_kwargs(kwargs, skip=None):
    """
    Takes keyword arguments *kwargs*, splits them into two separate dictionaries depending on their
    content, and returns them in a tuple. The first one will contain arguments related to file
    transfer operations (e.g. ``"cache"`` or ``"retries"``), while the second one will contain all
    remaining arguments. This function is used internally to decide which arguments to pass to
    target formatters. *skip* can be a list of argument keys that are ignored.
    """
    skip = make_list(skip) if skip else []
    transfer_kwargs = {}
    if "cache" not in skip:
        transfer_kwargs["cache"] = kwargs.pop("cache", True)
    if "retries" not in skip:
        transfer_kwargs["retries"] = kwargs.pop("retries", None)
    if "retry_delay" not in skip:
        transfer_kwargs["retry_delay"] = kwargs.pop("retry_delay", None)
    return transfer_kwargs, kwargs
