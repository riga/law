# -*- coding: utf-8 -*-

"""
Custom base target definition.
"""


__all__ = ["Target"]


from abc import abstractmethod, abstractproperty

import luigi

from law.util import colored


class Target(luigi.target.Target):

    def __init__(self, *args, **kwargs):
        self.optional = kwargs.pop("optional", False)

        luigi.target.Target.__init__(self, *args, **kwargs)

    def __repr__(self):
        return self.colored_repr(color=False)

    def colored_repr(self, color=True):
        pairs = self._repr_pairs()
        flags = ", ".join(self._repr_flags(color=color))

        tmpl = "{}("
        tmpl += ", ".join(len(pairs) * ["{}"])
        if flags:
            tmpl += ", " + flags
        tmpl += ")"

        parts = [self._repr_class(color=color)]
        parts += [self._repr_pair(*tpl, color=color) for tpl in pairs]

        return tmpl.format(*parts)

    def _repr_pairs(self):
        return []

    def _repr_flags(self, color=True):
        return [self.optional_text(color=color) if self.optional else ""]

    @classmethod
    def _repr_class(cls, color=True):
        return colored(cls.__name__, "cyan") if color else cls.__name__

    @classmethod
    def _repr_pair(cls, key, value, color=True):
        return "{}={}".format(colored(key, color="blue", style="bright") if color else key, value)

    @classmethod
    def _repr_flag(cls, name, color=True):
        return colored(name, color="blue", style="bright") if color else name

    def status_text(self, max_depth=0, color=True):
        if self.exists():
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red" if not self.optional else "grey"

        return colored(text, _color, style="bright") if color else text

    def optional_text(self, color=True):
        text = "optional" if self.optional else "non-optional"
        return colored(text, color="blue", style="bright") if color else text

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def remove(self, silent=True):
        pass

    @abstractproperty
    def hash(self):
        pass
