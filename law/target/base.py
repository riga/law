# coding: utf-8

"""
Custom base target definition.
"""


__all__ = ["Target"]


import logging
from abc import abstractmethod, abstractproperty

import luigi

from law.config import Config
from law.util import colored


logger = logging.getLogger(__name__)


class Target(luigi.target.Target):

    def __init__(self, *args, **kwargs):
        self.optional = kwargs.pop("optional", False)

        luigi.target.Target.__init__(self, *args, **kwargs)

    def __repr__(self):
        return self.repr(color=False)

    def repr(self, color=None):
        if color is None:
            cfg = Config.instance()
            color = cfg.get_expanded_boolean("target", "colored_repr")

        class_name = self._repr_class_name(self.__class__.__name__, color=color)

        parts = [self._repr_pair(*pair, color=color) for pair in self._repr_pairs()]
        parts += [self._repr_flag(flag, color=color) for flag in self._repr_flags()]

        return "{}({})".format(class_name, ", ".join(parts))

    def colored_repr(self):
        # deprecation warning until v0.1
        logger.warning("the use of {0}.colored_repr() is deprecated, please use "
            "{0}.repr(color=True) instead".format(self.__class__.__name__))

        return self.repr(color=True)

    def _repr_pairs(self):
        return []

    def _repr_flags(self):
        flags = []
        if self.optional:
            flags.append(self.optional_text())
        return flags

    @classmethod
    def _repr_class_name(cls, name, color=False):
        return colored(name, "cyan") if color else name

    @classmethod
    def _repr_pair(cls, key, value, color=False):
        return "{}={}".format(colored(key, color="blue", style="bright") if color else key, value)

    @classmethod
    def _repr_flag(cls, name, color=False):
        return colored(name, color="magenta") if color else name

    def _copy_kwargs(self):
        return {"optional": self.optional}

    def status_text(self, max_depth=0, flags=None, color=False, exists=None):
        if exists is None:
            exists = self.exists()

        if exists:
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

    @abstractmethod
    def uri(self, *args, **kwargs):
        return
