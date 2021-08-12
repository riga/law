# coding: utf-8

"""
Custom base target definition.
"""

__all__ = ["Target"]


from abc import abstractmethod

import luigi

from law.config import Config
from law.util import colored, create_hash
from law.logger import get_logger


logger = get_logger(__name__)


class Target(luigi.target.Target):

    def __init__(self, *args, **kwargs):
        self.optional = kwargs.pop("optional", False)
        self.external = kwargs.pop("external", False)

        luigi.target.Target.__init__(self, *args, **kwargs)

    def __repr__(self):
        color = Config.instance().get_expanded_boolean("target", "colored_repr")
        return self.repr(color=color)

    def __str__(self):
        color = Config.instance().get_expanded_boolean("target", "colored_str")
        return self.repr(color=color)

    def __hash__(self):
        return self.hash

    @property
    def hash(self):
        return create_hash(self.uri(), to_int=True)

    def repr(self, color=None):
        if color is None:
            color = Config.instance().get_expanded_boolean("target", "colored_repr")

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
            flags.append("optional")
        if self.external:
            flags.append("external")
        return flags

    def _repr_class_name(self, name, color=False):
        return colored(name, "cyan") if color else name

    def _repr_pair(self, key, value, color=False):
        return "{}={}".format(colored(key, color="blue", style="bright") if color else key, value)

    def _repr_flag(self, name, color=False):
        return colored(name, color="magenta") if color else name

    def _copy_kwargs(self):
        return {"optional": self.optional, "external": self.external}

    def status_text(self, max_depth=0, flags=None, color=False, exists=None):
        if exists is None:
            exists = self.exists()

        if exists:
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "grey" if self.optional else "red"

        return colored(text, _color, style="bright") if color else text

    @abstractmethod
    def exists(self):
        return

    @abstractmethod
    def remove(self, silent=True):
        return

    @abstractmethod
    def uri(self, return_all=False):
        return
