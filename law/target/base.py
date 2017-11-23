# -*- coding: utf-8 -*-

"""
Custom base target definition.
"""


__all__ = ["Target"]


import os
from abc import abstractmethod, abstractproperty

import luigi

from law.util import colored


class Target(luigi.target.Target):

    def __repr__(self):
        return "<{} at {}>".format(self.__class__.__name__, hex(id(self)))

    def colored_repr(self):
        return "{}({})".format(colored(self.__class__.__name__, "cyan"), hex(id(self)))

    def status_text(self, max_depth=0, color=True):
        if self.exists():
            text = "existent"
            _color = "green"
        else:
            text = "absent"
            _color = "red"

        return colored(text, _color, style="bright") if color else text

    @abstractmethod
    def exists(self):
        pass

    @abstractmethod
    def remove(self, silent=True):
        pass
