# -*- coding: utf-8 -*-

"""
Custom luigi base target.
"""


__all__ = ["Target"]


import os
from abc import abstractmethod, abstractproperty

import luigi

from law.util import colored


class Target(luigi.target.Target):

    def __init__(self, exists=None):
        self.custom_exists = exists

        luigi.target.Target.__init__(self)

    def __repr__(self):
        tpl = (self.__class__.__name__, hex(id(self)))
        return "<%s at %s>" % tpl

    @property
    def color_repr(self):
        tpl = (colored(self.__class__.__name__, "cyan"), hex(id(self)))
        return "%s(%s)" % tpl

    def status_text(self, _colored=True, **kwargs):
        if self.exists():
            if _colored:
                return colored("existent", "green", style="bright")
            else:
                return "existent"
        else:
            if _colored:
                return colored("absent", "red", style="bright")
            else:
                return "absent"

    @abstractmethod
    def exists(self, ignore_custom=False):
        pass

    @abstractmethod
    def remove(self, silent=True):
        pass
