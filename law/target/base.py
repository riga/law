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

    def colored_repr(self):
        tpl = (colored(self.__class__.__name__, "cyan"), hex(id(self)))
        return "%s(%s)" % tpl

    def status_text(self, max_depth=0, ignore_custom=True, **kwargs):
        """ status_text(max_depth=0, ignore_custom=True, colored=True)
        """
        _colored = kwargs.get("colored", True)

        if self.exists(ignore_custom=ignore_custom):
            text = "existent"
            if _colored:
                text = colored(text, "green", style="bright")
            return text
        else:
            text = "absent"
            if _colored:
                text = colored(text, "red", style="bright")
            return text

    @abstractmethod
    def exists(self, ignore_custom=False):
        pass

    @abstractmethod
    def remove(self, silent=True):
        pass
