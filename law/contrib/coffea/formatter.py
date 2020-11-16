# coding: utf-8

"""
Coffea target formatters.
"""


__all__ = ["CoffeaFormatter"]


import logging

from law.target.formatter import Formatter
from law.target.file import get_path


logger = logging.getLogger(__name__)


class CoffeaFormatter(Formatter):

    name = "coffea"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".coffea")

    @classmethod
    def load(cls, path, *args, **kwargs):
        from coffea.util import load

        path = get_path(path)
        return load(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, out, *args, **kwargs):
        from coffea.util import save

        path = get_path(path)
        save(out, path, *args, **kwargs)
