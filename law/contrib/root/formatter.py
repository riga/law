# -*- coding: utf-8 -*-

"""
ROOT target formatters.
"""


from contextlib import contextmanager

from law.target.formatter import Formatter
from law.target.file import get_path


class ROOTFormatter(Formatter):

    name = "root"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    @contextmanager
    def load(cls, path, *args, **kwargs):
        import ROOT

        tfile = ROOT.TFile.Open(get_path(path), *args, **kwargs)
        try:
            yield tfile
        finally:
            if tfile.IsOpen():
                tfile.Close()


class ROOTNumpyFormatter(Formatter):

    name = "root_numpy"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import root_numpy

        return root_numpy.root2array(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, arr, path, *args, **kwargs):
        import root_numpy

        return root_numpy.array2root(arr, get_path(path), *args, **kwargs)


class UprootFormatter(Formatter):

    name = "uproot"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import uproot

        return uproot.open(get_path(path), *args, **kwargs)
