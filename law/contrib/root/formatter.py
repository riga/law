# coding: utf-8

"""
ROOT target formatters.
"""


from law.target.formatter import Formatter
from law.target.file import get_path


guarded_tfile_cls = None


def GuardedTFile(*args, **kwargs):
    """
    Factory function that lazily creates the guarded TFile class, and creates and returns an
    instance with all passed *args* and *kwargs*. This is required as we do not want to import ROOT
    in the global scope.
    """
    global guarded_tfile_cls

    if not guarded_tfile_cls:
        import ROOT
        ROOT.gROOT.SetBatch()

        class GuardedTFile(ROOT.TFile):

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                if self.IsOpen():
                    self.Close()

        guarded_tfile_cls = GuardedTFile

    return guarded_tfile_cls(*args, **kwargs)


class ROOTFormatter(Formatter):

    name = "root"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        return GuardedTFile(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, *args, **kwargs):
        return GuardedTFile(get_path(path), *args, **kwargs)


class ROOTNumpyFormatter(Formatter):

    name = "root_numpy"

    @classmethod
    def accepts(cls, path):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import ROOT
        ROOT.gROOT.SetBatch()
        import root_numpy

        return root_numpy.root2array(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, arr, *args, **kwargs):
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
