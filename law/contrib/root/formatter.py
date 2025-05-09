# coding: utf-8

"""
ROOT target formatters.
"""

__all__ = [
    "GuardedTFile", "ROOTFormatter", "ROOTNumpyFormatter", "ROOTPandasFormatter", "UprootFormatter",
]


from contextlib import contextmanager

import six

from law.target.formatter import Formatter
from law.target.file import get_path
from law.util import no_value

from law.contrib.root.util import import_ROOT


class GuardedTFile(object):

    @classmethod
    def Open(cls, *args, **kwargs):
        ROOT = import_ROOT()
        return cls(ROOT.TFile.Open(*args, **kwargs))

    def __init__(self, *args, **kwargs):
        super(GuardedTFile, self).__init__()

        self._guarded_tfile = None

        ROOT = import_ROOT()
        if len(args) == 1 and isinstance(args[0], ROOT.TFile) and not kwargs:
            self._guarded_tfile = args[0]
        elif args or kwargs:
            self._guarded_tfile = ROOT.TFile.Open(*args, **kwargs)

    def __enter__(self):
        return self._guarded_tfile

    def __exit__(self, exc_type, exc_value, traceback):
        if self.IsOpen():
            self.Close()

    def __getattr__(self, attr):
        if self._guarded_tfile is not None:
            return getattr(self._guarded_tfile, attr)
        else:
            raise AttributeError("cannot forward attribute '{}' to undefined guarded tfile".format(
                attr))

    def __setattr__(self, attr, value):
        if attr != "_guarded_tfile":
            setattr(self._guarded_tfile, attr, value)
        else:
            super(GuardedTFile, self).__setattr__(attr, value)


class ROOTFormatter(Formatter):

    name = "root"

    @classmethod
    def accepts(cls, path, mode):
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
    def accepts(cls, path, mode):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        ROOT = import_ROOT()  # noqa: F841
        import root_numpy

        return root_numpy.root2array(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, arr, *args, **kwargs):
        ROOT = import_ROOT()  # noqa: F841
        import root_numpy

        perm = kwargs.pop("perm", None)

        ret = root_numpy.array2root(arr, get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class ROOTPandasFormatter(Formatter):

    name = "root_pandas"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        ROOT = import_ROOT()  # noqa: F841
        import root_pandas

        return root_pandas.read_root(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path, df, *args, **kwargs):
        ROOT = import_ROOT()  # noqa: F841
        # importing root_pandas adds the to_root() method to data frames
        import root_pandas  # noqa: F401

        perm = kwargs.pop("perm", None)

        ret = df.to_root(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class UprootFormatter(Formatter):

    name = "uproot"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import uproot

        return uproot.open(get_path(path), *args, **kwargs)

    @classmethod
    @contextmanager
    def dump(cls, path, mode="recreate", **kwargs):
        import uproot

        # check the mode and get the saving function
        allowed_modes = ["create", "recreate", "update"]
        if not isinstance(mode, six.string_types) or mode.lower() not in allowed_modes:
            raise ValueError("unknown uproot writing mode: {}".format(mode))
        fn = getattr(uproot, mode.lower())

        perm = kwargs.pop("perm", None)

        # create the file object and yield it
        f = fn(get_path(path), **kwargs)
        try:
            yield f
        finally:
            try:
                f.file.close()
                if perm is not None:
                    cls.chmod(path, perm)
            except:
                pass
