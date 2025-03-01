# coding: utf-8

"""
HDF5 target formatters.
"""

__all__ = ["H5pyFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path
from law.util import no_value


class H5pyFormatter(Formatter):

    name = "h5py"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        import h5py
        return h5py.File(get_path(path), "r", *args, **kwargs)

    @classmethod
    def dump(cls, path, *args, **kwargs):
        import h5py

        perm = kwargs.pop("perm", no_value)

        ret = h5py.File(get_path(path), "w", *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
