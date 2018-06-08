# -*- coding: utf-8 -*-

"""
HDF5 target formatters.
"""


from law.target.formatter import Formatter
from law.target.file import get_path


class H5pyFormatter(Formatter):

    name = "h5py"

    @classmethod
    def accepts(cls, path):
        path = get_path(path)
        return path.endswith(".hdf5") or path.endswith(".h5")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import h5py

        return h5py.File(get_path(path), "r", *args, **kwargs)

    @classmethod
    def dump(cls, path, *args, **kwargs):
        import h5py

        return h5py.File(get_path(path), "w", *args, **kwargs)
