"""
HDF5 target formatters.
"""

from __future__ import annotations

__all__ = ["H5pyFormatter"]

import pathlib

from law._types import Any
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter
from law.util import no_value


class H5pyFormatter(Formatter):
    """
    Formatter for hdf5 files (``.hdf5``, ``.h5``) that returns an opened ``h5py.File`` object, both
    when loading (in read mode) and dumping (in write mode). Additional arguments are forwarded to
    its constructor. When dumping, the file permission can be set via *perm*. Its name is
    ``"h5py"``, which can be passed as *formatter* to select it explicitly.
    """

    name = "h5py"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import h5py

        return h5py.File(get_path(path), "r", *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import h5py

        perm = kwargs.pop("perm", no_value)

        ret = h5py.File(get_path(path), "w", *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
