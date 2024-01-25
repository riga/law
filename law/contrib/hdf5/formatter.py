# coding: utf-8

"""
HDF5 target formatters.
"""

from __future__ import annotations

__all__ = ["H5pyFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law._types import Any


class H5pyFormatter(Formatter):

    name = "h5py"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".hdf5", ".h5"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import h5py  # type: ignore[import-untyped, import-not-found]

        return h5py.File(get_path(path), "r", *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import h5py  # type: ignore[import-untyped, import-not-found]

        return h5py.File(get_path(path), "w", *args, **kwargs)
