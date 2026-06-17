# coding: utf-8

"""
NumPy target formatters.
"""

from __future__ import annotations

__all__ = ["NumpyFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law.logger import get_logger
from law.util import no_value
from law._types import Any, Callable


logger = get_logger(__name__)


class NumpyFormatter(Formatter):

    name = "numpy"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".npy", ".npz", ".txt"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import numpy as np  # type: ignore[import-untyped, import-not-found]

        path = get_path(path)
        func = np.loadtxt if str(path).endswith(".txt") else np.load
        return func(path, *args, **kwargs)  # type: ignore[operator]

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import numpy as np

        _path = get_path(path)
        perm = kwargs.pop("perm", no_value)

        func: Callable
        if str(_path).endswith(".txt"):
            func = np.savetxt
        elif str(_path).endswith(".npz"):
            compress_flag = "savez_compressed"
            compress = False
            if compress_flag in kwargs:
                if isinstance(kwargs[compress_flag], bool):
                    compress = kwargs.pop(compress_flag)
                else:
                    logger.warning(f"the '{compress_flag}' argument is reserved to set compression")
            func = np.savez_compressed if compress else np.savez
        else:
            func = np.save

        ret = func(_path, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
