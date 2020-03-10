# coding: utf-8

"""
NumPy target formatters.
"""


__all__ = ["NumpyFormatter"]


import logging

from law.target.formatter import Formatter
from law.target.file import get_path


logger = logging.getLogger(__name__)


class NumpyFormatter(Formatter):

    name = "numpy"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".npy", ".npz", ".txt"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        import numpy as np

        path = get_path(path)
        func = np.loadtxt if path.endswith(".txt") else np.load
        return func(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, *args, **kwargs):
        import numpy as np

        path = get_path(path)

        if path.endswith(".txt"):
            func = np.savetxt
        elif path.endswith(".npz"):
            compress_flag = "savez_compressed"
            compress = False
            if compress_flag in kwargs:
                if isinstance(kwargs[compress_flag], bool):
                    compress = kwargs.pop(compress_flag)
                else:
                    logger.warning("the '{}' argument is reserved to set compression".format(
                        compress_flag))
            func = np.savez_compressed if compress else np.savez
        else:
            func = np.save

        func(path, *args, **kwargs)
