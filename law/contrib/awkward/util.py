# coding: utf-8

"""
Helpers for working with awkward.
"""

__all__ = [
    "from_parquet",
]


import inspect

from law.util import patch_object


def from_parquet(*args, use_threads=False, **kwargs):
    """
    Same as :func:`awkward.from_parquet`, but allowing to configure the *use_threads* option of the
    underlying ``pyarrow.parquet.ParquetFile.read`` operation.
    """
    import pyarrow.parquet as pq
    import awkward as ak

    # identify current signature defaults
    defaults = pq.ParquetFile.read.__defaults__

    # build patched ones
    spec = inspect.getfullargspec(pq.ParquetFile.read)
    default_arg_names = spec.args[len(spec.args) - len(spec.defaults):]
    if "use_threads" not in default_arg_names:
        raise RuntimeError("cannot find 'use_threads' argument in 'ParquetFile.read'")
    idx = default_arg_names.index("use_threads")
    defaults = defaults[:idx] + (use_threads,) + defaults[idx + 1:]

    # patch for the duration of the call
    with patch_object(pq.ParquetFile.read, "__defaults__", defaults):
        return ak.from_parquet(*args, **kwargs)
