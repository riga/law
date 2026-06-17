# coding: utf-8

"""
Helpers for working with awkward.
"""

__all__ = [
    "from_parquet",
]


import inspect

from law.util import patch_object
from law._types import Any


def from_parquet(*args, use_threads: bool = False, **kwargs) -> Any:
    """
    Same as :func:`awkward.from_parquet`, but allowing to configure the *use_threads* option of the
    underlying ``pyarrow.parquet.ParquetFile.read`` operation.
    """
    import pyarrow.parquet as pq  # type: ignore[import-untyped, import-not-found]
    import awkward as ak  # type: ignore[import-untyped, import-not-found]

    # identify current signature defaults
    defaults = pq.ParquetFile.read.__defaults__

    # build patched ones
    spec = inspect.getfullargspec(pq.ParquetFile.read)
    if getattr(spec, "defaults", None) is None:
        raise RuntimeError("cannot find default arguments of 'ParquetFile.read'")
    default_arg_names = spec.args[len(spec.args) - len(spec.defaults):]  # type: ignore[arg-type]
    if "use_threads" not in default_arg_names:
        raise RuntimeError("cannot find 'use_threads' argument in 'ParquetFile.read'")
    idx = default_arg_names.index("use_threads")
    defaults = defaults[:idx] + (use_threads,) + defaults[idx + 1:]

    # patch for the duration of the call
    with patch_object(pq.ParquetFile.read, "__defaults__", defaults):
        return ak.from_parquet(*args, **kwargs)
