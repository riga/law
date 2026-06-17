# coding: utf-8

"""
Pandas target formatters.
"""

from __future__ import annotations

__all__ = ["DataFrameFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import get_path, FileSystemFileTarget
from law.logger import get_logger
from law.util import no_value
from law._types import Any


logger = get_logger(__name__)


class DataFrameFormatter(Formatter):

    name = "pandas"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        # still missing: excel, html, xml, latex, feather, orc, sql, stata, markdown, ...
        suffixes = (".csv", ".json", ".parquet", ".h5", ".hdf5", ".pickle", ".pkl")
        return get_path(path).endswith(suffixes)

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import pandas  # type: ignore[import-untyped]

        path = get_path(path)

        if path.endswith(".csv"):
            return pandas.read_csv(path, *args, **kwargs)

        if path.endswith(".json"):
            return pandas.read_json(path, *args, **kwargs)

        if path.endswith(".parquet"):
            return pandas.read_parquet(path, *args, **kwargs)

        if path.endswith((".h5", ".hdf5")):
            return pandas.read_hdf(path, *args, **kwargs)

        if path.endswith((".pickle", ".pkl")):
            return pandas.read_pickle(path, *args, **kwargs)

        suffix = pathlib.Path(path).suffix
        raise NotImplementedError(f"suffix \"{suffix}\" not implemented in DataFrameFormatter")

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        obj: Any,
        *args,
        **kwargs,
    ) -> Any:
        _path = get_path(path)
        perm = kwargs.pop("perm", no_value)

        if _path.endswith(".csv"):
            ret = obj.to_csv(_path, *args, **kwargs)

        elif _path.endswith(".json"):
            ret = obj.to_json(_path, *args, **kwargs)

        elif _path.endswith(".parquet"):
            ret = obj.to_parquet(_path, *args, **kwargs)

        elif _path.endswith((".h5", ".hdf5")):
            ret = obj.to_hdf(_path, *args, **kwargs)

        elif _path.endswith((".pickle", ".pkl")):
            ret = obj.to_pickle(_path, *args, **kwargs)

        else:
            suffix = pathlib.Path(_path).suffix
            raise NotImplementedError(f"suffix \"{suffix}\" not implemented in DataFrameFormatter")

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
