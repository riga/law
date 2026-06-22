"""
Awkward target formatters.
"""

from __future__ import annotations

__all__ = ["AwkwardFormatter"]

import pathlib

from law._types import Any
from law.logger import get_logger
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter, PickleFormatter
from law.util import no_value

logger = get_logger(__name__)

from law.contrib.awkward.util import from_parquet


class AwkwardFormatter(Formatter):

    name = "awkward"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".parquet", ".parq", ".json", ".pickle", ".pkl"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        path = get_path(path)

        if path.endswith((".parquet", ".parq")):
            return from_parquet(path, *args, **kwargs)

        if path.endswith(".json"):
            import awkward as ak
            return ak.from_json(path, *args, **kwargs)

        # .pickle, .pkl
        return PickleFormatter.load(path, *args, **kwargs)

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

        if _path.endswith((".parquet", ".parq")):
            import awkward as ak
            ret = ak.to_parquet(obj, _path, *args, **kwargs)

        elif _path.endswith(".json"):
            import awkward as ak
            ret = ak.to_json(obj, _path, *args, **kwargs)

        else:  # .pickle, .pkl
            ret = PickleFormatter.dump(_path, obj, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class DaskAwkwardFormatter(Formatter):

    name = "dask_awkward"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".parquet", ".parq", ".json"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import dask_awkward as dak

        path = get_path(path)

        if path.endswith(".json"):
            return dak.from_json(path, *args, **kwargs)

        # .parquet, .parq
        return dak.from_parquet(path, *args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        obj: Any,
        *args,
        **kwargs,
    ) -> Any:
        import dask_awkward as dak

        _path = get_path(path)
        perm = kwargs.pop("perm", no_value)

        if _path.endswith(".json"):
            ret = dak.to_json(obj, _path, *args, **kwargs)

        else:  # .parquet, .parq
            ret = dak.to_parquet(obj, _path, *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
