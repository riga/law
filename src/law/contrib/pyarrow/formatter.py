"""
PyArrow target formatters.
"""

from __future__ import annotations

__all__ = ["ParquetFormatter", "ParquetTableFormatter"]

import pathlib

from law._types import Any
from law.logger import get_logger
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter
from law.util import no_value

logger = get_logger(__name__)


class ParquetFormatter(Formatter):
    """
    Formatter that loads parquet files (``.parquet``, ``.parq``) as ``pyarrow.parquet.ParquetFile`` objects. Dumping is
    not supported, see :py:class:`ParquetTableFormatter`. Additional arguments are forwarded. Its name is ``"parquet"``,
    which can be passed as *formatter* to select it explicitly.
    """

    name = "parquet"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".parquet", ".parq"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import pyarrow.parquet as pq

        return pq.ParquetFile(get_path(path), *args, **kwargs)


class ParquetTableFormatter(Formatter):
    """
    Formatter for pyarrow tables in parquet files (``.parquet``, ``.parq``), handled by ``pyarrow.parquet.read_table``
    and ``pyarrow.parquet.write_table``. Additional arguments are forwarded. When dumping, the file permission can be
    set via *perm*. Its name is ``"parquet_table"``, which can be passed as *formatter* to select it explicitly.
    """

    name = "parquet_table"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".parquet", ".parq"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import pyarrow.parquet as pq

        return pq.read_table(get_path(path), *args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        obj: Any,
        *args,
        **kwargs,
    ) -> Any:
        import pyarrow.parquet as pq

        perm = kwargs.pop("perm", no_value)

        ret = pq.write_table(obj, get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
