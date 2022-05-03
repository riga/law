# coding: utf-8

"""
PyArrow target formatters.
"""

__all__ = ["ParquetFormatter", "ParquetTableFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path
from law.logger import get_logger


logger = get_logger(__name__)


class ParquetFormatter(Formatter):

    name = "parquet"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".parquet")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import pyarrow.parquet as pq

        path = get_path(path)
        return pq.ParquetFile(path, *args, **kwargs)


class ParquetTableFormatter(Formatter):

    name = "parquet_table"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith(".parquet")

    @classmethod
    def load(cls, path, *args, **kwargs):
        import pyarrow.parquet as pq

        path = get_path(path)
        return pq.read_table(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        import pyarrow.parquet as pq

        path = get_path(path)
        return pq.write_table(obj, path, *args, **kwargs)
