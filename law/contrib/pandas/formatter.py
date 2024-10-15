# coding: utf-8

"""
Pandas target formatters.
"""

__all__ = ["DataFrameFormatter"]


import os

from law.target.formatter import Formatter
from law.target.file import get_path
from law.logger import get_logger
from law.util import no_value


logger = get_logger(__name__)


class DataFrameFormatter(Formatter):

    name = "dataframe"

    @classmethod
    def accepts(cls, path, mode):
        # still missing: excel, html, xml, latex, feather, orc, sql, stata, markdown, ...
        suffixes = (".csv", ".json", ".parquet", ".h5", ".hdf5", ".pickle", ".pkl")
        return get_path(path).endswith(suffixes)

    @classmethod
    def load(cls, path, *args, **kwargs):
        path = get_path(path)

        if path.endswith(".csv"):
            import pandas  # fmt: skip
            return pandas.read_csv(path, *args, **kwargs)

        if path.endswith(".json"):
            import pandas  # fmt: skip
            return pandas.read_json(path, *args, **kwargs)

        if path.endswith(".parquet"):
            import pandas  # fmt: skip
            return pandas.read_parquet(path, *args, **kwargs)

        if path.endswith((".h5", ".hdf5")):
            import pandas  # fmt: skip
            return pandas.read_hdf(path, *args, **kwargs)

        if path.endswith((".pickle", ".pkl")):
            import pandas  # fmt: skip
            return pandas.read_pickle(path, *args, **kwargs)

        suffix = os.path.splitext(path)[1]
        raise NotImplementedError(
            "suffix \"{}\" not implemented in DataFrameFormatter".format(suffix),
        )

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
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
            suffix = os.path.splitext(path)[1]
            raise NotImplementedError(
                "suffix \"{}\" not implemented in DataFrameFormatter".format(suffix),
            )

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
