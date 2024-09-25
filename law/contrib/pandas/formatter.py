# coding: utf-8

"""
Pandas target formatters.
"""

__all__ = ["DataFrameFormatter"]


import pathlib
from law.target.formatter import Formatter
from law.target.file import get_path
from law.logger import get_logger


logger = get_logger(__name__)


class DataFrameFormatter(Formatter):

    name = "dataframe"

    @classmethod
    def accepts(cls, path, mode):
        # still missing: excel, html, xml, latex, feather, orc, sql, stata, markdown, ...
        return get_path(path).endswith(
            (".csv", ".json", ".parquet", ".h5", ".hdf5", ".pickle", ".pkl")
        )

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

        suffix = pathlib.Path(path).suffix
        raise NotImplementedError(f"Suffix \"{suffix}\" not implemented in DataFrameFormatter")

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        path = get_path(path)

        if path.endswith(".csv"):
            return obj.to_csv(path, *args, **kwargs)

        if path.endswith(".json"):
            return obj.to_json(path, *args, **kwargs)

        if path.endswith(".parquet"):
            return obj.to_parquet(path, *args, **kwargs)

        if path.endswith((".h5", ".hdf5")):
            return obj.to_hdf(path, *args, **kwargs)

        if path.endswith((".pickle", ".pkl")):
            return obj.to_pickle(path, *args, **kwargs)

        suffix = pathlib.Path(path).suffix
        raise NotImplementedError(f"Suffix \"{suffix}\" not implemented in DataFrameFormatter")
