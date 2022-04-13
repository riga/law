# coding: utf-8

"""
Awkward target formatters.
"""

__all__ = ["AwkwardFormatter"]


from law.target.formatter import Formatter, PickleFormatter
from law.target.file import get_path
from law.logger import get_logger


logger = get_logger(__name__)


class AwkwardFormatter(Formatter):

    name = "awkward"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".parquet", ".pickle", ".pkl"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        path = get_path(path)

        if path.endswith(".parquet"):
            import awkward as ak
            return ak.from_parquet(path, *args, **kwargs)

        # .pickle, .pkl
        return PickleFormatter.load(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, obj, *args, **kwargs):
        path = get_path(path)

        if path.endswith(".parquet"):
            import awkward as ak
            return ak.to_parquet(obj, path, *args, **kwargs)

        # .pickle, .pkl
        return PickleFormatter.dump(path, obj, *args, **kwargs)
