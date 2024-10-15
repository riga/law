# coding: utf-8

"""
Coffea target formatters.
"""

__all__ = ["CoffeaFormatter"]


from law.target.formatter import Formatter
from law.target.file import get_path
from law.logger import get_logger
from law.util import no_value


logger = get_logger(__name__)


class CoffeaFormatter(Formatter):

    name = "coffea"

    @classmethod
    def accepts(cls, path, mode):
        return get_path(path).endswith((".coffea", ".root", ".parquet"))

    @classmethod
    def load(cls, path, *args, **kwargs):
        path = get_path(path)

        if path.endswith(".root"):
            from coffea.nanoevents import NanoEventsFactory
            return NanoEventsFactory.from_root(path, *args, **kwargs)

        if path.endswith(".parquet"):
            from coffea.nanoevents import NanoEventsFactory
            return NanoEventsFactory.from_parquet(path, *args, **kwargs)

        # .coffea
        from coffea.util import load
        return load(path, *args, **kwargs)

    @classmethod
    def dump(cls, path, out, *args, **kwargs):
        from coffea.util import save

        perm = kwargs.pop("perm", no_value)

        save(out, get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)
