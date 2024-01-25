# coding: utf-8

"""
Coffea target formatters.
"""

from __future__ import annotations

__all__ = ["CoffeaFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law.logger import get_logger
from law._types import Any


logger = get_logger(__name__)


class CoffeaFormatter(Formatter):

    name = "coffea"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith((".coffea", ".root", ".parquet"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        path = get_path(path)

        if path.endswith(".root"):
            from coffea.nanoevents import NanoEventsFactory  # type: ignore[import-untyped, import-not-found] # noqa
            return NanoEventsFactory.from_root(path, *args, **kwargs)

        if path.endswith(".parquet"):
            from coffea.nanoevents import NanoEventsFactory  # type: ignore[import-untyped, import-not-found] # noqa
            return NanoEventsFactory.from_parquet(path, *args, **kwargs)

        # .coffea
        from coffea.util import load  # type: ignore[import-untyped, import-not-found]
        return load(path, *args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        out: Any,
        *args,
        **kwargs,
    ) -> Any:
        from coffea.util import save  # type: ignore[import-untyped, import-not-found]

        save(out, get_path(path), *args, **kwargs)
