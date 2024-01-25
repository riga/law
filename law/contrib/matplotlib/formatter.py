# coding: utf-8

"""
Matplotlib target formatter.
"""

from __future__ import annotations

__all__ = ["MatplotlibFormatter"]

import pathlib

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law._types import Any


class MatplotlibFormatter(Formatter):

    name = "mpl"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        # only dumping supported
        return mode == "dump" and get_path(path).endswith((".pdf", ".png"))

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        fig: Any,
        *args,
        **kwargs,
    ) -> None:
        fig.savefig(get_path(path), *args, **kwargs)
