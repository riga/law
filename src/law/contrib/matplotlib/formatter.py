"""
Matplotlib target formatter.
"""

from __future__ import annotations

__all__ = ["MatplotlibFormatter"]

import pathlib

from law._types import Any
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter
from law.util import no_value


class MatplotlibFormatter(Formatter):
    """
    Formatter that saves matplotlib figures as pdf (``.pdf``) or png files (``.png``) via ``fig.savefig``. Loading is
    not supported. Additional arguments are forwarded. When dumping, the file permission can be set via *perm*. Its name
    is ``"mpl"``, which can be passed as *formatter* to select it explicitly.
    """

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
    ) -> Any:
        perm = kwargs.pop("perm", no_value)

        ret = fig.savefig(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret
