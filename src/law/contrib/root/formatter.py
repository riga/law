# coding: utf-8

"""
ROOT target formatters.
"""

from __future__ import annotations

__all__ = [
    "GuardedTFile", "ROOTFormatter", "ROOTNumpyFormatter", "ROOTPandasFormatter", "UprootFormatter",
]

import pathlib
from contextlib import contextmanager

from law.target.formatter import Formatter
from law.target.file import FileSystemFileTarget, get_path
from law.util import no_value
from law._types import Any, TracebackType, Iterator

from law.contrib.root.util import import_ROOT


class GuardedTFile(object):

    @classmethod
    def Open(cls, *args, **kwargs) -> Any:
        ROOT = import_ROOT()
        return cls(ROOT.TFile.Open(*args, **kwargs))

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()

        self._guarded_tfile = None

        ROOT = import_ROOT()
        if len(args) == 1 and isinstance(args[0], ROOT.TFile) and not kwargs:
            self._guarded_tfile = args[0]
        elif args or kwargs:
            self._guarded_tfile = ROOT.TFile.Open(*args, **kwargs)

    def __enter__(self) -> Any:
        return self._guarded_tfile

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> None:
        if self.IsOpen():
            self.Close()

    def __getattr__(self, attr: str) -> Any:
        if self._guarded_tfile is not None:
            return getattr(self._guarded_tfile, attr)
        raise AttributeError(f"cannot forward attribute '{attr}' to undefined guarded tfile")

    def __setattr__(self, attr: str, value: Any) -> None:
        if attr != "_guarded_tfile":
            setattr(self._guarded_tfile, attr, value)
        else:
            super().__setattr__(attr, value)


class ROOTFormatter(Formatter):

    name = "root"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> GuardedTFile:
        return GuardedTFile(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> GuardedTFile:
        return GuardedTFile(get_path(path), *args, **kwargs)


class ROOTNumpyFormatter(Formatter):

    name = "root_numpy"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_numpy  # type: ignore[import-untyped, import-not-found]

        return root_numpy.root2array(get_path(path), *args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        arr: Any,
        *args,
        **kwargs,
    ) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_numpy  # type: ignore[import-untyped, import-not-found]

        perm = kwargs.pop("perm", no_value)

        ret = root_numpy.array2root(arr, get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class ROOTPandasFormatter(Formatter):

    name = "root_pandas"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_pandas  # type: ignore[import-untyped, import-not-found]

        return root_pandas.read_root(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, df: Any, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        # importing root_pandas adds the to_root() method to data frames
        import root_pandas  # type: ignore[import-untyped, import-not-found] # noqa

        perm = kwargs.pop("perm", no_value)

        ret = df.to_root(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class UprootFormatter(Formatter):

    name = "uproot"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import uproot  # type: ignore[import-untyped, import-not-found]

        return uproot.open(get_path(path), *args, **kwargs)

    @classmethod
    @contextmanager
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        mode: str = "recreate",
        **kwargs,
    ) -> Iterator[Any]:
        import uproot  # type: ignore[import-untyped, import-not-found]

        # check the mode and get the saving function
        allowed_modes = ["create", "recreate", "update"]
        if not isinstance(mode, str) or mode.lower() not in allowed_modes:
            raise ValueError(f"unknown uproot writing mode: {mode}")
        fn = getattr(uproot, mode.lower())

        perm = kwargs.pop("perm", no_value)

        # create the file object and yield it
        f = fn(get_path(path), **kwargs)
        try:
            yield f
        finally:
            try:
                f.file.close()
                if perm != no_value:
                    cls.chmod(path, perm)
            except:
                pass
