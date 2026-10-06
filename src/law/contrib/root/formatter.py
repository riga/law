"""
ROOT target formatters.
"""

from __future__ import annotations

__all__ = [
    "GuardedTFile", "ROOTFormatter", "ROOTNumpyFormatter", "ROOTPandasFormatter", "UprootFormatter",
]

import contextlib
import pathlib

from law._types import Any, Iterator, TracebackType
from law.contrib.root.util import import_ROOT
from law.target.file import FileSystemFileTarget, get_path
from law.target.formatter import Formatter
from law.util import no_value


class GuardedTFile:
    """
    Wrapper around a ``ROOT.TFile`` that forwards all attributes to it and closes it when used as a context manager. It
    is created from an existing ``TFile``, or from arguments that are passed to ``ROOT.TFile.Open``. Example:

    .. code-block:: python

        with GuardedTFile("/path/to/file.root", "READ") as tfile:
            hist = tfile.Get("hist")
    """

    @classmethod
    def Open(cls, *args, **kwargs) -> Any:
        """
        Opens a ``ROOT.TFile`` and wraps it in a new guarded file.

        :param args: Arguments forwarded to ``ROOT.TFile.Open``.
        :param kwargs: Keyword arguments forwarded to ``ROOT.TFile.Open``.
        :return: The guarded file.
        """
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
    """
    Formatter for ROOT files (``.root``) that returns a :py:class:`GuardedTFile`, both when loading and dumping.
    Additional arguments, such as the open mode, are forwarded to ``ROOT.TFile.Open``. Its name is ``"root"``, which can
    be passed as *formatter* to select it explicitly.
    """

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
    """
    Formatter for numpy arrays in ROOT files (``.root``) using ``root_numpy.root2array`` and ``root_numpy.array2root``.
    Additional arguments are forwarded. When dumping, the file permission can be set via *perm*. Its name is
    ``"root_numpy"``, which can be passed as *formatter* to select it explicitly.
    """

    name = "root_numpy"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_numpy

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
        import root_numpy

        perm = kwargs.pop("perm", no_value)

        ret = root_numpy.array2root(arr, get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class ROOTPandasFormatter(Formatter):
    """
    Formatter for pandas dataframes in ROOT files (``.root``) using ``root_pandas.read_root`` and ``DataFrame.to_root``.
    Additional arguments are forwarded. When dumping, the file permission can be set via *perm*. Its name is
    ``"root_pandas"``, which can be passed as *formatter* to select it explicitly.
    """

    name = "root_pandas"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_pandas

        return root_pandas.read_root(get_path(path), *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemFileTarget, df: Any, *args, **kwargs) -> Any:
        ROOT = import_ROOT()  # noqa: F841
        import root_pandas  # noqa: F401

        perm = kwargs.pop("perm", no_value)

        ret = df.to_root(get_path(path), *args, **kwargs)

        if perm != no_value:
            cls.chmod(path, perm)

        return ret


class UprootFormatter(Formatter):
    """
    Formatter for ROOT files (``.root``) using uproot. ``load`` returns the file opened with ``uproot.open``. ``dump``
    returns a context manager that yields a writable file, created with ``uproot.create``, ``uproot.recreate`` or
    ``uproot.update`` depending on *mode* (defaulting to ``"recreate"``), and closes it afterwards. Additional arguments
    are forwarded. When dumping, the file permission can be set via *perm*. Its name is ``"uproot"``, which can be
    passed as *formatter* to select it explicitly. Example:

    .. code-block:: python

        with target.dump(formatter="uproot") as f:
            f["tree"] = {"x": [1, 2, 3]}
    """

    name = "uproot"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemFileTarget, mode: str) -> bool:
        return get_path(path).endswith(".root")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemFileTarget, *args, **kwargs) -> Any:
        import uproot

        return uproot.open(get_path(path), *args, **kwargs)

    @classmethod
    @contextlib.contextmanager
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemFileTarget,
        mode: str = "recreate",
        **kwargs,
    ) -> Iterator[Any]:
        import uproot

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
            with contextlib.suppress(Exception):
                f.file.close()
                if perm != no_value:
                    cls.chmod(path, perm)
