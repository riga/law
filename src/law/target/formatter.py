"""
Formatter classes for file targets.
"""

from __future__ import annotations

__all__ = ["AUTO_FORMATTER", "Formatter", "find_formatter", "find_formatters", "get_formatter"]

import gzip
import json
import os
import pathlib
import pickle
import tarfile
import zipfile

from law._types import Any, ModuleType
from law.logger import Logger, get_logger
from law.util import import_file, make_list

logger: Logger = get_logger(__name__)


AUTO_FORMATTER = "auto"


class FormatterRegister(type):

    formatters: dict[str, FormatterRegister] = {}
    name: str

    def __new__(meta_cls, cls_name, bases, cls_dict) -> FormatterRegister:
        cls: FormatterRegister = super().__new__(meta_cls, cls_name, bases, cls_dict)

        if cls_name in meta_cls.formatters:
            raise ValueError(f"duplicate formatter name '{cls_name}' for class {cls}")
        if cls_name == AUTO_FORMATTER:
            raise ValueError(f"formatter class {cls} must not be named '{AUTO_FORMATTER}'")

        # store classes by name attribute
        name = cls.name
        if name != "_base":
            meta_cls.formatters[name] = cls
            logger.debug(f"registered target formatter '{name}'")

        return cls

    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        raise NotImplementedError

    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        raise NotImplementedError

    def dump(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        raise NotImplementedError


class Formatter(metaclass=FormatterRegister):

    name = "_base"

    # modes
    LOAD = "load"
    DUMP = "dump"

    @classmethod
    def chmod(cls, target: FileSystemTarget | Any, perm: int | None = None) -> None:
        if not isinstance(target, FileSystemTarget):
            return

        if perm is None:
            perm = (
                target.fs.default_file_perm
                if isinstance(target, FileSystemFileTarget)
                else target.fs.default_dir_perm
            )

        if perm:
            target.chmod(perm)


def get_formatter(name: str, silent: bool = False) -> FormatterRegister | None:
    """
    Returns the formatter class whose name attribute is *name*. When no class could be found and
    *silent* is *True*, *None* is returned. Otherwise, an exception is raised.
    """
    formatter = FormatterRegister.formatters.get(name)
    if formatter or silent:
        return formatter
    raise Exception(f"cannot find formatter '{name}'")


def find_formatters(
    path: str | pathlib.Path | FileSystemTarget,
    mode: str,
    silent: bool = True,
) -> list[FormatterRegister]:
    """
    Returns a list of formatter classes which would accept the file given by *path* and *mode*,
    which should either be ``"load"`` or ``"dump"``. When no classes could be found and *silent* is
    *True*, an empty list is returned. Otherwise, an exception is raised.
    """
    path = get_path(path)
    formatters = [f for f in FormatterRegister.formatters.values() if f.accepts(path, mode)]
    if formatters or silent:
        return formatters
    raise Exception(f"cannot find any '{mode}' formatter for {path}")


def find_formatter(
    path: str | pathlib.Path | FileSystemTarget,
    mode: str,
    name: str = AUTO_FORMATTER,
) -> FormatterRegister:
    """
    Returns the formatter class whose name attribute is *name* when *name* is not *AUTO_FORMATTER*.
    Otherwise, the first formatter that accepts *path* is returned. Internally, this method simply
    uses :py:func:`get_formatter` or :py:func:`find_formatters` depending on the value of *name*.
    """
    if name == AUTO_FORMATTER:
        return find_formatters(path, mode, silent=False)[0]
    return get_formatter(name, silent=False)  # type: ignore[return-value]


def _accepts_ext(path: str | pathlib.Path | FileSystemTarget, *exts: str) -> bool:
    # checks if *path* ends with one of the extensions *exts*, optionally followed by ".gz"
    return get_path(path).endswith(exts + tuple(f"{ext}.gz" for ext in exts))


def _open(path: str | pathlib.Path | FileSystemTarget, mode: str, **kwargs) -> Any:
    # opens the file at *path*, using gzip for (de)compression when the path ends with ".gz"
    path = get_path(path)
    if path.endswith(".gz"):
        # gzip.open uses binary mode by default, so select text mode unless binary mode is requested
        if "b" not in mode and "t" not in mode:
            mode += "t"
        return gzip.open(path, mode, **kwargs)
    return open(path, mode, **kwargs)  # noqa: SIM115


class TextFormatter(Formatter):

    name = "text"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return _accepts_ext(path, ".txt")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> str:
        with _open(path, "r", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return f.read(*args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        content: Any,
        *args,
        **kwargs,
    ) -> None:
        with _open(path, "w", encoding=kwargs.pop("encoding", "utf-8")) as f:
            f.write(str(content), *args, **kwargs)


class JSONFormatter(Formatter):

    name = "json"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return _accepts_ext(path, ".json")

    @classmethod
    def load(_cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        # kwargs might contain *cls*
        with _open(path, "r", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return json.load(f, *args, **kwargs)

    @classmethod
    def dump(_cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        # kwargs might contain *cls*
        with _open(path, "w", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return json.dump(obj, f, *args, **kwargs)


class PickleFormatter(Formatter):

    name = "pickle"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return _accepts_ext(path, ".pkl", ".pickle", ".p")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        with _open(path, "rb") as f:
            return pickle.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        with _open(path, "wb") as f:
            return pickle.dump(obj, f, *args, **kwargs)


class YAMLFormatter(Formatter):

    name = "yaml"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return _accepts_ext(path, ".yaml", ".yml")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        import yaml

        with _open(path, "r", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return yaml.safe_load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        import yaml

        with _open(path, "w", encoding=kwargs.pop("encoding", "utf-8")) as f:
            return yaml.dump(obj, f, *args, **kwargs)


class TarFormatter(Formatter):

    name = "tar"

    @classmethod
    def infer_compression(cls, path: str | pathlib.Path | FileSystemTarget) -> str | None:
        path = get_path(path)
        if path.endswith((".tar.gz", ".tgz")):
            return "gz"
        if path.endswith((".tar.bz2", ".tbz2", ".bz2")):
            return "bz2"
        if path.endswith((".tar.xz", ".txz", ".lzma")):
            return "xz"
        return None

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        # accept uncompressed tar files as well as those with a known compression
        return get_path(path).endswith(".tar") or cls.infer_compression(path) is not None

    @classmethod
    def load(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        dst: str | pathlib.Path | FileSystemDirectoryTarget,
        *args,
        **kwargs,
    ) -> None:
        # get the mode from args and kwargs, default to read mode with inferred compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")
        else:
            compression = cls.infer_compression(path)
            mode = "r" if not compression else "r:" + compression

        # arguments passed to extractall(), using the "data" filter by default where supported (the
        # default as of python 3.14) for consistent and safe extraction across versions
        extractall_kwargs = dict(kwargs.pop("extractall_kwargs", None) or {})
        if getattr(tarfile, "data_filter", None) is not None:
            extractall_kwargs.setdefault("filter", "data")

        # open tar file and extract to dst
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            f.extractall(get_path(dst), **extractall_kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        src: str | pathlib.Path | FileSystemDirectoryTarget,
        *args,
        **kwargs,
    ) -> None:
        # get the mode from args and kwargs, default to write mode with inferred compression
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")
        else:
            compression = cls.infer_compression(path)
            mode = "w" if not compression else "w:" + compression

        # arguments passed to add()
        add_kwargs = kwargs.pop("add_kwargs", None) or {}

        # backwards compatibility
        _filter = kwargs.pop("filter", None)
        if _filter is not None:
            logger.warning_once(
                "passing filter=callback' to TarFormatter.dump is deprecated and will be removed "
                "in a future release; please use 'add_kwargs=dict(filter=callback)' instead",
            )
            add_kwargs["filter"] = _filter

        # open a new zip file and add all files in src
        with tarfile.open(get_path(path), mode, *args, **kwargs) as f:
            srcs = [os.path.abspath(get_path(src)) for src in make_list(src)]
            common_prefix = os.path.commonpath(srcs)
            for src in srcs:
                _add_kwargs = {"arcname": os.path.relpath(src, common_prefix)}
                _add_kwargs.update(add_kwargs)
                f.add(src, **_add_kwargs)  # type: ignore[arg-type]


class ZipFormatter(Formatter):

    name = "zip"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".zip")

    @classmethod
    def load(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        dst: str | pathlib.Path | FileSystemDirectoryTarget,
        *args,
        **kwargs,
    ) -> None:
        # assume read mode, but also check args and kwargs
        mode = "r"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # arguments passed to extractall()
        extractall_kwargs = kwargs.pop("extractall_kwargs", None) or {}

        # open zip file and extract to dst
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:  # type: ignore[call-overload]
            f.extractall(get_path(dst), **extractall_kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        src: str | pathlib.Path | FileSystemDirectoryTarget,
        *args,
        **kwargs,
    ) -> None:
        # assume write mode, but also check args and kwargs
        mode = "w"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # arguments passed to write()
        write_kwargs = kwargs.pop("write_kwargs", None) or {}

        # open a new zip file and add all files in src
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:  # type: ignore[call-overload]
            src = get_path(src)
            if os.path.isfile(src):
                f.write(src, os.path.basename(src), **write_kwargs)
            else:
                for elem in os.listdir(src):
                    f.write(os.path.join(src, elem), elem, **write_kwargs)


class GZipFormatter(Formatter):

    name = "gzip"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".gz")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        # assume read mode, but also check args and kwargs
        mode = "r"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # arguments passed to read()
        read_kwargs = kwargs.pop("read_kwargs", None) or {}

        # open with gzip and return content
        with gzip.open(get_path(path), mode, *args, **kwargs) as f:
            return f.read(**read_kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> int:
        # assume write mode, but also check args and kwargs
        mode = "w"
        if args:
            mode = args[0]
            args = args[1:]
        elif "mode" in kwargs:
            mode = kwargs.pop("mode")

        # arguments passed to write()
        write_kwargs = kwargs.pop("write_kwargs", None) or {}

        # write into a new gzip file
        with gzip.open(get_path(path), mode, *args, **kwargs) as f:
            return f.write(obj, **write_kwargs)


class PythonFormatter(Formatter):

    name = "python"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".py")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> ModuleType:
        return import_file(get_path(path), *args, **kwargs)


# trailing imports
from law.target.file import (
    FileSystemDirectoryTarget,
    FileSystemFileTarget,
    FileSystemTarget,
    get_path,
)
