# coding: utf-8

"""
Formatter classes for file targets.
"""

from __future__ import annotations

__all__ = ["AUTO_FORMATTER", "Formatter", "get_formatter", "find_formatters", "find_formatter"]

import os
import json
import pickle
import zipfile
import gzip
import tarfile
import pathlib

from law.util import make_list, import_file
from law.logger import get_logger, Logger
from law._types import Type, Any, ModuleType


logger: Logger = get_logger(__name__)  # type: ignore[assignment]


AUTO_FORMATTER = "auto"


class FormatterRegister(type):

    formatters: dict[str, Type[Formatter]] = {}

    def __new__(meta_cls, cls_name, bases, cls_dict) -> FormatterRegister:
        cls = super().__new__(meta_cls, cls_name, bases, cls_dict)

        if cls_name in meta_cls.formatters:
            raise ValueError(f"duplicate formatter name '{cls_name}' for class {cls}")
        if cls_name == AUTO_FORMATTER:
            raise ValueError(f"formatter class {cls} must not be named '{AUTO_FORMATTER}'")

        # store classes by name
        if cls_name != "_base":
            meta_cls.formatters[cls_name] = cls  # type: ignore[assignment]
            logger.debug(f"registered target formatter '{cls_name}'")

        return cls


class Formatter(metaclass=FormatterRegister):

    name = "_base"

    # modes
    LOAD = "load"
    DUMP = "dump"

    @classmethod
    def accepts(cls, path, mode):
        raise NotImplementedError

    @classmethod
    def load(cls, path, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def dump(cls, path, *args, **kwargs):
        raise NotImplementedError


def get_formatter(name: str, silent: bool = False) -> Type[Formatter] | None:
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
) -> list[Type[Formatter]]:
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
) -> Type[Formatter]:
    """
    Returns the formatter class whose name attribute is *name* when *name* is not *AUTO_FORMATTER*.
    Otherwise, the first formatter that accepts *path* is returned. Internally, this method simply
    uses :py:func:`get_formatter` or :py:func:`find_formatters` depending on the value of *name*.
    """
    if name == AUTO_FORMATTER:
        return find_formatters(path, mode, silent=False)[0]
    return get_formatter(name, silent=False)  # type: ignore[return-value]


class TextFormatter(Formatter):

    name = "text"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".txt")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> str:
        with open(get_path(path), "r") as f:
            return f.read(*args, **kwargs)

    @classmethod
    def dump(
        cls,
        path: str | pathlib.Path | FileSystemTarget,
        content: Any,
        *args,
        **kwargs,
    ) -> None:
        with open(get_path(path), "w") as f:
            f.write(str(content), *args, **kwargs)


class JSONFormatter(Formatter):

    name = "json"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".json")

    @classmethod
    def load(_cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        # kwargs might contain *cls*
        with open(get_path(path), "r") as f:
            return json.load(f, *args, **kwargs)

    @classmethod
    def dump(_cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        # kwargs might contain *cls*
        with open(get_path(path), "w") as f:
            return json.dump(obj, f, *args, **kwargs)


class PickleFormatter(Formatter):

    name = "pickle"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith((".pkl", ".pickle", ".p"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        with open(get_path(path), "rb") as f:
            return pickle.load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        with open(get_path(path), "wb") as f:
            return pickle.dump(obj, f, *args, **kwargs)


class YAMLFormatter(Formatter):

    name = "yaml"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith((".yaml", ".yml"))

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        import yaml  # type: ignore[import-untyped, import-not-found]

        with open(get_path(path), "r") as f:
            return yaml.safe_load(f, *args, **kwargs)

    @classmethod
    def dump(cls, path: str | pathlib.Path | FileSystemTarget, obj: Any, *args, **kwargs) -> None:
        import yaml  # type: ignore[import-untyped, import-not-found]

        with open(get_path(path), "w") as f:
            return yaml.dump(obj, f, *args, **kwargs)


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
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:  # type: ignore[arg-type]
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
        with zipfile.ZipFile(get_path(path), mode, *args, **kwargs) as f:  # type: ignore[arg-type]
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
        return cls.infer_compression(path) is not None

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

        # arguments passed to extractall()
        extractall_kwargs = kwargs.pop("extractall_kwargs", None) or {}

        # open zip file and extract to dst
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
            common_prefix = os.path.commonprefix(srcs)
            for src in srcs:
                _add_kwargs = {"arcname": os.path.relpath(src, common_prefix)}
                _add_kwargs.update(add_kwargs)
                f.add(src, **_add_kwargs)  # type: ignore[arg-type]


class PythonFormatter(Formatter):

    name = "python"

    @classmethod
    def accepts(cls, path: str | pathlib.Path | FileSystemTarget, mode: str) -> bool:
        return get_path(path).endswith(".py")

    @classmethod
    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> ModuleType:
        return import_file(get_path(path), *args, **kwargs)


# trailing imports
from law.target.file import get_path, FileSystemTarget, FileSystemDirectoryTarget
