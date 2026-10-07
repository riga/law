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
from law.errors import FormatterNotFoundError
from law.logger import Logger, get_logger
from law.util import import_file, make_list

logger: Logger = get_logger(__name__)


#: Formatter name that selects the formatter automatically based on the file path.
AUTO_FORMATTER = "auto"


class FormatterRegister(type):
    """
    Meta class of formatters that registers all formatter classes by their ``name`` attribute, see
    :py:func:`get_formatter`. Names must be unique and must not be :py:data:`AUTO_FORMATTER`.
    """

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
        """
        Returns whether the formatter accepts the file at *path* in a certain *mode*.

        :param path: The path of the file.
        :param mode: Either ``"load"`` or ``"dump"``.
        :raises NotImplementedError: When not implemented by the formatter.
        :return: Whether the file is accepted.
        """
        raise NotImplementedError

    def load(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        """
        Loads the content of the file at *path*.

        :param path: The path of the file.
        :param args: Formatter-specific arguments.
        :param kwargs: Formatter-specific keyword arguments.
        :raises NotImplementedError: When not implemented by the formatter.
        :return: The loaded content.
        """
        raise NotImplementedError

    def dump(cls, path: str | pathlib.Path | FileSystemTarget, *args, **kwargs) -> Any:
        """
        Dumps content into the file at *path*.

        :param path: The path of the file.
        :param args: Formatter-specific arguments, usually starting with the content to dump.
        :param kwargs: Formatter-specific keyword arguments.
        :raises NotImplementedError: When not implemented by the formatter.
        :return: Formatter-specific return value.
        """
        raise NotImplementedError


class Formatter(metaclass=FormatterRegister):
    """
    Base class of formatters that load and dump file contents, which is used by
    :py:meth:`~law.target.file.FileSystemTarget.load` and :py:meth:`~law.target.file.FileSystemTarget.dump`.

    Custom formatters are defined by subclassing this class, setting a unique ``name`` and implementing the class
    methods ``accepts``, ``load`` and ``dump``. Example:

    .. code-block:: python

        class CSVFormatter(law.target.formatter.Formatter):

            name = "csv"

            @classmethod
            def accepts(cls, path, mode):
                return get_path(path).endswith(".csv")

            @classmethod
            def load(cls, path, *args, **kwargs):
                with open(get_path(path), "r") as f:
                    return list(csv.reader(f, *args, **kwargs))

            @classmethod
            def dump(cls, path, rows, *args, **kwargs):
                with open(get_path(path), "w") as f:
                    csv.writer(f, *args, **kwargs).writerows(rows)
    """

    name = "_base"

    # modes
    LOAD = "load"
    DUMP = "dump"

    @classmethod
    def chmod(cls, target: FileSystemTarget | Any, perm: int | None = None) -> None:
        """
        Changes the permission of *target* when it is a file system target. This is a helper for formatters that create
        files or directories.

        :param target: The target.
        :param perm: The permission, defaulting to the default file or directory permission of the file system of
            *target*.
        """
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
    Returns the formatter class whose name attribute is *name*.

    :param name: The name of the formatter.
    :param silent: When *True*, *None* is returned instead of raising an exception when no class could be found.
    :raises FormatterNotFoundError: When no class could be found and *silent* is *False*.
    :return: The formatter class, or *None*.
    """
    formatter = FormatterRegister.formatters.get(name)
    if formatter or silent:
        return formatter
    raise FormatterNotFoundError(f"cannot find formatter '{name}'")


def find_formatters(
    path: str | pathlib.Path | FileSystemTarget,
    mode: str,
    silent: bool = True,
) -> list[FormatterRegister]:
    """
    Returns a list of formatter classes which would accept the file given by *path* and *mode*.

    :param path: The path of the file.
    :param mode: Either ``"load"`` or ``"dump"``.
    :param silent: When *True*, an empty list is returned instead of raising an exception when no classes could be
        found.
    :raises FormatterNotFoundError: When no classes could be found and *silent* is *False*.
    :return: The list of formatter classes.
    """
    path = get_path(path)
    formatters = [f for f in FormatterRegister.formatters.values() if f.accepts(path, mode)]
    if formatters or silent:
        return formatters
    raise FormatterNotFoundError(f"cannot find any '{mode}' formatter for {path}")


def find_formatter(
    path: str | pathlib.Path | FileSystemTarget,
    mode: str,
    name: str = AUTO_FORMATTER,
) -> FormatterRegister:
    """
    Returns a formatter class. Internally, this method simply uses :py:func:`get_formatter` or
    :py:func:`find_formatters` depending on the value of *name*.

    :param path: The path of the file.
    :param mode: Either ``"load"`` or ``"dump"``.
    :param name: The name of the formatter. When *AUTO_FORMATTER*, the first formatter that accepts *path* is returned.
    :return: The formatter class.
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
    """
    Formatter for text files (``.txt``). ``load`` returns the file content as a string and ``dump`` writes the string
    representation of an object. Additional arguments are forwarded to :py:meth:`~io.TextIOBase.read` and
    :py:meth:`~io.TextIOBase.write`. Gzip-compressed files with an additional ``.gz`` extension are handled
    transparently. The file encoding can be set via *encoding*, defaulting to ``"utf-8"``.
    """

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
    """
    Formatter for json files (``.json``). ``load`` and ``dump`` forward additional arguments to :py:func:`json.load` and
    :py:func:`json.dump`. Gzip-compressed files with an additional ``.gz`` extension are handled transparently. The file
    encoding can be set via *encoding*, defaulting to ``"utf-8"``.
    """

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
    """
    Formatter for pickle files (``.pkl``, ``.pickle`` or ``.p``). ``load`` and ``dump`` forward additional arguments to
    :py:func:`pickle.load` and :py:func:`pickle.dump`. Gzip-compressed files with an additional ``.gz`` extension are
    handled transparently.
    """

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
    """
    Formatter for yaml files (``.yaml`` or ``.yml``). ``load`` and ``dump`` forward additional arguments to
    ``yaml.safe_load`` and ``yaml.dump``, respectively. Gzip-compressed files with an additional ``.gz`` extension are
    handled transparently. The file encoding can be set via *encoding*, defaulting to ``"utf-8"``.
    """

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
    """
    Formatter for tar archives (``.tar``), optionally compressed (``.tar.gz``, ``.tgz``, ``.tar.bz2``, ``.tbz2``,
    ``.bz2``, ``.tar.xz``, ``.txz`` or ``.lzma``). The mode passed to :py:func:`tarfile.open` is inferred from the
    extension and can be set via the first additional argument or *mode*. All other additional arguments are forwarded
    to :py:func:`tarfile.open` as well. Example:

    .. code-block:: python

        # extract the archive into a directory, passing extractall_kwargs to TarFile.extractall()
        target.load("/path/to/dir")

        # add files or directories to a new archive with names relative to their common path,
        # passing add_kwargs to TarFile.add()
        target.dump(["/path/to/file", "/path/to/dir"])

    During extraction, the ``"data"`` filter is used by default where supported.
    """

    name = "tar"

    @classmethod
    def infer_compression(cls, path: str | pathlib.Path | FileSystemTarget) -> str | None:
        """
        Returns the compression type of the tar archive at *path* inferred from its extension.

        :param path: The path of the archive.
        :return: ``"gz"``, ``"bz2"`` or ``"xz"``, or *None* when it is not compressed.
        """
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
    """
    Formatter for zip archives (``.zip``). The mode passed to :py:class:`zipfile.ZipFile` can be set via the first
    additional argument or *mode*, and all other additional arguments are forwarded to it as well. Example:

    .. code-block:: python

        # extract the archive into a directory, passing extractall_kwargs to ZipFile.extractall()
        target.load("/path/to/dir")

        # add a file or the contents of a directory to a new archive,
        # passing write_kwargs to ZipFile.write()
        target.dump("/path/to/dir")
    """

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
    """
    Formatter for gzip-compressed files (``.gz``). ``load`` returns the decompressed content and ``dump`` writes an
    object. The mode passed to :py:func:`gzip.open` can be set via the first additional argument or *mode*, defaulting
    to binary mode, and all other additional arguments are forwarded to it as well. *read_kwargs* and *write_kwargs* are
    passed to the ``read`` and ``write`` methods of the file object.
    """

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
    """
    Formatter for python files (``.py``) that only supports loading. ``load`` imports the file as a module via
    :py:func:`law.util.import_file` and forwards all additional arguments.
    """

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
