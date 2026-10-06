"""
Custom luigi file system and target objects.
"""

from __future__ import annotations

__all__ = [
    "FileSystem",
    "FileSystemDirectoryTarget",
    "FileSystemFileTarget",
    "FileSystemTarget",
    "add_scheme",
    "get_path",
    "get_scheme",
    "has_scheme",
    "localize_file_targets",
    "remove_scheme",
]

import abc
import contextlib
import functools
import os
import pathlib
import re
import sys

import law.target.luigi_shims as shims
from law._types import (
    IO,
    AbstractContextManager,
    Any,
    Callable,
    Generator,
    Iterator,
    Literal,
    Self,
    T,
    overload,
)
from law.config import Config
from law.target.base import Target
from law.util import create_random_string, human_bytes, map_struct, no_value


class FileSystem(shims.FileSystem):
    """
    Abstract base class of file systems that perform operations on files and directories identified
    by paths. See :py:class:`law.target.local.LocalFileSystem` and
    :py:class:`law.target.remote.RemoteFileSystem` for implementations.

    *name* is an optional name of the file system, e.g. the config section it was configured from.
    *has_permissions* decides whether file and directory permissions are set at all, with
    *default_file_perm* and *default_dir_perm* being the default permissions of new files and
    directories. *create_file_dir* decides whether missing directories are created when files are
    written, copied or moved.
    """

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        # reads a law config section and returns parsed file system configs
        cfg = Config.instance()

        if config is None:
            config = {}

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option: str, func: Callable[[str, str], Any]) -> None:
            if option not in config or overwrite:
                config[option] = func(section, option)

        # read configs
        int_or_none = functools.partial(cfg.get_expanded_int, default=None)
        add("has_permissions", cfg.get_expanded_bool)
        add("default_file_perm", int_or_none)
        add("default_dir_perm", int_or_none)
        add("create_file_dir", cfg.get_expanded_bool)

        return config

    def __init__(
        self,
        name: str | None = None,
        *,
        has_permissions: bool = True,
        default_file_perm: int | None = None,
        default_dir_perm: int | None = None,
        create_file_dir: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.name = name
        self.has_permissions = has_permissions
        self.default_file_perm = default_file_perm
        self.default_dir_perm = default_dir_perm
        self.create_file_dir = create_file_dir

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name}, {hex(id(self))})"

    def dirname(self, path: str | pathlib.Path) -> str | None:
        """
        Returns the directory name of *path*.

        :param path: The path.
        :return: The directory name, or *None* for the root directory ``"/"``.
        """
        return os.path.dirname(str(path)) if path != "/" else None

    def basename(self, path: str | pathlib.Path) -> str:
        """
        Returns the base name of *path*.

        :param path: The path.
        :return: The base name, which is ``"/"`` for the root directory.
        """
        return os.path.basename(str(path)) if path != "/" else "/"

    def ext(self, path: str | pathlib.Path, n: int = 1) -> str:
        """
        Returns the file extension of *path*. Leading dots of hidden files are ignored. Example:

        .. code-block:: python

            fs.ext("/path/to/file.tar.gz")       # -> "gz"
            fs.ext("/path/to/file.tar.gz", n=2)  # -> "tar.gz"
            fs.ext("/path/to/file.tar.gz", n=0)  # -> "tar.gz"

        :param path: The path.
        :param n: Number of trailing dot-separated parts to return. All parts are returned when zero
            or negative.
        :return: The extension without the leading dot, or an empty string when there is no
            extension.
        """
        # split the path
        parts = self.basename(path).lstrip(".").split(".")

        # empty extension in the trivial case or use the last n parts except for the first one
        return "" if len(parts) == 1 else ".".join(parts[1:][min(-n, 0):])

    def _unscheme(self, path: str | pathlib.Path) -> str:
        return remove_scheme(path)

    @property
    @abc.abstractmethod
    def default_instance(self) -> FileSystem:
        """
        The default instance of this file system class.
        """
        ...

    @abc.abstractmethod
    def abspath(self, path: str | pathlib.Path) -> str:
        """
        Returns the absolute representation of *path* within this file system.

        :param path: The path.
        :return: The absolute path.
        """
        ...

    @abc.abstractmethod
    def stat(self, path: str | pathlib.Path, **kwargs) -> os.stat_result:
        """
        Returns the stat result of *path*.

        :param path: The path.
        :param kwargs: Additional, implementation-specific options.
        :return: The stat result.
        """
        ...

    @abc.abstractmethod
    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        **kwargs,
    ) -> bool | os.stat_result | None:
        """
        Returns whether *path* exists.

        :param path: The path.
        :param stat: When *True*, the stat result is returned instead of a boolean.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether *path* exists, or its stat result when *stat* is *True* (*None* when it
            does not exist).
        """
        ...

    @abc.abstractmethod
    def isdir(self, path: str | pathlib.Path, **kwargs) -> bool:
        """
        Returns whether *path* exists and is a directory.

        :param path: The path.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether *path* is an existing directory.
        """
        ...

    @abc.abstractmethod
    def isfile(self, path: str | pathlib.Path, **kwargs) -> bool:
        """
        Returns whether *path* exists and is a file.

        :param path: The path.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether *path* is an existing file.
        """
        ...

    @abc.abstractmethod
    def chmod(self, path: str | pathlib.Path, perm: int, *, silent: bool = True, **kwargs) -> bool:
        """
        Changes the permission of *path*. Nothing happens when the file system does not support
        permissions.

        :param path: The path.
        :param perm: The new permission. Nothing happens when *None*.
        :param silent: When *True* and *path* does not exist, *False* is returned instead of raising
            an error.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether the permission was changed.
        """
        ...

    @abc.abstractmethod
    def remove(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        *,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Removes the file or directory at *path*.

        :param path: The path.
        :param recursive: Whether directories are removed recursively.
        :param silent: When *True* and *path* does not exist, *False* is returned instead of raising
            an error.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether *path* was removed.
        """
        ...

    @abc.abstractmethod
    def mkdir(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        *,
        perm: int | None = None,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Creates a directory at *path*.

        :param path: The path.
        :param perm: The permission of the directory, defaulting to :py:attr:`default_dir_perm`.
        :param recursive: Whether missing intermediate directories are created as well.
        :param silent: When *True* and *path* already exists, *False* is returned instead of raising
            an error.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether the directory was created.
        """
        ...

    @abc.abstractmethod
    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        pattern: str | None = None,
        type: Literal["f", "d"] | None = None,
        **kwargs,
    ) -> list[str]:
        """
        Returns the base names of all elements in the directory at *path*.

        :param path: The path of the directory.
        :param pattern: Optional glob pattern to filter elements.
        :param type: Optional type to filter elements, either ``"f"`` for files or ``"d"`` for
            directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The list of base names.
        """
        ...

    @abc.abstractmethod
    def walk(
        self,
        path: str | pathlib.Path,
        *,
        max_depth: int = -1,
        **kwargs,
    ) -> Iterator[tuple[str, list[str], list[str], int]]:
        """
        Walks through the directory tree at *path*, similar to :py:func:`os.walk`.

        :param path: The path of the directory.
        :param max_depth: Maximum depth of the recursion. No limit is applied when negative.
        :param kwargs: Additional, implementation-specific options.
        :return: Generator that yields tuples *(directory, dir_names, file_names, depth)*.
        """
        ...

    @abc.abstractmethod
    def glob(
        self,
        pattern: str | pathlib.Path,
        *,
        cwd: str | pathlib.Path | None = None,
        **kwargs,
    ) -> list[str]:
        """
        Returns all paths matching a glob *pattern*.

        :param pattern: The glob pattern.
        :param cwd: When set, *pattern* is interpreted relative to it and the returned paths are
            relative to it as well.
        :param kwargs: Additional, implementation-specific options.
        :return: The list of matching paths.
        """
        ...

    @abc.abstractmethod
    def copy(  # type: ignore[override]
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Copies the file at *src* to *dst*. When *dst* is an existing directory, the file is copied
        into it.

        :param src: The source path.
        :param dst: The destination path.
        :param perm: The permission of the copied file, defaulting to :py:attr:`default_file_perm`.
        :param dir_perm: The permission of directories that are created on the way.
        :param kwargs: Additional, implementation-specific options.
        :return: The full destination path.
        """
        ...

    @abc.abstractmethod
    def move(  # type: ignore[override]
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Moves the file at *src* to *dst*. When *dst* is an existing directory, the file is moved
        into it.

        :param src: The source path.
        :param dst: The destination path.
        :param perm: The permission of the moved file, defaulting to :py:attr:`default_file_perm`.
        :param dir_perm: The permission of directories that are created on the way.
        :param kwargs: Additional, implementation-specific options.
        :return: The full destination path.
        """
        ...

    @abc.abstractmethod
    @contextlib.contextmanager
    def open(
        self,
        path: str | pathlib.Path,
        mode: str,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> Iterator[IO]:
        """
        Opens the file at *path* in *mode*.

        :param path: The path.
        :param mode: The file mode.
        :param perm: The permission of the file when it is written, defaulting to
            :py:attr:`default_file_perm`.
        :param dir_perm: The permission of missing directories that are created when the file is
            written.
        :param kwargs: Additional, implementation-specific options.
        :return: A context manager that yields the file object.
        """
        ...


class FileSystemTarget(Target, shims.FileSystemTarget):
    """
    Abstract base class of targets that refer to a file or directory at *path* within a
    :py:class:`FileSystem` *fs*. Environment variables and ``"~"`` in paths are expanded, while the
    original path is kept in :py:attr:`unexpanded_path`. All *kwargs* are forwarded to
    :py:class:`~law.target.base.Target`.

    .. py:classattribute:: file_class

        type: type

        The class of file targets on the same file system, e.g. used by
        :py:meth:`FileSystemDirectoryTarget.child`.

    .. py:classattribute:: directory_class

        type: type

        The class of directory targets on the same file system, e.g. used by :py:attr:`parent`.
    """

    # must be set by subclasses
    file_class: type[FileSystemFileTarget]
    directory_class: type[FileSystemDirectoryTarget]

    open: Callable | None = None  # type: ignore[assignment]

    def __init__(self, path: str | pathlib.Path, fs: FileSystem | None = None, **kwargs) -> None:
        if fs is not None:
            self.fs: FileSystem = fs  # type: ignore[misc]

        # _path and _unexpanded_path are set during super init through properties below
        self._path: str
        self._unexpanded_path: str

        super().__init__(path=path, **kwargs)

    def _repr_pairs(self, color: bool = True) -> list[tuple[str, Any]]:
        pairs = super()._repr_pairs()

        # add the fs name
        if self.fs:
            pairs.append(("fs", self.fs.name))

        # add the path
        cfg = Config.instance()
        expand = cfg.get_expanded_bool("target", "expand_path_repr")
        pairs.append(("path", self.path if expand else self.unexpanded_path))

        # optionally add the file size
        if cfg.get_expanded_bool("target", "filesize_repr"):
            stat: os.stat_result = self.exists(stat=True)  # type: ignore[assignment]
            pairs.append(("size", human_bytes(stat.st_size, fmt="{:.1f}{}") if stat else "-"))

        return pairs

    def _parent_args(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (), {}

    @property
    def unexpanded_path(self) -> str:
        """
        The path of this target without expanded environment variables and ``"~"``.
        """
        return self._unexpanded_path

    @property
    def path(self) -> str:
        """
        The path of this target with expanded environment variables and ``"~"``. When set, a leading
        file system scheme is removed.
        """
        return self._path

    @path.setter
    def path(self, path: str | pathlib.Path) -> None:
        path = self.fs._unscheme(str(path))
        self._unexpanded_path = path
        self._path = os.path.expandvars(os.path.expanduser(self._unexpanded_path))

    @property
    def dirname(self) -> str | None:
        """
        The directory name of :py:attr:`path`.
        """
        return self.fs.dirname(self.path)

    @property
    def absdirname(self) -> str | None:
        """
        The directory name of :py:attr:`abspath`.
        """
        return self.fs.dirname(self.abspath)

    @property
    def basename(self) -> str:
        """
        The base name of :py:attr:`path`.
        """
        return self.fs.basename(self.path)

    @property
    def unique_basename(self) -> str:
        """
        The base name of :py:attr:`path`, prefixed by a hash of this target, which differs between
        targets with the same base name.
        """
        return f"{hex(self.hash)[2:]}_{self.basename}"

    @property
    def parent(self) -> FileSystemDirectoryTarget | None:
        """
        The directory target that contains this target, or *None* for the root directory.
        Environment variables of the unexpanded path are preserved.
        """
        # get the dirname, but favor the unexpanded one to propagate variables
        dirname = self.dirname
        unexpanded_dirname: str = self.fs.dirname(self.unexpanded_path)  # type: ignore[assignment]
        expanded_dirname = os.path.expandvars(os.path.expanduser(unexpanded_dirname))
        if unexpanded_dirname and dirname and self.fs.abspath(dirname) == self.fs.abspath(expanded_dirname):
            dirname = unexpanded_dirname

        if dirname is None:
            return None

        args, kwargs = self._parent_args()
        return self.directory_class(dirname, *args, **kwargs)

    def sibling(self, *args, **kwargs) -> FileSystemTarget:
        """
        Returns a target in the same directory as this one.

        :param args: Arguments forwarded to :py:meth:`FileSystemDirectoryTarget.child` of
            :py:attr:`parent`.
        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystemDirectoryTarget.child` of
            :py:attr:`parent`.
        :raises ValueError: When this target has no parent.
        :return: The sibling target.
        """
        parent = self.parent
        if not parent:
            raise ValueError(f"cannot determine parent of {self!r}")

        return parent.child(*args, **kwargs)

    def stat(self, **kwargs) -> os.stat_result:
        """
        Returns the stat result of this target.

        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystem.stat`.
        :return: The stat result.
        """
        return self.fs.stat(self.path, **kwargs)

    def exists(self, **kwargs) -> bool | os.stat_result | None:  # type: ignore[override]
        return self.fs.exists(self.path, **kwargs)

    def remove(self, *, silent: bool = True, **kwargs) -> bool:
        return self.fs.remove(self.path, silent=silent, **kwargs)

    def chmod(self, perm, *, silent: bool = False, **kwargs) -> bool:
        """
        Changes the permission of this target. See :py:meth:`FileSystem.chmod` for more info.

        :param perm: The new permission.
        :param silent: When *True* and this target does not exist, *False* is returned instead of
            raising an error.
        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystem.chmod`.
        :return: Whether the permission was changed.
        """
        return self.fs.chmod(self.path, perm, silent=silent, **kwargs)

    def makedirs(self, *args, **kwargs) -> None:
        """
        Creates the parent directory of this target.

        :param args: Arguments forwarded to the :py:meth:`touch` method of the parent.
        :param kwargs: Keyword arguments forwarded to the :py:meth:`touch` method of the parent.
        """
        # overwrites luigi's makedirs method
        parent = self.parent
        if parent:
            parent.touch(*args, **kwargs)

    def _prepare_dir(self, **kwargs) -> None:
        dir_target = self if isinstance(self, self.directory_class) else self.parent
        dir_target.touch(**kwargs)  # type: ignore[union-attr]

    @property
    @abc.abstractmethod
    def fs(self) -> FileSystem:
        ...

    @property
    @abc.abstractmethod
    def abspath(self) -> str:
        """
        The absolute path of this target within its file system, without scheme.
        """
        ...

    @abc.abstractmethod
    def uri(self, *, return_all: bool = False, scheme: bool = True, **kwargs) -> str | list[str]:
        """
        Returns the uri of this target.

        :param return_all: See :py:meth:`Target.uri`.
        :param scheme: Whether the uri is prefixed by the file system scheme.
        :param kwargs: Additional, implementation-specific options.
        :return: The uri, or a list of all uris when *return_all* is *True*.
        """
        ...

    @abc.abstractmethod
    def touch(self, *, perm: int | None = None, dir_perm: int | None = None, **kwargs) -> bool:
        """
        Creates the file or directory this target refers to, including missing intermediate
        directories.

        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: Whether the target was created.
        """
        ...

    @abc.abstractmethod
    def copy_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Copies this target to *dst*. Directories are copied recursively.

        :param dst: The destination path or target. Paths are interpreted by the file system of this
            target, so use :py:meth:`copy_to_local` to copy remote targets to the local file system.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    def copy_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Copies *src* to the location of this target. Directories are copied recursively.

        :param src: The source path or target. Paths are interpreted by the file system of this
            target, so use :py:meth:`copy_from_local` to copy local files to remote targets.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    def move_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Moves this target to *dst*. See :py:meth:`copy_to` for more info.

        :param dst: The destination path or target.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    def move_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Moves *src* to the location of this target. See :py:meth:`copy_from` for more info.

        :param src: The source path or target.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    def copy_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Copies this target to *dst* on the local file system. For local targets, this is identical
        to :py:meth:`copy_to`.

        :param dst: The local destination path or target. Remote targets might also accept *None*,
            in which case the remote file system decides on the destination, e.g. its cache.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The local destination path.
        """
        ...

    @abc.abstractmethod
    def copy_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Copies *src* from the local file system to the location of this target. For local targets,
        this is identical to :py:meth:`copy_from`.

        :param src: The local source path or target.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    def move_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Moves this target to *dst* on the local file system. See :py:meth:`copy_to_local` for more
        info.

        :param dst: The local destination path or target.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The local destination path.
        """
        ...

    @abc.abstractmethod
    def move_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Moves *src* from the local file system to the location of this target. See
        :py:meth:`copy_from_local` for more info.

        :param src: The local source path or target.
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param kwargs: Additional, implementation-specific options.
        :return: The destination path.
        """
        ...

    @abc.abstractmethod
    @contextlib.contextmanager
    def localize(
        self,
        mode: str = "r",
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        tmp_dir: str | pathlib.Path | None = None,
        **kwargs,
    ) -> Iterator[FileSystemTarget]:
        """
        Context manager that yields a local representation of this target, which is helpful for
        tools that can only handle local files.

        In ``"r"`` mode, remote targets are copied to a temporary local target first. In ``"w"`` and
        ``"a"`` modes, a temporary local target is yielded (in ``"a"`` mode containing a copy of the
        existing content) which is copied to the location of this target when the context is left
        without an error. Local targets are yielded as they are, unless *is_tmp* is passed in
        *kwargs* to enforce the use of a temporary copy. Example:

        .. code-block:: python

            with target.localize("w") as tmp:
                some_tool_writing_to(tmp.abspath)

        :param mode: Either ``"r"`` (read), ``"w"`` (write) or ``"a"`` (append).
        :param perm: The permission of created files.
        :param dir_perm: The permission of created directories.
        :param tmp_dir: The directory for temporary targets.
        :param kwargs: Additional, implementation-specific options.
        :return: A context manager that yields the local target.
        """
        ...

    @abc.abstractmethod
    def load(self, *args, **kwargs) -> Any:
        """
        Loads the content of this target via a :py:class:`~law.target.formatter.Formatter`. Remote
        targets are localized first.

        :param args: Arguments forwarded to the ``load`` method of the formatter.
        :param kwargs: Keyword arguments forwarded to the ``load`` method of the formatter. A
            *formatter* keyword argument selects the formatter by name, which is otherwise
            determined by the file extension.
        :return: The loaded content.
        """
        ...

    @abc.abstractmethod
    def dump(self, *args, **kwargs) -> Any:
        """
        Dumps content into this target via a :py:class:`~law.target.formatter.Formatter`. Remote
        targets are localized first.

        :param args: Arguments forwarded to the ``dump`` method of the formatter.
        :param kwargs: Keyword arguments forwarded to the ``dump`` method of the formatter. A
            *formatter* keyword argument selects the formatter by name, which is otherwise
            determined by the file extension. *perm* and *dir_perm* set the permissions of the file
            and missing directories.
        :return: The return value of the ``dump`` method of the formatter.
        """
        ...


class FileSystemFileTarget(FileSystemTarget):
    """
    Abstract base class of targets that refer to files.
    """

    type: str = "f"

    def ext(self, n: int = 1) -> str:
        """
        Returns the file extension of this target, see :py:meth:`FileSystem.ext`.

        :param n: Number of trailing dot-separated parts to return. All parts are returned when zero
            or negative.
        :return: The extension without the leading dot.
        """
        return self.fs.ext(self.path, n=n)

    def open(self, mode: str, **kwargs) -> AbstractContextManager[IO]:
        return self.fs.open(self.path, mode, **kwargs)

    def touch(self, **kwargs) -> bool:
        """
        Creates an empty file at the location of this target, including missing directories. Note
        that the content of existing files is removed.

        :param kwargs: Keyword arguments forwarded to :py:meth:`open`.
        :return: Whether the file was created.
        """
        # create the file via open without content
        with self.open("w", **kwargs) as f:
            f.write("")
        return True

    def copy_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        if isinstance(dst, FileSystemTarget):
            dst._prepare_dir(perm=dir_perm, **kwargs)

        # TODO: complain when dst not local? forward to copy_from request depending on protocol?
        return self.fs.copy(self.path, get_path(dst), perm=perm, **kwargs)

    def copy_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        self._prepare_dir(perm=dir_perm, **kwargs)

        if isinstance(src, FileSystemFileTarget):
            return src.copy_to(self.abspath, perm=perm or self.fs.default_file_perm, **kwargs)

        # TODO: complain when src not local? forward to copy_to request depending on protocol?
        # when src is a plain string, let the fs handle it
        return self.fs.copy(get_path(src), self.path, perm=perm, **kwargs)

    def move_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        if isinstance(dst, FileSystemTarget):
            dst._prepare_dir(perm=dir_perm, **kwargs)

        # TODO: complain when dst not local? forward to copy_from request depending on protocol?
        return self.fs.move(self.path, get_path(dst), perm=perm, **kwargs)

    def move_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        self._prepare_dir(perm=dir_perm, **kwargs)

        if isinstance(src, FileSystemFileTarget):
            return src.move_to(self.abspath, perm=perm or self.fs.default_file_perm, **kwargs)

        # when src is a plain string, let the fs handle it
        # TODO: complain when src not local? forward to copy_to request depending on protocol?
        return self.fs.move(get_path(src), self.path, perm=perm, **kwargs)


class FileSystemDirectoryTarget(FileSystemTarget):
    """
    Abstract base class of targets that refer to directories.
    """

    type = "d"

    open = None

    def _child_args(
        self,
        path: str | pathlib.Path,
        type: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (), {}

    @overload
    def child(
        self,
        path: str | pathlib.Path,
        type: Literal["f"],
        *,
        mktemp_pattern: str | None = None,
        **kwargs,
    ) -> FileSystemFileTarget: ...

    @overload
    def child(
        self,
        path: str | pathlib.Path,
        type: Literal["d"],
        *,
        mktemp_pattern: str | None = None,
        **kwargs,
    ) -> Self: ...

    @overload
    def child(
        self,
        path: str | pathlib.Path,
        type: str | None = None,
        *,
        mktemp_pattern: str | None = None,
        **kwargs,
    ) -> FileSystemTarget: ...

    def child(
        self,
        path: str | pathlib.Path,
        type: str | None = None,
        *,
        mktemp_pattern: str | None = None,
        **kwargs,
    ) -> FileSystemTarget:
        """
        Returns a target for *path* relative to this directory. Example:

        .. code-block:: python

            d = LocalDirectoryTarget("/path/to/dir")
            d.child("data.json", type="f")  # -> LocalFileTarget("/path/to/dir/data.json")
            d.child("plots", type="d")      # -> LocalDirectoryTarget("/path/to/dir/plots")

        :param path: The path relative to this directory.
        :param type: Either ``"f"`` for a file target (:py:attr:`file_class`) or ``"d"`` for a
            directory target. When *None*, the type is determined from the existing path.
        :param mktemp_pattern: When set, sequences of at least three ``"X"`` in *path* are replaced
            by random characters, similar to ``mktemp``.
        :param kwargs: Keyword arguments forwarded to the constructor of the target.
        :raises FileNotFoundError: When *type* is *None* and the path does not exist.
        :raises ValueError: When *type* is invalid.
        :return: The child target.
        """
        if type not in (None, "f", "d"):
            raise ValueError("invalid child type, use 'f' or 'd'")

        # apply mktemp's feature to replace at least three consecutive 'X' with random characters
        path = get_path(path)
        if mktemp_pattern and "XXX" in path:
            repl = lambda m: create_random_string(len(m.group(1)))
            path = re.sub(r"(X{3,})", repl, path)

        unexpanded_path = os.path.join(self.unexpanded_path, path)
        path = os.path.join(self.path, path)
        if type == "f":
            cls = self.file_class
        elif type == "d":
            cls = self.__class__  # type: ignore[assignment]
        elif not self.fs.exists(path):
            raise FileNotFoundError(f"cannot guess type of non-existing path '{path}'")
        elif self.fs.isdir(path):
            cls = self.__class__  # type: ignore[assignment]
            type = "d"
        else:
            cls = self.file_class
            type = "f"

        args, _kwargs = self._child_args(path, type)
        _kwargs.update(kwargs)

        return cls(unexpanded_path, *args, **_kwargs)

    def listdir(self, **kwargs) -> list[str]:
        """
        Returns the base names of all elements in this directory.

        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystem.listdir`.
        :return: The list of base names.
        """
        return self.fs.listdir(self.path, **kwargs)

    def glob(self, pattern: str | pathlib.Path, **kwargs) -> list[str]:
        """
        Returns the paths of all elements matching a glob *pattern*.

        :param pattern: The glob pattern, relative to this directory.
        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystem.glob`.
        :return: The list of matching paths, relative to this directory.
        """
        return self.fs.glob(pattern, cwd=self.path, **kwargs)

    def walk(self, **kwargs) -> Iterator[tuple[str, list[str], list[str], int]]:
        """
        Walks through this directory, see :py:meth:`FileSystem.walk`.

        :param kwargs: Keyword arguments forwarded to :py:meth:`FileSystem.walk`.
        :return: Generator that yields tuples *(directory, dir_names, file_names, depth)*.
        """
        return self.fs.walk(self.path, **kwargs)

    def touch(self, **kwargs) -> bool:
        kwargs.setdefault("silent", True)
        return self.fs.mkdir(self.path, **kwargs)

    def copy_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        # create the target dir
        _dst = get_path(dst)
        if isinstance(dst, FileSystemDirectoryTarget):
            dst.touch(perm=dir_perm, **kwargs)
        else:
            # TODO: complain when dst not local? forward to copy_from request depending on protocol?
            self.fs.mkdir(_dst, perm=dir_perm, **kwargs)

        # walk and operate recursively
        for _, dirs, files, _ in self.walk(max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), (files, "f")]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)
                    t.copy_to(os.path.join(_dst, basename), perm=perm, dir_perm=dir_perm, **kwargs)

        return _dst

    def copy_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        # when src is a directory target itself, forward to its copy_to implementation as it might
        # be more performant to use its own directory walking
        if isinstance(src, FileSystemDirectoryTarget):
            return src.copy_to(self, perm=perm, dir_perm=dir_perm, **kwargs)

        # create the target dir
        self.touch(perm=dir_perm, **kwargs)

        # when src is a plain string, let the fs handle it
        # walk and operate recursively
        # TODO: complain when src not local? forward to copy_from request depending on protocol?
        _src = get_path(src)
        for _, dirs, files, _ in self.fs.walk(_src, max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), (files, "f")]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)
                    t.copy_from(os.path.join(_src, basename), perm=perm, dir_perm=dir_perm, **kwargs)

        return self.abspath

    def move_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        # create the target dir
        _dst = get_path(dst)
        if isinstance(dst, FileSystemDirectoryTarget):
            dst.touch(perm=dir_perm, **kwargs)
        else:
            # TODO: complain when dst not local? forward to copy_from request depending on protocol?
            self.fs.mkdir(_dst, perm=dir_perm, **kwargs)

        # walk and operate recursively
        for _, dirs, files, _ in self.walk(max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), (files, "f")]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)
                    t.move_to(os.path.join(_dst, basename), perm=perm, dir_perm=dir_perm, **kwargs)

        # finally remove
        self.remove()

        return _dst

    def move_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        # when src is a directory target itself, forward to its move_to implementation as it might
        # be more performant to use its own directory walking
        if isinstance(src, FileSystemDirectoryTarget):
            return src.move_to(self, perm=perm, dir_perm=dir_perm, **kwargs)

        # create the target dir
        self.touch(perm=dir_perm, **kwargs)

        # when src is a plain string, let the fs handle it
        # walk and operate recursively
        # TODO: complain when src not local? forward to copy_from request depending on protocol?
        _src = get_path(src)
        for _, dirs, files, _ in self.fs.walk(_src, max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), (files, "f")]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)
                    t.copy_from(os.path.join(_src, basename), perm=perm, dir_perm=dir_perm, **kwargs)

        # finally remove
        self.fs.remove(_src)

        return self.abspath


def get_path(target: T) -> str:
    """
    Returns the path of *target*.

    :param target: A file system target, an object with a *path* attribute, a string or a
        :py:class:`pathlib.Path`.
    :raises TypeError: When the path cannot be determined.
    :return: The absolute path of file system targets, the *path* attribute of other objects, or the
        string representation of strings and :py:class:`pathlib.Path` objects.
    """
    # file targets
    if isinstance(target, FileSystemTarget):
        path = getattr(target, "abspath", no_value)
        if path != no_value:
            return str(path)

    # objects that have a "path" attribute
    path = getattr(target, "path", no_value)
    if path != no_value:
        return str(path)

    # strings and paths
    if isinstance(target, (str, pathlib.Path)):
        return str(target)

    raise TypeError(f"cannot get path from {target!r}")


def get_scheme(uri: str | pathlib.Path) -> str | None:
    """
    Returns the scheme of *uri*, e.g. ``"root"`` for ``"root://host//path"``.

    :param uri: The uri.
    :return: The scheme, or *None* when *uri* has no scheme.
    """
    # ftp://path/to/file -> ftp
    # /path/to/file -> None
    m = re.match(r"^(\w+)\:\/\/.*$", str(uri))
    return m.group(1) if m else None


def has_scheme(uri: str | pathlib.Path) -> bool:
    """
    Returns whether *uri* has a scheme.

    :param uri: The uri.
    :return: Whether *uri* has a scheme.
    """
    return get_scheme(uri) is not None


def add_scheme(path: str | pathlib.Path, scheme: str) -> str:
    """
    Adds *scheme* to *path* unless it already has one, e.g. ``"file"`` and ``"/path"`` result in
    ``"file:///path"``.

    :param path: The path.
    :param scheme: The scheme to add.
    :return: The uri.
    """
    # adds a scheme to a path, if it does not already contain one
    path = str(path)
    if has_scheme(path):
        return path
    return f"{scheme.rstrip(':/')}://{path}"


def remove_scheme(uri: str | pathlib.Path) -> str:
    """
    Removes the scheme from *uri*, e.g. ``"file:///path"`` results in ``"/path"``.

    :param uri: The uri.
    :return: The uri without scheme.
    """
    # ftp://path/to/file -> /path/to/file
    # /path/to/file -> /path/to/file
    return re.sub(r"^(\w+\:\/\/)", "", str(uri))


@contextlib.contextmanager
def localize_file_targets(struct, *args, **kwargs) -> Generator[Any, None, None]:
    """
    Context manager that takes an arbitrary *struct* of targets, opens the contexts returned by
    their :py:meth:`FileSystemFileTarget.localize` implementations and yields their localized
    representations in the same structure. When the context is closed, the contexts of all localized
    targets are closed.

    :param struct: The structure of targets. Objects without a ``localize`` method are passed
        through.
    :param args: Arguments forwarded to the ``localize`` method of each target.
    :param kwargs: Keyword arguments forwarded to the ``localize`` method of each target.
    :raises Exception: The first exception raised while closing the localized contexts, given that
        no exception occurred within the context itself.
    :return: A context manager that yields the structure of localized targets.
    """
    managers = []

    def enter(target):
        if callable(getattr(target, "localize", None)):
            manager = target.localize(*args, **kwargs)
            managers.append(manager)
            return manager.__enter__()  # noqa: PLC2801

        return target

    # localize all targets, maintain the structure
    localized_targets = map_struct(enter, struct)

    # prepare exception info
    exc = None
    exc_info = (None, None, None)

    try:
        yield localized_targets

    except (Exception, KeyboardInterrupt) as e:
        exc = e
        exc_info = sys.exc_info()  # type: ignore[assignment]
        raise

    finally:
        exit_exc = []
        for manager in managers:
            try:
                manager.__exit__(*exc_info)
            except Exception as e:
                exit_exc.append(e)

        # when there was no exception during the actual yield and
        # an exception occured in one of the exit methods, raise the first one
        if not exc and exit_exc:
            raise exit_exc[0]
