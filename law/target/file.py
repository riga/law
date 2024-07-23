# coding: utf-8

"""
Custom luigi file system and target objects.
"""

from __future__ import annotations

__all__ = [
    "FileSystem", "FileSystemTarget", "FileSystemFileTarget", "FileSystemDirectoryTarget",
    "get_path", "get_scheme", "has_scheme", "add_scheme", "remove_scheme", "localize_file_targets",
]

import os
import sys
import re
import pathlib
import contextlib
from abc import abstractmethod, abstractproperty
from functools import partial

from law.config import Config
import law.target.luigi_shims as shims
from law.target.base import Target
from law.util import map_struct, create_random_string, human_bytes, no_value
from law._types import (
    Any, Generator, Callable, T, Literal, Iterator, Type, AbstractContextManager, IO,
)


class FileSystem(shims.FileSystem):

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
        int_or_none = partial(cfg.get_expanded_int, default=None)
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
        return os.path.dirname(str(path)) if path != "/" else None

    def basename(self, path: str | pathlib.Path) -> str:
        return os.path.basename(str(path)) if path != "/" else "/"

    def ext(self, path: str | pathlib.Path, n: int = 1) -> str:
        # split the path
        parts = self.basename(path).lstrip(".").split(".")

        # empty extension in the trivial case or use the last n parts except for the first one
        return "" if len(parts) == 1 else ".".join(parts[1:][min(-n, 0):])

    def _unscheme(self, path: str | pathlib.Path) -> str:
        return remove_scheme(path)

    @abstractproperty
    def default_instance(self) -> FileSystem:
        ...

    @abstractmethod
    def abspath(self, path: str | pathlib.Path) -> str:
        ...

    @abstractmethod
    def stat(self, path: str | pathlib.Path, **kwargs) -> os.stat_result:
        ...

    @abstractmethod
    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        **kwargs,
    ) -> bool | os.stat_result | None:
        ...

    @abstractmethod
    def isdir(self, path: str | pathlib.Path, **kwargs) -> bool:
        ...

    @abstractmethod
    def isfile(self, path: str | pathlib.Path, **kwargs) -> bool:
        ...

    @abstractmethod
    def chmod(self, path: str | pathlib.Path, perm: int, *, silent: bool = True, **kwargs) -> bool:
        ...

    @abstractmethod
    def remove(
        self,
        path: str | pathlib.Path,
        *,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        ...

    @abstractmethod
    def mkdir(
        self,
        path: str | pathlib.Path,
        *,
        perm: int | None = None,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        ...

    @abstractmethod
    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        pattern: str | None = None,
        type: Literal["f", "d"] | None = None,
        **kwargs,
    ) -> list[str]:
        ...

    @abstractmethod
    def walk(
        self,
        path: str | pathlib.Path,
        *,
        max_depth: int = -1,
        **kwargs,
    ) -> Iterator[tuple[str, list[str], list[str], int]]:
        ...

    @abstractmethod
    def glob(
        self,
        pattern: str | pathlib.Path,
        *,
        cwd: str | pathlib.Path | None = None,
        **kwargs,
    ) -> list[str]:
        ...

    @abstractmethod
    def copy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def move(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def open(
        self,
        path: str | pathlib.Path,
        mode: str,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> AbstractContextManager[IO]:
        ...


class FileSystemTarget(Target, shims.FileSystemTarget):

    # must be set by subclasses
    file_class: Type[FileSystemFileTarget]
    directory_class: Type[FileSystemDirectoryTarget]

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
        return self._unexpanded_path

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str | pathlib.Path) -> None:
        path = self.fs._unscheme(str(path))
        self._unexpanded_path = path
        self._path = os.path.expandvars(os.path.expanduser(self._unexpanded_path))

    @property
    def dirname(self) -> str:
        return self.fs.dirname(self.path)  # type: ignore[return-value]

    @property
    def absdirname(self) -> str:
        return self.fs.dirname(self.abspath)  # type: ignore[return-value]

    @property
    def basename(self) -> str:
        return self.fs.basename(self.path)

    @property
    def unique_basename(self) -> str:
        return f"{hex(self.hash)[2:]}_{self.basename}"

    @property
    def parent(self) -> Type[FileSystemDirectoryTarget] | None:
        # get the dirname, but favor the unexpanded one to propagate variables
        dirname = self.dirname
        unexpanded_dirname: str = self.fs.dirname(self.unexpanded_path)  # type: ignore[assignment]
        expanded_dirname = os.path.expandvars(os.path.expanduser(unexpanded_dirname))
        if unexpanded_dirname and self.fs.abspath(dirname) == self.fs.abspath(expanded_dirname):
            dirname = unexpanded_dirname

        if dirname is None:
            return None

        args, kwargs = self._parent_args()
        return self.directory_class(dirname, *args, **kwargs)

    def sibling(self, *args, **kwargs) -> FileSystemTarget:
        parent = self.parent
        if not parent:
            raise Exception(f"cannot determine parent of {self!r}")

        return parent.child(*args, **kwargs)

    def stat(self, **kwargs) -> os.stat_result:
        return self.fs.stat(self.path, **kwargs)

    def exists(self, **kwargs) -> bool | os.stat_result | None:  # type: ignore[override]
        return self.fs.exists(self.path, **kwargs)

    def remove(self, *, silent: bool = True, **kwargs) -> bool:  # type: ignore[override]
        return self.fs.remove(self.path, silent=silent, **kwargs)

    def chmod(self, perm, *, silent: bool = False, **kwargs) -> bool:
        return self.fs.chmod(self.path, perm, silent=silent, **kwargs)

    def makedirs(self, *args, **kwargs) -> None:
        parent = self.parent
        if parent:
            parent.touch(*args, **kwargs)

    @abstractproperty
    def fs(self) -> FileSystem:
        ...

    @abstractproperty
    def abspath(self) -> str:
        ...

    @abstractmethod
    def uri(self, *, return_all: bool = False, scheme: bool = True, **kwargs) -> str | list[str]:
        ...

    @abstractmethod
    def touch(self, *, perm: int | None = None, dir_perm: int | None = None, **kwargs) -> bool:
        ...

    @abstractmethod
    def copy_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def copy_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def move_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def move_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def copy_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def copy_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def move_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    def move_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        ...

    @abstractmethod
    @contextlib.contextmanager
    def localize(
        self,
        mode: str = "r",
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        tmp_dir: str | pathlib.Path | None = None,
        **kwargs,
    ) -> Generator[FileSystemTarget, None, None]:
        ...

    @abstractmethod
    def load(self, *args, **kwargs) -> Any:
        ...

    @abstractmethod
    def dump(self, *args, **kwargs) -> Any:
        ...


class FileSystemFileTarget(FileSystemTarget):

    type = "f"

    def ext(self, n: int = 1) -> str:
        return self.fs.ext(self.path, n=n)

    def open(self, mode: str, **kwargs) -> AbstractContextManager[IO]:
        return self.fs.open(self.path, mode, **kwargs)

    def touch(self, **kwargs) -> bool:
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
        # TODO: complain when dst not local? forward to copy_from request depending on protocol?
        return self.fs.copy(self.path, get_path(dst), perm=perm, dir_perm=dir_perm, **kwargs)

    def copy_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        if isinstance(src, FileSystemFileTarget):
            return src.copy_to(self.abspath, perm=perm, dir_perm=dir_perm, **kwargs)

        # when src is a plain string, let the fs handle it
        # TODO: complain when src not local? forward to copy_to request depending on protocol?
        return self.fs.copy(get_path(src), self.path, perm=perm, dir_perm=dir_perm, **kwargs)

    def move_to(
        self,
        dst: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        # TODO: complain when dst not local? forward to copy_from request depending on protocol?
        return self.fs.move(self.path, get_path(dst), perm=perm, dir_perm=dir_perm, **kwargs)

    def move_from(
        self,
        src: str | pathlib.Path | FileSystemTarget,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        if isinstance(src, FileSystemFileTarget):
            return src.move_to(self.abspath, perm=perm, dir_perm=dir_perm, **kwargs)

        # when src is a plain string, let the fs handle it
        # TODO: complain when src not local? forward to copy_to request depending on protocol?
        return self.fs.move(get_path(src), self.path, perm=perm, dir_perm=dir_perm, **kwargs)


class FileSystemDirectoryTarget(FileSystemTarget):

    type = "d"

    open = None

    def _child_args(
        self,
        path: str | pathlib.Path,
        type: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        return (), {}

    def child(
        self,
        path: str | pathlib.Path,
        type: Literal["f", "d"] | None = None,
        *,
        mktemp_pattern: str | None = None,
        **kwargs,
    ) -> FileSystemTarget:
        if type not in (None, "f", "d"):
            raise ValueError("invalid child type, use 'f' or 'd'")

        # apply mktemp's feature to replace at least three consecutive 'X' with random characters
        path = get_path(path)
        if mktemp_pattern and "XXX" in path:
            repl = lambda m: create_random_string(l=len(m.group(1)))
            path = re.sub("(X{3,})", repl, path)

        unexpanded_path = os.path.join(self.unexpanded_path, path)
        path = os.path.join(self.path, path)
        if type == "f":
            cls = self.file_class
        elif type == "d":
            cls = self.__class__
        elif not self.fs.exists(path):
            raise Exception(f"cannot guess type of non-existing path '{path}'")
        elif self.fs.isdir(path):
            cls = self.__class__
            type = "d"
        else:
            cls = self.file_class
            type = "f"

        args, _kwargs = self._child_args(path, type)
        _kwargs.update(kwargs)

        return cls(unexpanded_path, *args, **_kwargs)

    def listdir(self, **kwargs) -> list[str]:
        return self.fs.listdir(self.path, **kwargs)

    def glob(self, pattern: str | pathlib.Path, **kwargs) -> list[str]:
        return self.fs.glob(pattern, cwd=self.path, **kwargs)

    def walk(self, **kwargs) -> Iterator[tuple[str, list[str], list[str], int]]:
        return self.fs.walk(self.path, **kwargs)

    def touch(self, **kwargs) -> bool:  # type: ignore[override]
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
        for path, dirs, files, _ in self.walk(max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), [files, "f"]]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)  # type: ignore[arg-type]
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
        for path, dirs, files, _ in self.fs.walk(_src, max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), [files, "f"]]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)  # type: ignore[arg-type]
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
        for path, dirs, files, _ in self.walk(max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), [files, "f"]]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)  # type: ignore[arg-type]
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
        for path, dirs, files, _ in self.fs.walk(_src, max_depth=0, **kwargs):
            # recurse through directories and files
            for basenames, type_flag in [(dirs, "d"), [files, "f"]]:
                for basename in basenames:
                    t = self.child(basename, type=type_flag)  # type: ignore[arg-type]
                    t.copy_from(os.path.join(_src, basename), perm=perm, dir_perm=dir_perm, **kwargs)

        # finally remove
        self.fs.remove(_src)

        return self.abspath


def get_path(target: T) -> str:
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
    # ftp://path/to/file -> ftp
    # /path/to/file -> None
    m = re.match(r"^(\w+)\:\/\/.*$", str(uri))
    return m.group(1) if m else None


def has_scheme(uri: str | pathlib.Path) -> bool:
    return get_scheme(uri) is not None


def add_scheme(path: str | pathlib.Path, scheme: str) -> str:
    # adds a scheme to a path, if it does not already contain one
    path = str(path)
    if has_scheme(path):
        return path
    return f"{scheme.rstrip(':/')}://{path}"


def remove_scheme(uri: str | pathlib.Path) -> str:
    # ftp://path/to/file -> /path/to/file
    # /path/to/file -> /path/to/file
    return re.sub(r"^(\w+\:\/\/)", "", str(uri))


@contextlib.contextmanager
def localize_file_targets(struct, *args, **kwargs) -> Generator[Any, None, None]:
    """
    Takes an arbitrary *struct* of targets, opens the contexts returned by their
    :py:meth:`FileSystemFileTarget.localize` implementations and yields their localized
    representations in the same structure as passed in *struct*. When the context is closed, the
    contexts of all localized targets are closed.
    """
    managers = []

    def enter(target):
        if callable(getattr(target, "localize", None)):
            manager = target.localize(*args, **kwargs)
            managers.append(manager)
            return manager.__enter__()

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
