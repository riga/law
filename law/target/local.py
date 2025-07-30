# coding: utf-8

"""
Local target implementations.
"""

from __future__ import annotations

__all__ = ["LocalFileSystem", "LocalTarget", "LocalFileTarget", "LocalDirectoryTarget"]

import os
import fnmatch
import shutil
import pathlib
import glob
import random
import contextlib

from law.config import Config
import law.target.luigi_shims as shims
from law.target.file import (
    FileSystem, FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, get_path,
    get_scheme, add_scheme, remove_scheme,
)
from law.target.formatter import AUTO_FORMATTER, find_formatter
from law.logger import get_logger
from law._types import Any, Callable, Literal, Iterator, AbstractContextManager, IO, Generator


logger = get_logger(__name__)


class LocalFileSystem(FileSystem, shims.LocalFileSystem):

    # set right below the class definition
    default_instance: LocalFileSystem = None  # type: ignore[assignment]

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        config = super().parse_config(section, config=config, overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option: str, func: Callable[[str, str], Any]) -> None:
            if option not in config or overwrite:
                config[option] = func(section, option)

        # default base path
        add("base", cfg.get_expanded)

        # number of fragments of the absolute local path to check when used in a MirroredTarget
        add("local_root_depth", cfg.get_expanded_int)

        return config

    def __init__(self, section: str | None = None, *, base: str | None = None, **kwargs) -> None:
        # setting both section and base is ambiguous and not allowed
        if section and base:
            raise Exception(
                f"setting both 'section' and 'base' as {self.__class__.__name__} arguments is "
                f"ambiguous and therefore not supported, but got {section} and {base}",
            )

        # determine the configured default local fs section
        cfg = Config.instance()
        default_section = cfg.get_expanded("target", "default_local_fs")
        self.config_section: str | None = None

        # determine the correct config section
        if not section:
            # use the default section when none is set
            section = default_section
        elif section != default_section:
            # check if the section exists
            if not cfg.has_section(section):
                raise Exception(
                    f"law config has no section '{section}' to read {self.__class__.__name__} "
                    "options",
                )
            # extend non-default sections by options of the default one
            data = dict(cfg.items(default_section, expand_vars=False, expand_user=False))
            cfg.update({section: data}, overwrite_sections=True, overwrite_options=False)
        self.config_section = section

        # parse the config
        kwargs = self.parse_config(self.config_section, kwargs)  # type: ignore[arg-type]
        kwargs.setdefault("name", self.config_section)

        # when no base is given, use the config value
        if not base:
            base = kwargs.pop("base", None) or os.sep
            # special case: the default local fs is not allowed to have a base directory other than
            # "/" to ensure that files and directories wrapped by local targets in law or derived
            # projects for convenience are interpreted as such and in particular do not resolve them
            # relative to a base path defined in some config
            if self.config_section == default_section and base != os.sep:
                raise Exception(
                    f"the default local fs '{default_section}' must not have a base defined, "
                    f"but got {base}",
                )

        # store attributes
        self.base = os.path.abspath(self._unscheme(str(base)))
        self.local_root_depth: int = kwargs["local_root_depth"]

        super().__init__(**kwargs)

    def _unscheme(self, path: str | os.PathLike) -> str:
        path = str(path)
        return remove_scheme(path) if get_scheme(path) == "file" else path

    def abspath(self, path: str | pathlib.Path) -> str:
        path = os.path.expandvars(os.path.expanduser(self._unscheme(path)))

        # join with the base path
        base = os.path.expandvars(os.path.expanduser(str(self.base)))
        path = os.path.join(base, path)

        return os.path.abspath(path)

    def stat(self, path: str | pathlib.Path, **kwargs) -> os.stat_result:
        return os.stat(self.abspath(path))

    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        **kwargs,
    ) -> bool | os.stat_result | None:
        exists = os.path.exists(self.abspath(path))
        return (self.stat(path, **kwargs) if exists else None) if stat else exists

    def isdir(self, path: str | pathlib.Path, **kwargs) -> bool:
        return os.path.isdir(self.abspath(path))

    def isfile(self, path: str | pathlib.Path, **kwargs) -> bool:
        return os.path.isfile(self.abspath(path))

    def chmod(self, path: str | pathlib.Path, perm: int, *, silent: bool = True, **kwargs) -> bool:
        if not self.has_permissions or perm is None:
            return True

        if silent and not self.exists(path):
            return False

        os.chmod(self.abspath(path), perm)

        return True

    def remove(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        *,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        abspath = self.abspath(path)

        if silent and not self.exists(path):
            return False

        if self.isdir(path):
            if recursive:
                shutil.rmtree(abspath)
            else:
                os.rmdir(abspath)
        else:
            os.remove(abspath)

        return True

    def mkdir(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        *,
        perm: int | None = None,
        recursive: bool = True,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        if silent and self.exists(path):
            return False

        if perm is None:
            perm: int = self.default_dir_perm

        # prepare arguments passed to makedirs or mkdir
        args: tuple[Any, ...] = (self.abspath(path),)
        if perm is not None:
            args += (perm,)

        # the mode passed to os.mkdir or os.makedirs is ignored on some systems, so the strategy
        # here is to disable the process' current umask, create the directories and use chmod again
        orig = os.umask(0o0770 - perm) if perm is not None else None
        func = os.makedirs if recursive else os.mkdir
        try:
            try:
                func(*args)
            except Exception as e:
                if not silent or not isinstance(e, FileExistsError):
                    raise
            self.chmod(path, perm)  # type: ignore[arg-type]
        finally:
            if orig is not None:
                os.umask(orig)

        return True

    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        pattern: str | None = None,
        type: Literal["f", "d"] | None = None,
        **kwargs,
    ) -> list[str]:
        abspath = self.abspath(path)
        elems = os.listdir(abspath)

        # apply pattern filter
        if pattern is not None:
            elems = fnmatch.filter(elems, pattern)

        # apply type filter
        if type == "f":
            elems = [e for e in elems if not self.isdir(os.path.join(path, e))]
        elif type == "d":
            elems = [e for e in elems if self.isdir(os.path.join(path, e))]

        return elems

    def walk(
        self,
        path: str | pathlib.Path,
        *,
        max_depth: int = -1,
        **kwargs,
    ) -> Iterator[tuple[str, list[str], list[str], int]]:
        # mimic os.walk with a max_depth and yield the current depth
        search_dirs = [(str(path), 0)]
        while search_dirs:
            (search_dir, depth) = search_dirs.pop(0)

            # check depth
            if max_depth >= 0 and depth > max_depth:
                continue

            # find dirs and files
            dirs = []
            files = []
            for elem in self.listdir(search_dir):
                if self.isdir(os.path.join(search_dir, elem)):
                    dirs.append(elem)
                else:
                    files.append(elem)

            # yield everything
            yield (self.abspath(search_dir), dirs, files, depth)

            # use dirs to update search dirs
            search_dirs.extend((os.path.join(search_dir, d), depth + 1) for d in dirs)

    def glob(
        self,
        pattern: str | pathlib.Path,
        *,
        cwd: str | pathlib.Path | None = None,
        **kwargs,
    ) -> list[str]:
        if cwd is None:
            pattern = self.abspath(pattern)
        else:
            cwd = self.abspath(cwd)
            pattern = os.path.join(cwd, pattern)

        elems = glob.glob(pattern)

        # cut the cwd if there was any
        if cwd is not None:
            elems = [os.path.relpath(e, cwd) for e in elems]

        return elems

    def _prepare_dst_dir(
        self,
        dst: str | pathlib.Path,
        *,
        src: str | pathlib.Path | None = None,
        perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Prepares the directory of a target located at *dst* for copying and returns its full
        location as specified below. *src* can be the location of a source file target, which is
        (e.g.) used by a file copy or move operation. When *dst* is already a directory, calling
        this method has no effect and the *dst* path is returned, optionally joined with the
        basename of *src*. When *dst* is a file, the absolute *dst* path is returned. Otherwise,
        when *dst* does not exist yet, it is interpreted as a file path and missing directories are
        created when :py:attr:`create_file_dir` is *True*, using *perm* to set the directory
        permission. The absolute path to *dst* is returned.
        """
        dst = str(dst)
        if src is not None:
            src = str(src)

        if self.isdir(dst):
            full_dst = os.path.join(dst, os.path.basename(src)) if src else dst

        elif self.isfile(dst):
            full_dst = dst

        else:
            # interpret dst as a file name, create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and self.create_file_dir and not self.isdir(dst_dir):
                self.mkdir(dst_dir, perm=perm, recursive=True)
            full_dst = dst

        return full_dst

    def copy(  # type: ignore[override]
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm=None,
        dir_perm=None,
        **kwargs,
    ):
        dst = self._prepare_dst_dir(dst, src=src, perm=dir_perm)

        # copy the file
        shutil.copy2(self.abspath(src), self.abspath(dst))

        # set permissions
        if perm is None:
            perm = self.default_file_perm
        self.chmod(dst, perm)

        return dst

    def move(  # type: ignore[override]
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        dst = self._prepare_dst_dir(dst, src=src, perm=dir_perm)

        # move the file
        shutil.move(self.abspath(src), self.abspath(dst))

        # set permissions
        if perm is None:
            perm = self.default_file_perm
        self.chmod(dst, perm)  # type: ignore[arg-type]

        return dst

    def open(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        mode: str,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> AbstractContextManager[IO]:
        abspath = self.abspath(path)

        # some preparations in case the file is written or updated
        # check if the file is only read
        if not mode.startswith("r"):
            # prepare the destination directory
            self._prepare_dst_dir(path, perm=dir_perm)

            if perm is None:
                perm = self.default_file_perm

            # when setting permissions, ensure the file exists first
            if perm is not None and self.has_permissions:
                open(abspath, mode).close()
                self.chmod(path, perm)

        return open(abspath, mode)


LocalFileSystem.default_instance = LocalFileSystem()


class LocalTarget(FileSystemTarget, shims.LocalTarget):

    fs = LocalFileSystem.default_instance  # type: ignore[assignment]

    def __init__(
        self,
        path: str | pathlib.Path | None = None,
        fs: LocalFileSystem = LocalFileSystem.default_instance,
        *,
        is_tmp: bool | str = False,
        tmp_dir: str | pathlib.Path | LocalDirectoryTarget | None = None,
        **kwargs,
    ) -> None:
        if isinstance(fs, str):
            fs = LocalFileSystem(fs)

        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("when no target path is defined, is_tmp must be set")
            if str(fs.base) != "/":
                raise Exception(
                    "when is_tmp is set, the base of the underlying file system must be '/', but "
                    f"found '{fs.base}'",
                )

            # if not set, get the tmp dir from the config and ensure that it exists
            cfg = Config.instance()
            _tmp_dir = (
                get_path(tmp_dir)
                if tmp_dir
                else os.path.realpath(cfg.get_expanded("target", "tmp_dir"))
            )
            if not fs.exists(_tmp_dir):
                perm = cfg.get_expanded_int("target", "tmp_dir_perm")
                fs.mkdir(_tmp_dir, perm=perm)

            # create a random path
            while True:
                basename = "luigi-tmp-{:09d}".format(random.randint(0, 999999999))
                path = os.path.join(_tmp_dir, basename)
                if not fs.exists(path):
                    break

            # is_tmp might be a file extension
            if isinstance(is_tmp, str):
                if is_tmp[0] != ".":
                    is_tmp = "." + is_tmp
                path += is_tmp

        # ensure path is not a target and has no scheme
        path = fs._unscheme(get_path(path))

        super().__init__(path=path, is_tmp=is_tmp, fs=fs, **kwargs)

    def __del__(self) -> None:
        # when loosing all references during shutdown, os.path or os.path.exists might be unset
        if getattr(os, "path", None) is None or not callable(os.path.exists):
            return

        super().__del__()

    def _repr_flags(self) -> list[str]:
        flags = super()._repr_flags()
        if self.is_tmp:
            flags.append("temporary")
        return flags

    def _parent_args(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args, kwargs = super()._parent_args()
        kwargs["fs"] = self.fs
        return args, kwargs

    @property
    def abspath(self) -> str:
        return self.uri(scheme=False, return_all=False)  # type: ignore[return-value]

    def uri(self, *, scheme: bool = True, return_all: bool = False, **kwargs) -> str | list[str]:
        uri = self.fs.abspath(self.path)
        if scheme:
            uri = add_scheme(uri, "file")
        return [uri] if return_all else uri

    def copy_to_local(self, *args, **kwargs) -> str:
        return self.fs._unscheme(self.copy_to(*args, **kwargs))

    def copy_from_local(self, *args, **kwargs) -> str:
        return self.fs._unscheme(self.copy_from(*args, **kwargs))

    def move_to_local(self, *args, **kwargs) -> str:
        return self.fs._unscheme(self.move_to(*args, **kwargs))

    def move_from_local(self, *args, **kwargs) -> str:
        return self.fs._unscheme(self.move_from(*args, **kwargs))

    def load(self, *args, **kwargs) -> Any:
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        # invoke formatter
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return find_formatter(self.abspath, "load", formatter).load(self.abspath, *args, **kwargs)

    def dump(self, *args, **kwargs) -> Any:
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        # also remove permission settings
        perm = kwargs.pop("perm", None)
        dir_perm = kwargs.pop("dir_perm", None)

        # create intermediate directories
        self.parent.touch(perm=dir_perm)  # type: ignore[union-attr, call-arg]

        # invoke the formatter
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        ret = find_formatter(self.abspath, "dump", formatter).dump(self.abspath, *args, **kwargs)

        # chmod
        if perm is None:
            perm = self.fs.default_file_perm
        if perm and self.exists():
            self.chmod(perm)

        return ret


class LocalFileTarget(FileSystemFileTarget, LocalTarget):  # type: ignore[misc]

    @contextlib.contextmanager
    def localize(
        self,
        mode: str = "r",
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        tmp_dir: str | pathlib.Path | None = None,
        **kwargs,
    ) -> Generator[LocalFileTarget, None, None]:
        if mode not in ["r", "w", "a"]:
            raise Exception(f"unknown mode '{mode}', use 'r', 'w' or 'a'")

        logger.debug(f"localizing {self!r} with mode '{mode}'")

        # get additional arguments
        is_tmp = kwargs.pop("is_tmp", mode in ("w", "a"))

        if mode == "r":
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext(n=1) or True, tmp_dir=tmp_dir)

                # always copy
                self.copy_to_local(tmp)

                # yield the copy
                try:
                    yield tmp
                finally:
                    tmp.remove(silent=True)
            else:
                # simply yield
                yield self

        else:  # mode "w" or "a"
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext(n=1) or True, tmp_dir=tmp_dir)

                # copy in append mode
                if mode == "a" and self.exists():
                    self.copy_to_local(tmp)

                # yield the copy
                try:
                    yield tmp

                    # move back again
                    if tmp.exists():
                        self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm)
                    else:
                        logger.warning(
                            "cannot move non-existing localized target to actual representation "
                            f"{self!r}",
                        )
                finally:
                    tmp.remove()
            else:
                # create the parent dir
                self.parent.touch(perm=dir_perm)  # type: ignore[union-attr, call-arg]

                # simply yield
                yield self

                if perm is None:
                    perm = self.fs.default_file_perm
                self.chmod(perm, silent=True)


class LocalDirectoryTarget(FileSystemDirectoryTarget, LocalTarget):  # type: ignore[misc]

    def _child_args(
        self,
        path: str | pathlib.Path,
        type: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args, kwargs = super(LocalDirectoryTarget, self)._child_args(path, type)
        kwargs["fs"] = self.fs
        return args, kwargs

    @contextlib.contextmanager
    def localize(
        self,
        mode: str = "r",
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        tmp_dir: str | pathlib.Path | None = None,
        **kwargs,
    ) -> Generator[LocalDirectoryTarget, None, None]:
        if mode not in ["r", "w", "a"]:
            raise Exception(f"unknown mode '{mode}', use 'r', 'w' or 'a'")

        logger.debug(f"localizing {self!r} with mode '{mode}'")

        # get additional arguments
        is_tmp = kwargs.pop("is_tmp", mode in ("w", "a"))

        if mode == "r":
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=True, tmp_dir=tmp_dir)

                # copy contents
                self.copy_to_local(tmp)

                # yield the copy
                try:
                    yield tmp
                finally:
                    tmp.remove(silent=True)
            else:
                # simply yield
                yield self

        else:  # mode "w" or "a"
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=True, tmp_dir=tmp_dir)

                # copy in append mode, otherwise ensure that it exists
                if mode == "a" and self.exists():
                    self.copy_to_local(tmp)
                else:
                    tmp.touch()

                # yield the copy
                try:
                    yield tmp

                    # move back again, first removing current content
                    # TODO: keep track of changed contents in "a" mode and copy only those?
                    if tmp.exists():
                        self.remove()
                        self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm)
                    else:
                        logger.warning(
                            "cannot move non-existing localized target to actual representation "
                            f"{self!r}, leaving original contents unchanged",
                        )
                finally:
                    tmp.remove()
            else:
                # create the parent dir and the directory itself
                self.parent.touch(perm=dir_perm)  # type: ignore[union-attr, call-arg]
                self.touch(perm=perm)

                # simply yield, do not differentiate "w" and "a" modes
                yield self


# TODO: since some abstract methods defined on their superclass are simply overwritten via
# assignment, the type checker does not recognize them as concrete
LocalTarget.file_class = LocalFileTarget  # type: ignore[type-abstract]
LocalTarget.directory_class = LocalDirectoryTarget  # type: ignore[type-abstract]


# trailing imports
from law.target.remote.base import RemoteFileSystem
