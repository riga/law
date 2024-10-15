# coding: utf-8

"""
Remote filesystem and targets, using a configurable remote file interface for atomic operations.
"""

from __future__ import annotations

__all__ = ["RemoteFileSystem", "RemoteTarget", "RemoteFileTarget", "RemoteDirectoryTarget"]

import os
import time
import abc
import fnmatch
import pathlib
import contextlib

from law.config import Config
from law.target.file import (
    FileSystem, FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, get_path,
    get_scheme, add_scheme, remove_scheme,
)
from law.target.local import LocalFileSystem, LocalFileTarget, LocalDirectoryTarget
from law.target.remote.interface import RemoteFileInterface
from law.target.remote.cache import RemoteCache
from law.util import make_list, merge_dicts, is_pattern
from law.logger import get_logger
from law._types import Any, Callable, Iterator, TracebackType, Sequence, Literal, Generator, IO


logger = get_logger(__name__)

_local_fs = LocalFileSystem.default_instance


class RemoteFileSystem(FileSystem):

    # set right below the class definition
    default_instance: RemoteFileSystem = None  # type: ignore[assignment]

    # to be set by inheriting classes
    file_interface_cls: RemoteFileInterface

    local_fs = _local_fs
    _updated_sections: set[str] = set()

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

        # default setting for validation for existence after copy
        add("validate_copy", cfg.get_expanded_bool)

        # default setting for using the cache
        add("use_cache", cfg.get_expanded_bool)

        # cache options
        if cfg.options(section, prefix="cache_"):
            RemoteCache.parse_config(
                section,
                config.setdefault("cache_config", {}),
                overwrite=overwrite,
            )

        return config

    @classmethod
    def _update_section_defaults(cls, default_section: str, section: str) -> None:
        # do not update the section when it is the default one or it was already updated
        if section == default_section or section in cls._updated_sections:
            return
        cls._updated_sections.add(section)

        # get the defaults
        cfg = Config.instance()
        defaults = dict(cfg.items(default_section, expand_vars=False, expand_user=False))

        # update
        cfg.update({section: defaults}, overwrite_sections=True, overwrite_options=False)

    @classmethod
    def split_remote_kwargs(
        cls,
        kwargs: dict[str, Any],
        include: str | Sequence[str] | None = None,
        skip: str | Sequence[str] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        """
        Takes keyword arguments *kwargs*, splits them into two separate dictionaries depending on
        their content, and returns them in a tuple. The first one will contain arguments related to
        potential remote file operations (e.g. ``"cache"`` or ``"retries"``), while the second one
        will contain all remaining arguments. This function is used internally to decide which
        arguments to pass to target formatters. *include* (*skip*) can be a list of argument keys
        that are considered as well (ignored).
        """
        include = make_list(include) if include else []
        skip = make_list(skip) if skip else []
        transfer_kwargs = {
            name: kwargs.pop(name)
            for name in ["cache", "prefer_cache", "retries", "retry_delay"] + include
            if name in kwargs and name not in skip
        }
        return transfer_kwargs, kwargs

    def __init__(
        self,
        file_interface: RemoteFileInterface,
        validate_copy: bool = False,
        use_cache: bool = False,
        cache_config: dict[str, Any] | None = None,
        local_fs: LocalFileSystem | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # store the file interface
        self.file_interface = file_interface

        # store other configs
        self.validate_copy = validate_copy
        self.use_cache = use_cache

        # set the cache when a cache root is set in the cache_config
        self.cache: RemoteCache | None = None
        if cache_config and cache_config.get("root"):
            self.cache = RemoteCache(self, **cache_config)

        # when passed, store a custom local fs on instance level
        # otherwise, the class level member is used
        if local_fs:
            self.local_fs = local_fs

    def __del__(self) -> None:
        # cleanup the cache
        if getattr(self, "cache", None):
            del self.cache
            self.cache = None

    def __repr__(self) -> str:
        return "{}({}, name={}, base={}, {})".format(
            self.__class__.__name__,
            self.file_interface.__class__.__name__,
            self.name,
            self.base[0],
            hex(id(self)),
        )

    def _init_configs(
        self,
        section: str | None,
        default_fs_option: str,
        default_section: str,
        init_kwargs: dict[str, Any],
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        cfg = Config.instance()

        # get the proper section
        if section is None:
            section = cfg.get_expanded("target", default_fs_option)

        # try to read it and fill configs to pass to the file system and the remote file interface
        fs_config = {}
        fi_config = {}
        if isinstance(section, str):
            # when set, the section must exist
            if not cfg.has_section(section):
                raise Exception(
                    f"law config has no section '{section}' to read {self.__class__.__name__} "
                    "options",
                )

            # extend options of sections other than the default one with its values
            self._update_section_defaults(default_section, section)

            # read the configs from the section for both the file system and remote interface
            fs_config = self.parse_config(section)
            fi_config = self.file_interface_cls.parse_config(section)

        # update both configs with init kwargs
        fs_config = merge_dicts(fs_config, init_kwargs, deep=True)
        fi_config = merge_dicts(fi_config, init_kwargs, deep=True)

        return section, fs_config, fi_config

    @property
    def base(self) -> list[str]:
        return self.file_interface.base

    def is_local(self, path: str | pathlib.Path) -> bool:
        return get_scheme(path) == "file"

    def abspath(self, path: str | pathlib.Path) -> str:
        # due to the dynamic definition of remote bases, path is supposed to be already absolute,
        # so just handle leading and trailing slashes when there is no scheme scheme
        path = str(path)
        return ("/" + path.strip("/")) if not get_scheme(path) else path

    def uri(self, path: str | pathlib.Path, **kwargs) -> str | list[str]:
        return self.file_interface.uri(self.abspath(path), **kwargs)

    def dirname(self, path: str | pathlib.Path) -> str:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.dirname(path)  # type: ignore[return-value]

        return super().dirname(self.abspath(path))  # type: ignore[return-value]

    def basename(self, path: str | pathlib.Path) -> str:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.basename(path)

        return super().basename(self.abspath(path))

    def stat(self, path: str | pathlib.Path, **kwargs) -> os.stat_result:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.stat(path)

        return self.file_interface.stat(self.abspath(path), **kwargs)

    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        **kwargs,
    ) -> bool | os.stat_result | None:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.exists(path, stat=stat)

        return self.file_interface.exists(self.abspath(path), stat=stat, **kwargs)

    def isdir(
        self,
        path: str | pathlib.Path,
        rstat: os.stat_result | None = None,
        **kwargs,
    ) -> bool:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.isdir(path)

        return self.file_interface.isdir(path, stat=rstat, **kwargs)

    def isfile(
        self,
        path: str | pathlib.Path,
        rstat: os.stat_result | None = None,
        **kwargs,
    ) -> bool:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.isfile(path)

        return self.file_interface.isfile(path, stat=rstat, **kwargs)

    def chmod(self, path: str | pathlib.Path, perm: int, **kwargs) -> bool:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.chmod(path, perm)

        if not self.has_permissions:
            return True

        return self.file_interface.chmod(self.abspath(path), perm, **kwargs)

    def remove(self, path: str | pathlib.Path, **kwargs) -> bool:  # type: ignore[override]
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.remove(path)

        # protection against removing the base directory of the remote file system
        path = self.abspath(path)
        if path == "/":
            logger.warning(f"refused request to remove base directory of {self!r}")
            return False

        return self.file_interface.remove(path, **kwargs)

    def mkdir(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        perm: int | None = None,
        *,
        recursive: bool = True,
        **kwargs,
    ) -> bool:
        # forward to local_fs
        if self.is_local(path):
            return self.local_fs.mkdir(path, perm=perm, recursive=recursive)

        if perm is None:
            perm = self.default_dir_perm or 0o0770

        func = self.file_interface.mkdir_rec if recursive else self.file_interface.mkdir
        return func(self.abspath(path), perm, **kwargs)  # type: ignore[operator]

    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        pattern: str | None = None,
        type: Literal["f", "d"] | None = None,
        **kwargs,
    ) -> list[str]:
        # forward to local_fs
        path = str(path)
        if self.is_local(path):
            return self.local_fs.listdir(path, pattern=pattern, type=type)

        elems = self.file_interface.listdir(self.abspath(path), **kwargs)

        # apply pattern filter
        if pattern is not None:
            elems = fnmatch.filter(elems, pattern)

        # apply type filter
        if type == "f":
            elems = [e for e in elems if not self.isdir(os.path.join(path, e), **kwargs)]
        elif type == "d":
            elems = [e for e in elems if self.isdir(os.path.join(path, e, **kwargs))]

        return elems

    def walk(
        self,
        path: str | pathlib.Path,
        *,
        max_depth: int = -1,
        **kwargs,
    ) -> Iterator[tuple[str, list[str], list[str], int]]:
        # forward to local_fs
        if self.is_local(path):
            for obj in self.local_fs.walk(path, max_depth=max_depth):
                yield obj
            return

        # mimic os.walk with a max_depth and yield the current depth
        search_dirs = [(self.abspath(path), 0)]
        while search_dirs:
            (search_dir, depth) = search_dirs.pop(0)

            # check depth
            if max_depth >= 0 and depth > max_depth:
                continue

            # find dirs and files
            dirs = []
            files = []
            for elem in self.listdir(search_dir, **kwargs):
                if self.isdir(os.path.join(search_dir, elem), **kwargs):
                    dirs.append(elem)
                else:
                    files.append(elem)

            # yield everything
            yield (search_dir, dirs, files, depth)

            # use dirs to update search dirs
            search_dirs.extend((os.path.join(search_dir, d), depth + 1) for d in dirs)

    def glob(
        self,
        pattern: str | pathlib.Path,
        *,
        cwd: str | pathlib.Path | None = None,
        **kwargs,
    ) -> list[str]:
        # forward to local_fs
        pattern = str(pattern)
        if self.is_local(pattern):
            return self.local_fs.glob(pattern, cwd=cwd)

        # prepare pattern
        if cwd is not None:
            pattern = os.path.join(cwd, pattern)

        # split the pattern to determine the search path, i.e. the leading part that does not
        # contain any glob chars, e.g. "foo/bar/test*/baz*" -> "foo/bar"
        search_dirs = []
        patterns: list[str] = []
        for part in pattern.split(os.sep):
            if not patterns and not is_pattern(part):
                search_dirs.append(part)
            else:
                patterns.append(part)
        search_dir = self.abspath(os.sep.join(search_dirs))

        # walk trough the search path and use fnmatch for comparison
        elems = []
        max_depth = len(patterns) - 1
        for root, dirs, files, depth in self.walk(search_dir, max_depth=max_depth, **kwargs):
            # get the current pattern
            pattern = patterns[depth]

            # when we are still below the max depth, filter dirs
            # otherwise, filter files and dirs to select
            if depth < max_depth:
                dirs[:] = fnmatch.filter(dirs, pattern)
            else:
                elems += [os.path.join(root, e) for e in fnmatch.filter(dirs + files, pattern)]

        # cut the cwd if there was any
        if cwd is not None:
            elems = [os.path.relpath(e, cwd) for e in elems]

        return elems

    # atomic copy
    def _atomic_copy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        validate: bool | None = None,
        **kwargs,
    ) -> str:
        if validate is None:
            validate = self.validate_copy

        src = self.abspath(src)
        dst = self.abspath(dst)

        # actual copy
        src_uri, dst_uri = self.file_interface.filecopy(src, dst, **kwargs)

        # copy validation
        dst_fs = self.local_fs if self.is_local(dst_uri) else self
        if validate:
            if not dst_fs.exists(dst):
                raise Exception(f"validation failed after copying {src_uri} to {dst_uri}")

        # handle permissions
        if perm is None:
            perm = dst_fs.default_file_perm
        dst_fs.chmod(dst, perm)  # type: ignore[arg-type]

        return dst_uri

    # generic copy with caching ability (local paths must have a "file://" scheme)
    def _cached_copy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path | None,
        *,
        perm: int | None = None,
        cache: bool | None = None,
        prefer_cache: bool = False,
        validate: bool | None = None,
        **kwargs,
    ) -> str:
        """
        When this method is called, both *src* and *dst* should refer to files.
        """
        if self.cache is None:
            cache = False
        else:
            cache_inst = self.cache
            cache = self.use_cache if cache is None else bool(cache)

        # ensure absolute paths
        src = self.abspath(src)
        dst = self.abspath(dst) if dst is not None else None
        _dst = str(dst)

        # determine the copy mode for code readability
        # (remote-remote: "rr", remote-local: "rl", remote-cache: "rc", ...)
        src_local = self.is_local(src)
        dst_local = self.is_local(dst) if dst is not None else False
        mode = "rl"[src_local] + ("rl"[dst_local] if dst is not None else "c")

        # disable caching when the mode is local-local, local-cache or remote-remote
        if mode in ("ll", "lc", "rr"):
            cache = False

        # dst can be None, but in this case, caching should be enabled
        if dst is None and not cache:
            raise Exception("copy destination must not be empty when caching is disabled")

        if not cache:
            # simply copy and return the dst path
            return self._atomic_copy(src, _dst, perm=perm, validate=validate, **kwargs)

        kwargs_no_retries = kwargs.copy()
        kwargs_no_retries["retries"] = 0

        # handle 3 cases: lr, rl, rc
        if mode == "lr":
            # strategy: copy to remote, copy to cache, sync stats

            # copy to remote, no need to validate as we compute the stat anyway
            dst_uri = self._atomic_copy(src, _dst, perm=perm, validate=False, **kwargs)
            rstat = self.stat(_dst, **kwargs_no_retries)

            # remove the cache entry
            if _dst in cache_inst:
                logger.debug(f"removing destination file {_dst} from cache")
                cache_inst.remove(_dst)

            # allocate cache space and copy to cache
            lstat = self.local_fs.stat(src)
            cache_inst.allocate(lstat.st_size)
            cdst_uri = add_scheme(cache_inst.cache_path(_dst), "file")
            with cache_inst.lock(_dst):
                logger.debug("loading source file {} to cache".format(src))
                self._atomic_copy(src, cdst_uri, validate=False)
                cache_inst.touch(_dst, (int(time.time()), rstat.st_mtime))

            return dst_uri

        else:  # rl, rc
            # strategy: copy to cache when not up to date, sync stats, opt. copy to local

            # build the uri to the cache path of the src file
            csrc_uri = add_scheme(cache_inst.cache_path(src), "file")

            # if the file is cached and prefer_cache is true,
            # return the cache path, no questions asked
            # otherwise, check if the file is there and up to date
            if not prefer_cache or src not in cache_inst:
                with cache_inst.lock(src):
                    # in cache and outdated?
                    rstat = self.stat(src, **kwargs_no_retries)
                    if src in cache_inst and not cache_inst.check_mtime(src, rstat.st_mtime):
                        logger.debug("source file {} is outdated in cache, removing".format(src))
                        cache_inst.remove(src, lock=False)
                    # in cache at all?
                    if src not in cache_inst:
                        cache_inst.allocate(rstat.st_size)
                        self._atomic_copy(src, csrc_uri, validate=validate, **kwargs)
                        logger.debug("loading source file {} to cache".format(src))
                        cache_inst.touch(src, (int(time.time()), rstat.st_mtime))

            if mode == "rl":
                # simply use the local_fs for copying
                self.local_fs.copy(csrc_uri, _dst, perm=perm)
                return _dst

            # mode is rc
            return csrc_uri

    def _prepare_dst_dir(
        self,
        dst: str | pathlib.Path,
        src: str | pathlib.Path | None = None,
        *,
        perm: int | None = None,
        **kwargs,
    ) -> str:
        """
        Prepares the directory of a target located at *dst* for copying and returns its full
        location as specified below. *src* can be the location of a source file target, which is
        (e.g.) used by a file copy or move operation. When *dst* is already a directory, calling
        this method has no effect and the *dst* path is returned, optionally joined with the
        basename of *src*. When *dst* is a file, *dst* path is returned unchanged. Otherwise, when
        *dst* does not exist yet, it is interpreted as a file path and missing directories are
        created when :py:attr:`create_file_dir` is *True*, using *perm* to set the directory
        permission. *dst* is returned.
        """
        dst = str(dst)
        rstat: os.stat_result | None = self.exists(dst, stat=True)  # type: ignore[assignment]

        if rstat is not None:
            if self.file_interface.isdir(dst, stat=rstat) and src:
                full_dst = os.path.join(dst, os.path.basename(src))
            else:
                full_dst = dst

        else:
            # interpret dst as a file name, create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and self.create_file_dir and not self.isdir(dst_dir):
                self.mkdir(dst_dir, perm=perm, recursive=True, **kwargs)
            full_dst = dst

        return full_dst

    def copy(  # type: ignore[override]
        self,
        src,
        dst,
        *,
        perm=None,
        dir_perm=None,
        **kwargs,
    ) -> str:
        # dst might be an existing directory
        if dst:
            dst_fs = self.local_fs if self.is_local(dst) else self
            dst_fs._prepare_dst_dir(dst, src=src, perm=dir_perm, **kwargs)  # type: ignore[attr-defined] # noqa

        # copy the file
        return self._cached_copy(src, dst, perm=perm, **kwargs)

    def move(  # type: ignore[override]
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        **kwargs,
    ) -> str:
        if not dst:
            raise Exception("move requires dst to be set")

        # copy the file
        kwargs["cache"] = False
        kwargs.setdefault("validate", True)
        dst = self.copy(src, dst, perm=perm, dir_perm=dir_perm, **kwargs)

        # remove the src
        src_fs = self.local_fs if self.is_local(src) else self
        src_fs.remove(src, **kwargs)

        return dst

    def open(  # type: ignore[override]
        self,
        path: str | pathlib.Path,
        mode: str,
        *,
        perm: int | None = None,
        dir_perm: int | None = None,
        cache: bool | None = None,
        **kwargs,
    ) -> RemoteProxyBase:
        if self.cache is None:
            cache = False
        else:
            cache = self.use_cache if cache is None else bool(cache)

        yield_path = kwargs.pop("_yield_path", False)
        path = self.abspath(path)
        tmp = None
        read_mode = mode.startswith("r")

        if read_mode:
            if cache:
                lpath = self._cached_copy(path, None, cache=cache, **kwargs)
            else:
                tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
                lpath = self.copy(path, tmp.uri(), cache=cache, **kwargs)
            lpath = remove_scheme(lpath)

            def cleanup() -> None:
                if not cache and tmp and tmp.exists():
                    tmp.remove()

            if yield_path:
                return RemoteFilePathProxy(lpath, success_fn=cleanup, failure_fn=cleanup)
            return RemoteFileProxy(open(lpath, mode), success_fn=cleanup, failure_fn=cleanup)

        # write or update
        tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
        lpath = tmp.path

        def cleanup() -> None:
            tmp.remove(silent=True)

        def copy_and_cleanup() -> None:
            exists = True
            try:
                exists: bool = tmp.exists()  # type: ignore[assignment]
                if exists:
                    self.copy(
                        tmp.uri(),
                        path,
                        perm=perm,
                        dir_perm=dir_perm,
                        cache=cache,
                        **kwargs,
                    )
            finally:
                if exists:
                    tmp.remove(silent=True)

        if yield_path:
            return RemoteFilePathProxy(lpath, success_fn=copy_and_cleanup, failure_fn=cleanup)
        return RemoteFileProxy(open(lpath, mode), success_fn=copy_and_cleanup, failure_fn=cleanup)


class RemoteTarget(FileSystemTarget):

    def __init__(self, path: str | pathlib.Path, fs: RemoteFileSystem, **kwargs) -> None:
        if not isinstance(fs, RemoteFileSystem):
            raise TypeError(f"fs must be a {RemoteFileSystem} instance, got '{fs}'")

        self.fs = fs  # type: ignore[misc]

        super().__init__(path, **kwargs)

    def _parent_args(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args, kwargs = super()._parent_args()
        args += (self.fs,)
        return args, kwargs

    @property
    def path(self) -> str:
        return self._path

    @path.setter
    def path(self, path: str | pathlib.Path) -> None:
        if os.path.normpath(str(path)).startswith(".."):
            raise ValueError("path {} forbidden, surpasses file system root".format(path))

        path = self.fs.abspath(path)
        super(RemoteTarget, self.__class__).path.fset(self, path)  # type: ignore[attr-defined]

    @property
    def abspath(self) -> str:
        return self.uri(return_all=False)  # type: ignore[return-value]

    def uri(self, **kwargs) -> str | list[str]:
        return self.fs.uri(self.path, **kwargs)  # type: ignore[attr-defined]

    def copy_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget | None = None,
        **kwargs,
    ) -> str:
        if dst is not None:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")  # type: ignore[attr-defined] # noqa
        dst = self.copy_to(dst, **kwargs)  # type: ignore[arg-type]
        return remove_scheme(dst)

    def copy_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget | None = None,
        **kwargs,
    ) -> str:
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")  # type: ignore[attr-defined] # noqa
        return self.copy_from(src, **kwargs)

    def move_to_local(
        self,
        dst: str | pathlib.Path | FileSystemTarget | None = None,
        **kwargs,
    ) -> str:
        if dst is not None:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")  # type: ignore[attr-defined] # noqa
        dst = self.move_to(dst, **kwargs)  # type: ignore[arg-type]
        return remove_scheme(dst)

    def move_from_local(
        self,
        src: str | pathlib.Path | FileSystemTarget | None = None,
        **kwargs,
    ) -> str:
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")  # type: ignore[attr-defined] # noqa
        return self.move_from(src, **kwargs)

    def load(self, *args, **kwargs) -> Any:
        # split kwargs that might be designated for remote files
        remote_kwargs, kwargs = self.fs.split_remote_kwargs(kwargs)  # type: ignore[attr-defined]

        # forward to the localized representation
        with self.localize(mode="r", **remote_kwargs) as loc:
            return loc.load(*args, **kwargs)

    def dump(self, *args, **kwargs) -> Any:
        # split kwargs that might be designated for remote files
        remote_kwargs, kwargs = self.fs.split_remote_kwargs(kwargs)  # type: ignore[attr-defined]

        # forward to the localized representation
        with self.localize(mode="a", **remote_kwargs) as loc:
            return loc.dump(*args, **kwargs)


class RemoteFileTarget(FileSystemFileTarget, RemoteTarget):

    @property
    def cache_path(self) -> str | None:
        if not self.fs.cache:  # type: ignore[attr-defined]
            return None

        return self.fs.cache.cache_path(self.path)  # type: ignore[attr-defined]

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

        if mode == "r":
            with self.fs.open(self.path, "r", _yield_path=True, perm=perm, **kwargs) as lpath:
                yield LocalFileTarget(lpath)  # type: ignore[arg-type]

        else:  # mode "w" or "a"
            tmp = LocalFileTarget(is_tmp=self.ext(n=1) or True, tmp_dir=tmp_dir)

            # copy to local in append mode
            if mode == "a" and self.exists():
                self.copy_to_local(tmp, **kwargs)

            try:
                yield tmp

                if tmp.exists():
                    self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm, **kwargs)
                else:
                    logger.warning(
                        "cannot move non-existing localized target to actual representation "
                        f"{self!r}",
                    )
            finally:
                tmp.remove()


class RemoteDirectoryTarget(FileSystemDirectoryTarget, RemoteTarget):

    def _child_args(
        self,
        path: str | pathlib.Path,
        type: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        args, kwargs = super()._child_args(path, type)
        args += (self.fs,)
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

        if mode == "r":
            # create a temporary directory
            tmp = LocalDirectoryTarget(is_tmp=True, tmp_dir=tmp_dir)

            # copy contents
            self.copy_to_local(tmp, **kwargs)

            # yield the copy
            try:
                yield tmp
            finally:
                tmp.remove(silent=True)

        else:  # mode "w" or "a"
            # create a temporary directory
            tmp = LocalDirectoryTarget(is_tmp=True, tmp_dir=tmp_dir)

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
                    self.remove(**kwargs)
                    self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm, **kwargs)
                else:
                    logger.warning(
                        "cannot move non-existing localized target to actual representation "
                        f"{self!r}, leaving original contents unchanged",
                    )
            finally:
                tmp.remove()


# TODO: since some abstract methods defined on their superclass are simply overwritten via
# assignment, the type checker does not recognize them as concrete
RemoteTarget.file_class = RemoteFileTarget  # type: ignore[type-abstract]
RemoteTarget.directory_class = RemoteDirectoryTarget  # type: ignore[type-abstract]


class RemoteProxyBase(object, metaclass=abc.ABCMeta):

    def __init__(
        self,
        *,
        close_fn: Callable | None = None,
        success_fn: Callable | None = None,
        failure_fn: Callable | None = None,
    ) -> None:
        super().__init__()

        self.close_fn = close_fn
        self.success_fn = success_fn
        self.failure_fn = failure_fn

    def _on_success_or_failure(self, success: bool) -> None:
        func = self.success_fn if success else self.failure_fn
        if callable(func):
            func()

    @abc.abstractmethod
    def __enter__(self) -> Any:
        ...

    @abc.abstractmethod
    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> bool:
        ...

    def close(self) -> None:
        if callable(self.close_fn):
            self.close_fn()


class RemoteFileProxy(RemoteProxyBase):

    def __init__(self, f: IO, **kwargs) -> None:
        super().__init__(**kwargs)

        self.f = f

    def __call__(self) -> IO:
        return self.f

    def __getattr__(self, attr: str) -> Any:
        return getattr(self.f, attr)

    def __enter__(self) -> IO:
        return self.f.__enter__()

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> bool:
        # when an exception was raised, the context was not successful
        success = exc_type is None

        # invoke the exit of the file object
        # when its return value is True, it overwrites the success flag
        if getattr(self.f, "__exit__", None) is not None:
            exit_ret = self.f.__exit__(exc_type, exc_value, traceback)  # type: ignore[func-returns-value] # noqa
            if exit_ret is True:
                success = True

        self._on_success_or_failure(success)

        return success

    def close(self, *args, **kwargs) -> None:
        self.f.close(*args, **kwargs)
        super().close()


class RemoteFilePathProxy(RemoteProxyBase):

    def __init__(self, f: str | pathlib.Path, **kwargs) -> None:
        super().__init__(**kwargs)

        self.f = str(f)

    def __enter__(self) -> str:
        return self.f

    def __exit__(self, exc_type: type, exc_value: BaseException, traceback: TracebackType) -> bool:
        # when an exception was raised, the context was not successful
        success = exc_type is None

        self._on_success_or_failure(success)

        return success
