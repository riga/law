# coding: utf-8

"""
Remote filesystem and targets, using a configurable remote file interface for atomic operations.
"""

__all__ = ["RemoteFileSystem", "RemoteTarget", "RemoteFileTarget", "RemoteDirectoryTarget"]


import os
import time
import fnmatch
from contextlib import contextmanager

import six

from law.config import Config
from law.target.file import (
    FileSystem, FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, get_path,
    get_scheme, add_scheme, remove_scheme,
)
from law.target.local import LocalFileSystem, LocalFileTarget
from law.target.remote.cache import RemoteCache
from law.target.formatter import find_formatter
from law.util import make_list, merge_dicts
from law.logger import get_logger


logger = get_logger(__name__)

_local_fs = LocalFileSystem.default_instance


class RemoteFileSystem(FileSystem):

    default_instance = None
    file_interface_cls = None
    local_fs = _local_fs
    _updated_sections = set()

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        config = super(RemoteFileSystem, cls).parse_config(section, config=config,
            overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
            if option not in config or overwrite:
                config[option] = func(section, option)

        # default setting for validation for existence after copy
        add("validate_copy", cfg.get_expanded_boolean)

        # default setting for using the cache
        add("use_cache", cfg.get_expanded_boolean)

        # cache options
        if cfg.options(section, prefix="cache_"):
            RemoteCache.parse_config(section, config.setdefault("cache_config", {}),
                overwrite=overwrite)

        return config

    @classmethod
    def _update_section_defaults(cls, default_section, section):
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
    def split_remote_kwargs(cls, kwargs, include=None, skip=None):
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

    def __init__(self, file_interface, validate_copy=False, use_cache=False, cache_config=None,
            local_fs=None, **kwargs):
        super(RemoteFileSystem, self).__init__(**kwargs)

        # store the file interface
        self.file_interface = file_interface

        # store other configs
        self.validate_copy = validate_copy
        self.use_cache = use_cache

        # set the cache when a cache root is set in the cache_config
        if cache_config and cache_config.get("root"):
            self.cache = RemoteCache(self, **cache_config)
        else:
            self.cache = None

        # when passed, store a custom local fs on instance level
        # otherwise, the class level member is used
        if local_fs:
            self.local_fs = local_fs

    def __del__(self):
        # cleanup the cache
        if getattr(self, "cache", None):
            self.cache._cleanup()

    def __repr__(self):
        return "{}({}, base={}, {})".format(self.__class__.__name__,
            self.file_interface.__class__.__name__, self.base[0], hex(id(self)))

    def _init_configs(self, section, default_fs_option, default_section, init_kwargs):
        cfg = Config.instance()

        # get the proper section
        if not section:
            section = cfg.get_expanded("target", default_fs_option)

        # try to read it and fill configs to pass to the file system and the remote file interface
        fs_config = {}
        fi_config = {}
        if isinstance(section, six.string_types):
            # when set, the section must exist
            if not cfg.has_section(section):
                raise Exception("law config has no section '{}' to read {} options".format(
                    section, self.__class__.__name__))

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
    def base(self):
        return self.file_interface.base

    def is_local(self, path):
        return get_scheme(path) == "file"

    def abspath(self, path):
        # due to the dynamic definition of remote bases, path is supposed to be already absolute,
        # so just handle leading and trailing slashes when there is no scheme scheme
        return ("/" + path.strip("/")) if not get_scheme(path) else path

    def uri(self, path, **kwargs):
        return self.file_interface.uri(self.abspath(path), **kwargs)

    def dirname(self, path):
        return FileSystem.dirname(self, self.abspath(path))

    def basename(self, path):
        return FileSystem.basename(self, self.abspath(path))

    def stat(self, path, **kwargs):
        return self.file_interface.stat(self.abspath(path), **kwargs)

    def exists(self, path, stat=False, **kwargs):
        return self.file_interface.exists(self.abspath(path), stat=stat, **kwargs)

    def isdir(self, path, rstat=None, **kwargs):
        return self.file_interface.isdir(path, stat=rstat, **kwargs)

    def isfile(self, path, rstat=None, **kwargs):
        return self.file_interface.isfile(path, stat=rstat, **kwargs)

    def chmod(self, path, perm, **kwargs):
        if not self.has_permissions:
            return True

        return self.file_interface.chmod(self.abspath(path), perm, **kwargs)

    def remove(self, path, **kwargs):
        # protection against removing the base directory of the remote file system
        path = self.abspath(path)
        if path == "/":
            logger.warning("refused request to remove base directory of {!r}".format(self))
            return

        return self.file_interface.remove(path, **kwargs)

    def mkdir(self, path, perm=None, recursive=True, **kwargs):
        if perm is None:
            perm = self.default_dir_perm or 0o0770

        func = self.file_interface.mkdir_rec if recursive else self.file_interface.mkdir
        x = func(self.abspath(path), perm, **kwargs)
        return x

    def listdir(self, path, pattern=None, type=None, **kwargs):
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

    def walk(self, path, max_depth=-1, **kwargs):
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

    def glob(self, pattern, cwd=None, **kwargs):
        # helper to check if a string represents a pattern
        def is_pattern(s):
            return "*" in s or "?" in s

        # prepare pattern
        if cwd is not None:
            pattern = os.path.join(cwd, pattern)

        # split the pattern to determine the search path, i.e. the leading part that does not
        # contain any glob chars, e.g. "foo/bar/test*/baz*" -> "foo/bar"
        search_dir = []
        patterns = []
        for part in pattern.split("/"):
            if not patterns and not is_pattern(part):
                search_dir.append(part)
            else:
                patterns.append(part)
        search_dir = self.abspath("/".join(search_dir))

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
    def _atomic_copy(self, src, dst, perm=None, validate=None, **kwargs):
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
                raise Exception("validation failed after copying {} to {}".format(src_uri, dst_uri))

        # handle permissions
        if perm is None:
            perm = dst_fs.default_file_perm
        dst_fs.chmod(dst, perm)

        return dst_uri

    # generic copy with caching ability (local paths must have a "file://" scheme)
    def _cached_copy(self, src, dst, perm=None, cache=None, prefer_cache=False, validate=None,
            **kwargs):
        """
        When this method is called, both *src* and *dst* should refer to files.
        """
        if self.cache is None:
            cache = False
        elif cache is None:
            cache = self.use_cache
        else:
            cache = bool(cache)

        # ensure absolute paths
        src = self.abspath(src)
        dst = dst and self.abspath(dst) or None

        # determine the copy mode for code readability
        # (remote-remote: "rr", remote-local: "rl", remote-cache: "rc", ...)
        src_local = self.is_local(src)
        dst_local = dst and self.is_local(dst)
        mode = "rl"[src_local] + ("rl"[dst_local] if dst is not None else "c")

        # disable caching when the mode is local-local, local-cache or remote-remote
        if mode in ("ll", "lc", "rr"):
            cache = False

        # dst can be None, but in this case, caching should be enabled
        if dst is None and not cache:
            raise Exception("copy destination must not be empty when caching is disabled")

        if cache:
            kwargs_no_retries = kwargs.copy()
            kwargs_no_retries["retries"] = 0

            # handle 3 cases: lr, rl, rc
            if mode == "lr":
                # strategy: copy to remote, copy to cache, sync stats

                # copy to remote, no need to validate as we compute the stat anyway
                dst_uri = self._atomic_copy(src, dst, perm=perm, validate=False, **kwargs)
                rstat = self.stat(dst, **kwargs_no_retries)

                # remove the cache entry
                if dst in self.cache:
                    self.cache.remove(dst)

                # allocate cache space and copy to cache
                lstat = self.local_fs.stat(src)
                self.cache.allocate(lstat.st_size)
                cdst_uri = add_scheme(self.cache.cache_path(dst), "file")
                with self.cache.lock(dst):
                    self._atomic_copy(src, cdst_uri, validate=False)
                    self.cache.touch(dst, (int(time.time()), rstat.st_mtime))

                return dst_uri

            else:  # rl, rc
                # strategy: copy to cache when not up to date, sync stats, opt. copy to local

                # build the uri to the cache path of the src file
                csrc_uri = add_scheme(self.cache.cache_path(src), "file")

                # if the file is cached and prefer_cache is true,
                # return the cache path, no questions asked
                # otherwise, check if the file is there and up to date
                if not prefer_cache or src not in self.cache:
                    with self.cache.lock(src):
                        # in cache and outdated?
                        rstat = self.stat(src, **kwargs_no_retries)
                        if src in self.cache and abs(self.cache.mtime(src) - rstat.st_mtime) > 1:
                            self.cache.remove(src, lock=False)
                        # in cache at all?
                        if src not in self.cache:
                            self.cache.allocate(rstat.st_size)
                            self._atomic_copy(src, csrc_uri, validate=validate, **kwargs)
                            self.cache.touch(src, (int(time.time()), rstat.st_mtime))

                if mode == "rl":
                    # simply use the local_fs for copying
                    self.local_fs.copy(csrc_uri, dst, perm=perm)
                    return dst
                else:  # rc
                    return csrc_uri

        else:
            # simply copy and return the dst path
            return self._atomic_copy(src, dst, perm=perm, validate=validate, **kwargs)

    def _prepare_dst_dir(self, dst, src=None, perm=None, **kwargs):
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
        rstat = self.exists(dst, stat=True)

        if rstat:
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

    def copy(self, src, dst, perm=None, dir_perm=None, **kwargs):
        # dst might be an existing directory
        if dst:
            dst_fs = self.local_fs if self.is_local(dst) else self
            dst_fs._prepare_dst_dir(dst, src=src, perm=dir_perm, **kwargs)

        # copy the file
        return self._cached_copy(src, dst, perm=perm, **kwargs)

    def move(self, src, dst, perm=None, dir_perm=None, **kwargs):
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

    def open(self, path, mode, perm=None, dir_perm=None, cache=None, **kwargs):
        if self.cache is None:
            cache = False
        elif cache is None:
            cache = self.use_cache
        else:
            cache = bool(cache)

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

            def cleanup():
                if not cache and tmp and tmp.exists():
                    tmp.remove()

            f = lpath if yield_path else open(lpath, mode)
            return RemoteFileProxy(f, success_fn=cleanup, failure_fn=cleanup)

        else:  # write or update
            tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
            lpath = tmp.path

            def cleanup():
                tmp.remove(silent=True)

            def copy_and_cleanup():
                exists = True
                try:
                    exists = tmp.exists()
                    if exists:
                        self.copy(tmp.uri(), path, perm=perm, dir_perm=dir_perm, cache=cache,
                            **kwargs)
                finally:
                    if exists:
                        tmp.remove(silent=True)

            f = lpath if yield_path else open(lpath, mode)
            return RemoteFileProxy(f, success_fn=copy_and_cleanup, failure_fn=cleanup)

    def load(self, path, formatter, *args, **kwargs):
        # split kwargs that might be designated for remote files
        remote_kwargs, kwargs = self.split_remote_kwargs(kwargs)

        fmt = find_formatter(path, "load", formatter)
        with self.open(path, "r", _yield_path=True, **remote_kwargs) as lpath:
            return fmt.load(lpath, *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        # split kwargs that might be designated for remote files
        remote_kwargs, kwargs = self.split_remote_kwargs(kwargs, include=["perm", "dir_perm"])

        fmt = find_formatter(path, "dump", formatter)
        with self.open(path, "w", _yield_path=True, **remote_kwargs) as lpath:
            return fmt.dump(lpath, *args, **kwargs)


class RemoteTarget(FileSystemTarget):

    fs = None

    def __init__(self, path, fs, **kwargs):
        if not isinstance(fs, RemoteFileSystem):
            raise TypeError("fs must be a {} instance, is {}".format(RemoteFileSystem, fs))

        self.fs = fs

        FileSystemTarget.__init__(self, path, **kwargs)

    def _parent_args(self):
        args, kwargs = FileSystemTarget._parent_args(self)
        args += (self.fs,)
        return args, kwargs

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        if os.path.normpath(path).startswith(".."):
            raise ValueError("path {} forbidden, surpasses file system root".format(path))

        path = self.fs.abspath(path)
        FileSystemTarget.path.fset(self, path)

    def uri(self, **kwargs):
        return self.fs.uri(self.path, **kwargs)


class RemoteFileTarget(RemoteTarget, FileSystemFileTarget):

    def copy_to_local(self, dst=None, **kwargs):
        if dst:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")
        dst = FileSystemFileTarget.copy_to(self, dst, **kwargs)
        return remove_scheme(dst)

    def copy_from_local(self, src=None, **kwargs):
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.copy_from(self, src, **kwargs)

    def move_to_local(self, dst=None, **kwargs):
        if dst:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")
        dst = FileSystemFileTarget.move_to(self, dst, **kwargs)
        return remove_scheme(dst)

    def move_from_local(self, src=None, **kwargs):
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.move_from(self, src, **kwargs)

    @contextmanager
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        if mode not in ["r", "w", "a"]:
            raise Exception("unknown mode '{}', use 'r', 'w' or 'a'".format(mode))

        logger.debug("localizing file target {!r} with mode '{}'".format(self, mode))

        if mode == "r":
            with self.fs.open(self.path, "r", _yield_path=True, perm=perm, **kwargs) as lpath:
                yield LocalFileTarget(lpath)

        else:  # mode "w" or "a"
            tmp = LocalFileTarget(is_tmp=self.ext(n=1) or True, tmp_dir=tmp_dir)

            # copy to local in append mode
            if mode == "a" and self.exists():
                self.copy_to_local(tmp)

            try:
                yield tmp

                if tmp.exists():
                    self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm, **kwargs)
                else:
                    logger.warning("cannot move non-existing localized file target {!r}".format(
                        self))
            finally:
                tmp.remove()


class RemoteDirectoryTarget(RemoteTarget, FileSystemDirectoryTarget):

    def _child_args(self, path):
        args, kwargs = FileSystemDirectoryTarget._child_args(self, path)
        args += (self.fs,)
        return args, kwargs


RemoteTarget.file_class = RemoteFileTarget
RemoteTarget.directory_class = RemoteDirectoryTarget


class RemoteFileProxy(object):

    def __init__(self, f, close_fn=None, success_fn=None, failure_fn=None):
        super(RemoteFileProxy, self).__init__()

        self.f = f
        self.is_file = not isinstance(f, six.string_types)

        self.close_fn = close_fn
        self.success_fn = success_fn
        self.failure_fn = failure_fn

    def __call__(self):
        return self.f

    def __getattr__(self, attr):
        return getattr(self.f, attr)

    def __enter__(self):
        return self.f.__enter__() if self.is_file else self.f

    def __exit__(self, exc_type, exc_value, traceback):
        # when an exception was raised, the context was not successful
        success = exc_type is None

        # invoke the exit of the file object
        # when its return value is True, it overwrites the success flag
        if getattr(self.f, "__exit__", None) is not None:
            exit_ret = self.f.__exit__(exc_type, exc_value, traceback)
            if exit_ret is True:
                success = True

        if success:
            if callable(self.success_fn):
                self.success_fn()
        else:
            if callable(self.failure_fn):
                self.failure_fn()

        return success

    def close(self, *args, **kwargs):
        ret = self.f.close(*args, **kwargs)

        if callable(self.close_fn):
            self.close_fn()

        return ret
