# coding: utf-8

"""
Local target implementations.
"""

__all__ = ["LocalFileSystem", "LocalTarget", "LocalFileTarget", "LocalDirectoryTarget"]


import os
import fnmatch
import shutil
import glob
import random
from contextlib import contextmanager

import six

from law.config import Config
import law.target.luigi_shims as shims
from law.target.file import (
    FileSystem, FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, get_path,
    get_scheme, add_scheme, remove_scheme,
)
from law.target.formatter import find_formatter
from law.util import is_file_exists_error
from law.logger import get_logger


logger = get_logger(__name__)


class LocalFileSystem(FileSystem, shims.LocalFileSystem):

    default_instance = None

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        config = super(LocalFileSystem, cls).parse_config(section, config=config,
            overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
            if option not in config or overwrite:
                config[option] = func(section, option)

        # default base path
        add("base", cfg.get_expanded)

        return config

    def __init__(self, section=None, base=None, **kwargs):
        # if present, read options from the section in the law config
        self.config_section = None
        cfg = Config.instance()
        if not section:
            section = cfg.get_expanded("target", "default_local_fs")
        if isinstance(section, six.string_types):
            if cfg.has_section(section):
                # extend options of sections other than "local_fs" with its defaults
                if section != "local_fs":
                    data = dict(cfg.items("local_fs", expand_vars=False, expand_user=False))
                    cfg.update({section: data}, overwrite_sections=True, overwrite_options=False)
                kwargs = self.parse_config(section, kwargs)
                self.config_section = section
                kwargs.setdefault("name", self.config_section)
            else:
                raise Exception("law config has no section '{}' to read {} options".format(
                    section, self.__class__.__name__))

        # set the base, giving priority to config values in kwargs
        self.base = os.path.join(os.sep, kwargs.pop("base", None) or base or "")

        super(LocalFileSystem, self).__init__(**kwargs)

    def _unscheme(self, path):
        return remove_scheme(path) if get_scheme(path) == "file" else path

    def abspath(self, path):
        path = self._unscheme(path)
        return os.path.abspath(os.path.join(self.base, path.lstrip(os.sep)))

    def stat(self, path, **kwargs):
        return os.stat(self.abspath(path))

    def exists(self, path, stat=False, **kwargs):
        exists = os.path.exists(self.abspath(path))
        return (self.stat(path, **kwargs) if exists else None) if stat else exists

    def isdir(self, path, **kwargs):
        return os.path.isdir(self.abspath(path))

    def isfile(self, path, **kwargs):
        return os.path.isfile(self.abspath(path))

    def chmod(self, path, perm, silent=True, **kwargs):
        if not self.has_permissions or perm is None:
            return True

        if silent and not self.exists(path):
            return False

        os.chmod(self.abspath(path), perm)

        return True

    def remove(self, path, recursive=True, silent=True, **kwargs):
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

    def mkdir(self, path, perm=None, recursive=True, silent=True, **kwargs):
        if silent and self.exists(path):
            return False

        if perm is None:
            perm = self.default_dir_perm

        # prepare arguments passed to makedirs or mkdir
        args = (self.abspath(path),)
        if perm is not None:
            args += (perm,)

        # the mode passed to os.mkdir or os.makedirs is ignored on some systems, so the strategy
        # here is to disable the process' current umask, create the directories and use chmod again
        orig = os.umask(0) if perm is not None else None
        func = os.makedirs if recursive else os.mkdir
        try:
            try:
                func(*args)
            except Exception as e:
                if not silent or not is_file_exists_error(e):
                    raise
            self.chmod(path, perm)
        finally:
            if orig is not None:
                os.umask(orig)

        return True

    def listdir(self, path, pattern=None, type=None, **kwargs):
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

    def walk(self, path, max_depth=-1, **kwargs):
        # mimic os.walk with a max_depth and yield the current depth
        search_dirs = [(path, 0)]
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

    def glob(self, pattern, cwd=None, **kwargs):
        pattern = self.abspath(pattern)

        if cwd is not None:
            cwd = self.abspath(cwd)
            pattern = os.path.join(cwd, pattern)

        elems = glob.glob(pattern)

        # cut the cwd if there was any
        if cwd is not None:
            elems = [os.path.relpath(e, cwd) for e in elems]

        return elems

    def _prepare_dst_dir(self, dst, src=None, perm=None, **kwargs):
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
        if self.isdir(dst):
            if src:
                full_dst = os.path.join(dst, os.path.basename(src))
            else:
                full_dst = dst

        elif self.isfile(dst):
            full_dst = dst

        else:
            # interpret dst as a file name, create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and self.create_file_dir and not self.isdir(dst_dir):
                self.mkdir(dst_dir, perm=perm, recursive=True)
            full_dst = dst

        return full_dst

    def copy(self, src, dst, perm=None, dir_perm=None, **kwargs):
        dst = self._prepare_dst_dir(dst, src=src, perm=dir_perm)

        # copy the file
        shutil.copy2(self.abspath(src), self.abspath(dst))

        # set permissions
        if perm is None:
            perm = self.default_file_perm
        self.chmod(dst, perm)

        return dst

    def move(self, src, dst, perm=None, dir_perm=None, **kwargs):
        dst = self._prepare_dst_dir(dst, src=src, perm=dir_perm)

        # move the file
        shutil.move(self.abspath(src), self.abspath(dst))

        # set permissions
        if perm is None:
            perm = self.default_file_perm
        self.chmod(dst, perm)

        return dst

    def open(self, path, mode, perm=None, dir_perm=None, **kwargs):
        abspath = self.abspath(path)

        # check if the file is only read
        read_mode = mode.startswith("r")

        if read_mode:
            return open(abspath, mode)

        else:  # write or update
            # prepare the destination directory
            self._prepare_dst_dir(path, perm=dir_perm)

            if perm is None:
                perm = self.default_file_perm

            # when setting permissions, ensure the file exists first
            if perm is not None and self.has_permissions:
                open(abspath, mode).close()
                self.chmod(path, perm)

            return open(abspath, mode)

    def load(self, path, formatter, *args, **kwargs):
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        return find_formatter(path, "load", formatter).load(self.abspath(path), *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        # also remove permission settings
        perm = kwargs.pop("perm", None)
        dir_perm = kwargs.pop("dir_perm", None)

        # use open() to handle parent directory writing and permissisions
        self.open(path, "w", perm=perm, dir_perm=dir_perm).close()

        return find_formatter(path, "dump", formatter).dump(self.abspath(path), *args, **kwargs)


LocalFileSystem.default_instance = LocalFileSystem()


class LocalTarget(FileSystemTarget, shims.LocalTarget):

    fs = LocalFileSystem.default_instance

    def __init__(self, path=None, fs=LocalFileSystem.default_instance, is_tmp=False, tmp_dir=None,
            **kwargs):
        if isinstance(fs, six.string_types):
            fs = LocalFileSystem(fs)

        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("either path or is_tmp must be set")

            # if not set, get the tmp dir from the config and ensure that it exists
            cfg = Config.instance()
            if tmp_dir:
                tmp_dir = get_path(tmp_dir)
            else:
                tmp_dir = os.path.realpath(cfg.get_expanded("target", "tmp_dir"))
            if not fs.exists(tmp_dir):
                perm = cfg.get_expanded_int("target", "tmp_dir_perm")
                fs.mkdir(tmp_dir, perm=perm)

            # create a random path
            while True:
                basename = "luigi-tmp-{:09d}".format(random.randint(0, 999999999))
                path = os.path.join(tmp_dir, basename)
                if not fs.exists(path):
                    break

            # is_tmp might be a file extension
            if isinstance(is_tmp, six.string_types):
                if is_tmp[0] != ".":
                    is_tmp = "." + is_tmp
                path += is_tmp
        else:
            # ensure path is not a target and does not contain a scheme
            path = fs._unscheme(get_path(path))
            # make absolute when not starting with a variable
            if not path.startswith(("$", "~")):
                path = os.path.abspath(path)

        super(LocalTarget, self).__init__(path=path, is_tmp=is_tmp, fs=fs, **kwargs)

    def _repr_flags(self):
        flags = super(LocalTarget, self)._repr_flags()
        if self.is_tmp:
            flags.append("temporary")
        return flags

    def uri(self, scheme=True, return_all=False, **kwargs):
        uri = self.fs.abspath(self.path)
        if scheme:
            uri = add_scheme(uri, "file")
        return [uri] if return_all else uri

    def copy_to_local(self, *args, **kwargs):
        return self.fs._unscheme(self.copy_to(*args, **kwargs))

    def copy_from_local(self, *args, **kwargs):
        return self.fs._unscheme(self.copy_from(*args, **kwargs))

    def move_to_local(self, *args, **kwargs):
        return self.fs._unscheme(self.move_to(*args, **kwargs))

    def move_from_local(self, *args, **kwargs):
        return self.fs._unscheme(self.move_from(*args, **kwargs))

    def __del__(self):
        # when this destructor is called during shutdown, os.path or os.path.exists might be unset
        if getattr(os, "path", None) is None or not callable(os.path.exists):
            return

        super(LocalTarget, self).__del__()


class LocalFileTarget(FileSystemFileTarget, LocalTarget):

    @contextmanager
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        if mode not in ["r", "w", "a"]:
            raise Exception("unknown mode '{}', use 'r', 'w' or 'a'".format(mode))

        logger.debug("localizing file target {!r} with mode '{}'".format(self, mode))

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
                        tmp.move_to_local(self, perm=perm, dir_perm=dir_perm)
                    else:
                        logger.warning("cannot move non-existing, temporary target to localized "
                            "file target {!r}".format(self))
                finally:
                    tmp.remove()
            else:
                # create the parent dir
                self.parent.touch(perm=dir_perm)

                # simply yield
                yield self

                self.chmod(perm, silent=True)


class LocalDirectoryTarget(FileSystemDirectoryTarget, LocalTarget):

    pass


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget


# trailing imports
from law.target.remote.base import RemoteFileSystem
