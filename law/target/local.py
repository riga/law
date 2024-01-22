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
from law.target.formatter import AUTO_FORMATTER, find_formatter
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
        # setting both section and base is ambiguous and not allowed
        if section and base:
            raise Exception(
                "setting both 'section' and 'base' as {} arguments is ambiguous and therefore not "
                "supported, but got {} and {}".format(self.__class__.__name__, section, base),
            )

        # determine the configured default local fs section
        cfg = Config.instance()
        default_section = cfg.get_expanded("target", "default_local_fs")
        self.config_section = None

        # when no base is given, evaluate the config section
        if not base:
            if not section:
                # use the default section when none is set
                section = default_section
            elif section != default_section:
                # check if the section exists
                if not cfg.has_section(section):
                    raise Exception("law config has no section '{}' to read {} options".format(
                        section, self.__class__.__name__))
                # extend non-default sections by options of the default one
                data = dict(cfg.items(default_section, expand_vars=False, expand_user=False))
                cfg.update({section: data}, overwrite_sections=True, overwrite_options=False)
            self.config_section = section

            # parse the config and set fs name and base
            kwargs = self.parse_config(self.config_section, kwargs)
            kwargs.setdefault("name", self.config_section)
            base = kwargs.pop("base", None) or os.sep

            # special case: the default local fs is not allowed to have a base directory other than
            # "/" to ensure that files and directories wrapped by local targets in law or derived
            # projects for convenience are interpreted as such and in particular do not resolve them
            # relative to a base path defined in some config
            if self.config_section == default_section and base != os.sep:
                raise Exception(
                    "the default local fs '{}' must not have a base defined, but got {}".format(
                        default_section, base),
                )

        # set the base
        self.base = os.path.abspath(self._unscheme(str(base)))

        super(LocalFileSystem, self).__init__(**kwargs)

    def _unscheme(self, path):
        path = str(path)
        return remove_scheme(path) if get_scheme(path) == "file" else path

    def abspath(self, path):
        path = os.path.expandvars(os.path.expanduser(self._unscheme(path)))

        # join with the base path
        base = os.path.expandvars(os.path.expanduser(str(self.base)))
        path = os.path.join(base, path.lstrip(os.sep))

        return os.path.abspath(path)

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
                if not silent or not isinstance(e, FileExistsError):
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
        dst, src = str(dst), src and str(src)
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

    fs = LocalFileSystem.default_instance

    def __init__(self, path=None, fs=LocalFileSystem.default_instance, is_tmp=False, tmp_dir=None,
            **kwargs):
        if isinstance(fs, six.string_types):
            fs = LocalFileSystem(fs)

        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("when no target path is defined, is_tmp must be set")
            if str(fs.base) != "/":
                raise Exception(
                    "when is_tmp is set, the base of the underlying file system must be '/', but "
                    "found '{}'".format(fs.base),
                )

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
            # ensure path is not a target and has no scheme
            path = fs._unscheme(get_path(path))

        super(LocalTarget, self).__init__(path=path, is_tmp=is_tmp, fs=fs, **kwargs)

    def __del__(self):
        # when this destructor is called during shutdown, os.path or os.path.exists might be unset
        if getattr(os, "path", None) is None or not callable(os.path.exists):
            return

        super(LocalTarget, self).__del__()

    def _repr_flags(self):
        flags = super(LocalTarget, self)._repr_flags()
        if self.is_tmp:
            flags.append("temporary")
        return flags

    def _parent_args(self):
        args, kwargs = super(LocalTarget, self)._parent_args()
        kwargs["fs"] = self.fs
        return args, kwargs

    @property
    def abspath(self):
        return self.uri(scheme=False)

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

    def load(self, *args, **kwargs):
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        # invoke formatter
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return find_formatter(self.abspath, "load", formatter).load(self.abspath, *args, **kwargs)

    def dump(self, *args, **kwargs):
        # remove kwargs that might be designated for remote files
        kwargs = RemoteFileSystem.split_remote_kwargs(kwargs)[1]

        # also remove permission settings
        perm = kwargs.pop("perm", None)
        dir_perm = kwargs.pop("dir_perm", None)

        # create intermediate directories
        self.parent.touch(perm=dir_perm)

        # invoke the formatter
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        ret = find_formatter(self.abspath, "dump", formatter).dump(self.abspath, *args, **kwargs)

        # chmod
        if perm and self.exists():
            self.chmod(perm)

        return ret


class LocalFileTarget(FileSystemFileTarget, LocalTarget):

    @contextmanager
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        if mode not in ["r", "w", "a"]:
            raise Exception("unknown mode '{}', use 'r', 'w' or 'a'".format(mode))

        logger.debug("localizing {!r} with mode '{}'".format(self, mode))

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
                        tmp.copy_to_local(self, perm=perm, dir_perm=dir_perm)
                    else:
                        logger.warning("cannot move non-existing localized target to actual "
                            "representation {!r}".format(self))
                finally:
                    tmp.remove()
            else:
                # create the parent dir
                self.parent.touch(perm=dir_perm)

                # simply yield
                yield self

                self.chmod(perm, silent=True)


class LocalDirectoryTarget(FileSystemDirectoryTarget, LocalTarget):

    def _child_args(self, path):
        args, kwargs = super(LocalDirectoryTarget, self)._child_args(path)
        kwargs["fs"] = self.fs
        return args, kwargs

    @contextmanager
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        if mode not in ["r", "w", "a"]:
            raise Exception("unknown mode '{}', use 'r', 'w' or 'a'".format(mode))

        logger.debug("localizing {!r} with mode '{}'".format(self, mode))

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
                        tmp.copy_to_local(self, perm=perm, dir_perm=dir_perm)
                    else:
                        logger.warning("cannot move non-existing localized target to actual "
                            "representation {!r}, leaving original contents unchanged".format(self))
                finally:
                    tmp.remove()
            else:
                # create the parent dir and the directory itself
                self.parent.touch(perm=dir_perm)
                self.touch(perm=perm)

                # simply yield, do not differentiate "w" and "a" modes
                yield self


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget


# trailing imports
from law.target.remote.base import RemoteFileSystem
