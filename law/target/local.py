# -*- coding: utf-8 -*-

"""
Local target implementations.
"""


__all__ = ["LocalFileSystem", "LocalFileTarget", "LocalDirectoryTarget"]


import os
import fnmatch
import shutil
import glob
import random
import logging
from contextlib import contextmanager

import luigi
import six

from law.target.file import (FileSystem, FileSystemTarget, FileSystemFileTarget,
    FileSystemDirectoryTarget, get_scheme, remove_scheme)
from law.target.formatter import AUTO_FORMATTER, get_formatter, find_formatters
from law.config import Config


logger = logging.getLogger(__name__)


class LocalFileSystem(FileSystem):

    default_instance = None

    def __eq__(self, other):
        return self.__class__ == other.__class__

    def _unscheme(self, path):
        return remove_scheme(path) if get_scheme(path) == "file" else path

    def abspath(self, path):
        return os.path.abspath(self._unscheme(path))

    def exists(self, path):
        return os.path.exists(self._unscheme(path))

    def stat(self, path, **kwargs):
        return os.stat(self._unscheme(path))

    def chmod(self, path, perm, silent=True, **kwargs):
        if perm is not None and (not silent or self.exists(path)):
            os.chmod(self._unscheme(path), perm)

    def remove(self, path, recursive=True, silent=True, **kwargs):
        path = self._unscheme(path)
        if not silent or self.exists(path):
            if self.isdir(path):
                if recursive:
                    shutil.rmtree(path)
                else:
                    os.rmdir(path)
            else:
                os.remove(path)

    def mkdir(self, path, perm=None, recursive=True, silent=True, **kwargs):
        if not self.exists(path) or not silent:
            # the mode passed to os.mkdir or os.makedirs is ignored on some systems, so the strategy
            # here is to disable the process' current umask, create the directories and use chmod
            if perm is not None:
                orig = os.umask(0)

            try:
                args = (self._unscheme(path),)
                if perm is not None:
                    args += (perm,)
                (os.makedirs if recursive else os.mkdir)(*args)
                self.chmod(path, perm)
            finally:
                if perm is not None:
                    os.umask(orig)

    def listdir(self, path, pattern=None, type=None, **kwargs):
        path = self._unscheme(path)
        elems = os.listdir(path)

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
        search_dirs = [(self._unscheme(path), 0)]
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
            yield (search_dir, dirs, files, depth)

            # use dirs to update search dirs
            search_dirs.extend((os.path.join(search_dir, d), depth + 1) for d in dirs)

    def glob(self, pattern, cwd=None, **kwargs):
        pattern = self._unscheme(pattern)

        if cwd is not None:
            cwd = self._unscheme(cwd)
            pattern = os.path.join(cwd, pattern)

        elems = glob.glob(pattern)

        # cut the cwd if there was any
        if cwd is not None:
            elems = [os.path.relpath(e, cwd) for e in elems]

        return elems

    def copy(self, src, dst, dir_perm=None, **kwargs):
        src = self._unscheme(src)
        dst = self._unscheme(dst)

        # dst might be an existing directory
        if self.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        else:
            # create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and not self.exists(dst_dir):
                self.mkdir(dst_dir, dir_perm=dir_perm, recursive=True)

        # copy the file
        shutil.copy2(src, dst)

        return dst

    def move(self, src, dst, dir_perm=None, **kwargs):
        src = self._unscheme(src)
        dst = self._unscheme(dst)

        # dst might be an existing directory
        if self.exists(dst) and self.isdir(dst):
            # add src basename to dst
            dst = os.path.join(dst, os.path.basename(src))
        else:
            # create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and not self.exists(dst_dir):
                self.mkdir(dst_dir, dir_perm=dir_perm, recursive=True)

        # simply move
        shutil.move(src, dst)

        return dst

    def open(self, path, mode, **kwargs):
        return open(self._unscheme(path), mode)

    def load(self, path, formatter, *args, **kwargs):
        path = self._unscheme(path)
        if formatter == AUTO_FORMATTER:
            errors = []
            for f in find_formatters(path, silent=False):
                try:
                    return f.load(path, *args, **kwargs)
                except ImportError as e:
                    errors.append(str(e))
            else:
                raise Exception("could not automatically load '{}', errors:\n{}".format(
                    path, "\n".join(errors)))
        else:
            return get_formatter(formatter, silent=False).load(path, *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        path = self._unscheme(path)
        if formatter == AUTO_FORMATTER:
            errors = []
            for f in find_formatters(path, silent=False):
                try:
                    return f.dump(path, *args, **kwargs)
                except ImportError as e:
                    errors.append(str(e))
            else:
                raise Exception("could not automatically dump '{}', errors:\n{}".format(
                    path, "\n".join(errors)))
        else:
            return get_formatter(formatter, silent=False).dump(path, *args, **kwargs)


LocalFileSystem.default_instance = LocalFileSystem()


class LocalTarget(FileSystemTarget, luigi.LocalTarget):

    fs = LocalFileSystem.default_instance

    def __init__(self, path=None, is_tmp=False, **kwargs):
        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("either path or is_tmp must be set")

            # get the tmp dir from the config and ensure it exists
            tmp_dir = os.path.realpath(Config.instance().get_expanded("target", "tmp_dir"))
            if not self.fs.exists(tmp_dir):
                perm = Config.instance().get("target", "tmp_dir_permission")
                self.fs.mkdir(tmp_dir, perm=perm and int(perm))

            # create a random path
            while True:
                path = os.path.join(tmp_dir, "luigi-tmp-%09d" % (random.randint(0, 999999999,)))
                if not self.fs.exists(path):
                    break

            # is_tmp might be an extension
            if isinstance(is_tmp, six.string_types):
                if is_tmp[0] != ".":
                    is_tmp = "." + is_tmp
                path += is_tmp
        else:
            path = self.fs.abspath(os.path.expandvars(os.path.expanduser(remove_scheme(path))))

        luigi.LocalTarget.__init__(self, path=path, is_tmp=is_tmp)
        FileSystemTarget.__init__(self, self.path, **kwargs)


class LocalFileTarget(LocalTarget, FileSystemFileTarget):

    def copy_to_local(self, *args, **kwargs):
        return self.copy_to(*args, **kwargs)

    def copy_from_local(self, *args, **kwargs):
        return self.copy_from_local(*args, **kwargs)

    def move_to_local(self, *args, **kwargs):
        return self.move_to_local(*args, **kwargs)

    def move_from_local(self, *args, **kwargs):
        return self.move_from_local(*args, **kwargs)

    @contextmanager
    def localize(self, mode="r", perm=None, parent_perm=None, **kwargs):
        """ localize(mode="r", perm=None, parent_perm=None, skip_copy=False, is_tmp=None, **kwargs)
        """
        if mode not in ("r", "w"):
            raise Exception("unknown mode '{}', use r or w".format(mode))

        # get additional arguments
        skip_copy = kwargs.pop("skip_copy", False)
        is_tmp = kwargs.pop("is_tmp", mode == "w")

        if mode == "r":
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext(n=0) or True)

                # always copy
                self.copy(tmp)

                # yield the copy
                try:
                    yield tmp
                finally:
                    tmp.remove()
            else:
                # simply yield
                yield self

        else:  # write mode
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext(n=0) or True)

                # copy when existing
                if not skip_copy and self.exists():
                    self.copy(tmp)

                # yield the copy
                try:
                    yield tmp

                    # move back again
                    if tmp.exists():
                        tmp.move_to(self, dir_perm=parent_perm)
                    else:
                        logger.warning("cannot move non-existing localized file target {!r}".format(
                            self))
                finally:
                    tmp.remove()
            else:
                # create the parent dir
                self.parent.touch(perm=parent_perm)

                # simply yield
                yield self

            self.chmod(perm)


class LocalDirectoryTarget(LocalTarget, FileSystemDirectoryTarget):

    pass


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget
