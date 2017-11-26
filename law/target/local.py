# -*- coding: utf-8 -*-

"""
Local target implementations.
"""


__all__ = ["LocalFileSystem", "LocalFileTarget", "LocalDirectoryTarget"]


import os
import fnmatch
import shutil
import glob
import tempfile
import random
import zipfile
import tarfile
from contextlib import contextmanager

import luigi
import six

from law.target.file import FileSystem, FileSystemTarget, FileSystemFileTarget, \
    FileSystemDirectoryTarget, get_path
from law.target.formatter import find_formatter
from law.config import Config
from law.util import make_list


class LocalFileSystem(FileSystem):

    def abspath(self, path):
        return os.path.abspath(path)

    def exists(self, path):
        return os.path.exists(path)

    def stat(self, path):
        return os.stat(path)

    def chmod(self, path, perm):
        if perm is not None:
            os.chmod(path, perm)

    def remove(self, path, recursive=True, silent=True):
        if not silent or self.exists(path):
            if self.isdir(path):
                if recursive:
                    shutil.rmtree(path)
                else:
                    os.rmdir(path)
            else:
                os.remove(path)

    def isdir(self, path):
        return os.path.isdir(path)

    def mkdir(self, path, perm=None, recursive=True, silent=True):
        if not self.exists(path) or not silent:
            # the mode passed to os.mkdir or os.makedirs is ignored on some systems, so the strategy
            # here is to disable the process' current umask, create the directories and use chmod
            if perm is not None:
                orig = os.umask(0)

            try:
                args = (path,) if perm is None else (path, perm)
                (os.makedirs if recursive else os.mkdir)(*args)
                self.chmod(path, perm)
            finally:
                if perm is not None:
                    os.umask(orig)

    def listdir(self, path, pattern=None, type=None):
        elems = os.listdir(path)

        # apply pattern filter
        if pattern is not None:
            elems = fnmatch.filter(elems, pattern)

        # apply type filter
        if type == "f":
            elems = [elem for elem in elems if os.path.isfile(os.path.join(path, elem))]
        elif type == "d":
            elems = [elem for elem in elems if os.path.isdir(os.path.join(path, elem))]

        return elems

    def walk(self, path):
        return os.walk(path)

    def glob(self, pattern, cwd=None):
        if cwd is not None:
            origin = os.getcwd()
            os.chdir(cwd)

        elems = glob.glob(pattern)

        if cwd is not None:
            os.chdir(origin)

        return elems

    def copy(self, src, dst, dir_perm=None):
        # ensure we deal with lists of same size
        srcs = make_list(src)
        dsts = make_list(dst)
        if len(srcs) != len(dsts):
            raise ValueError("src(s) and dst(s) must have equal lengths")

        for src, dst in six.moves.zip(srcs, dsts):
            if not self.exists(src):
                raise IOError("cannot copy non-existing file or directory '{}'".format(src))
            else:
                # create missing dirs
                dst_dir = self.dirname(dst)
                if dst_dir and not os.path.exists(dst_dir):
                    self.mkdir(dst_dir, dir_perm=dir_perm, recursive=True)

                # handle directories or files
                if self.isdir(src):
                    # copy the entire tree
                    shutil.copytree(src, dst)
                else:
                    # copy the file
                    shutil.copy2(src, dst)

    def move(self, src, dst, dir_perm=None):
        # ensure we deal with lists of same size
        srcs = make_list(src)
        dsts = make_list(dst)
        if len(srcs) != len(dsts):
            raise ValueError("src(s) and dst(s) must have equal lengths")

        for src, dst in six.moves.zip(srcs, dsts):
            if not self.exists(src):
                raise IOError("cannot move non-existing file or directory '{}'".format(src))
            else:
                # create missing dirs
                dst_dir = self.dirname(dst)
                if dst_dir and not os.path.exists(dst_dir):
                    self.mkdir(dst_dir, perm=dir_perm, recursive=True)

                # simply move
                shutil.move(src, dst)

    def load(self, path, formatter, *args, **kwargs):
        return find_formatter(path, formatter).load(path, *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        return find_formatter(path, formatter).dump(path, *args, **kwargs)


_default_local_fs = LocalFileSystem()


class LocalTarget(FileSystemTarget, luigi.LocalTarget):

    fs = _default_local_fs

    def __init__(self, path=None, format=None, is_tmp=False):
        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("either path or is_tmp must be set")

            # get the tmp dir from the config and ensure it exists
            tmp_dir = Config.instance().get("target", "tmp_dir")
            tmp_dir = os.path.realpath(os.path.expandvars(os.path.expanduser(tmp_dir)))
            if not self.fs.exists(tmp_dir):
                perm = Config.instance().get("target", "tmp_dir_permission")
                _default_local_fs.mkdir(tmp_dir, perm=perm)

            # create a random path
            while True:
                path = os.path.join(tmp_dir, "luigi-tmp-%09d" % (random.randint(0, 999999999,)))
                if not _default_local_fs.exists(path):
                    break

            # is_tmp might be an extension
            if isinstance(is_tmp, six.string_types):
                if is_tmp[0] != ".":
                    is_tmp = "." + is_tmp
                path += is_tmp
        else:
            path = _default_local_fs.abspath(os.path.expandvars(os.path.expanduser(path)))

        luigi.LocalTarget.__init__(self, path=path, format=format, is_tmp=is_tmp)
        FileSystemTarget.__init__(self, self.path)

    def load(self, formatter, *args, **kwargs):
        return self.fs.load(self.path, formatter, *args, **kwargs)

    def dump(self, formatter, *args, **kwargs):
        return self.fs.dump(self.path, formatter, *args, **kwargs)

    @contextmanager
    def localize(self, mode="r", perm=None, parent_perm=None, skip_copy=False, **kwargs):
        """ localize(mode="r", perm=None, parent_perm=None, skip_copy=False, is_tmp=None)
        """
        if mode not in ("r", "w"):
            raise Exception("unknown mode '{}', use r or w".format(mode))

        # get the is_tmp value, which defaults to True for write, and to False for read mode
        is_tmp = kwargs.get("is_tmp", mode == "w")

        if mode == "r":
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext() or True)

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

        else:
            if is_tmp:
                # create a temporary target
                tmp = self.__class__(is_tmp=self.ext() or True)

                # copy when existing
                if not skip_copy and self.exists():
                    self.copy(tmp)

                # yield the copy
                try:
                    yield tmp

                    # move back again
                    if tmp.exists():
                        tmp.move(self, dir_perm=parent_perm)
                finally:
                    tmp.remove()
            else:
                # create the parent dir
                self.parent.touch(perm=parent_perm)

                # simply yield
                yield self

            self.chmod(perm)


class LocalFileTarget(LocalTarget, FileSystemFileTarget):

    pass


class LocalDirectoryTarget(LocalTarget, FileSystemDirectoryTarget):

    pass


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget
