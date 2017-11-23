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
from law.config import Config
from law.util import make_list


class LocalFileSystem(FileSystem):

    def abspath(self, path):
        return os.path.abspath(path)

    def exists(self, path):
        return os.path.exists(path)

    def stat(self, path):
        return os.stat(path)

    def chmod(self, path, mode):
        if mode is not None:
            os.chmod(path, mode)

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

    def mkdir(self, path, mode=None, recursive=True, silent=True):
        if not self.exists(path) or not silent:
            # the mode passed to os.mkdir or os.makedirs is ignored on some systems, so the strategy
            # here is to disable the process' current umask, create the directories and use chmod
            if mode is not None:
                orig = os.umask(0)

            try:
                args = (path, mode) if mode is None else (path,)
                (os.makedirs if recursive else os.mkdir)(*args)
                self.chmod(path, mode)
            finally:
                if mode is not None:
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

    def copy(self, src, dst, dir_mode=None):
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
                    self.mkdir(dst_dir, dir_mode=dir_mode, recursive=True)

                # handle directories or files
                if self.isdir(src):
                    # copy the entire tree
                    shutil.copytree(src, dst)
                else:
                    # copy the file
                    shutil.copy2(src, dst)

    def move(self, src, dst, dir_mode=None):
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
                    self.mkdir(dst_dir, dir_mode=dir_mode, recursive=True)

                # simply move
                shutil.move(src, dst)

    def put(self, src, dst, dir_mode=None):
        self.copy(src, dst, dir_mode=dir_mode)

    def fetch(self, src, dst, dir_mode=None):
        self.copy(src, dst, dir_mode=dir_mode)


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
            tmp_dir = os.path.expandvars(os.path.expanduser(tmp_dir))
            if not self.fs.exists(tmp_dir):
                mode = Config.instance().get("target", "tmp_dir_mode")
                _default_local_fs.mkdir(tmp_dir, mode=mode)

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

    def fetch(self, dst, dir_mode=None):
        return self.path

    def put(self, dst, dir_mode=None):
        return self.path


class LocalFileTarget(LocalTarget, FileSystemFileTarget):

    def touch(self, content=" ", mode=None, parent_mode=None):
        # create the parent
        parent = self.parent
        if parent is not None:
            parent.touch(mode=parent_mode)

        # create the file via open and write content
        with self.open("w") as f:
            if content:
                f.write(content)

        self.chmod(mode)

    @contextmanager
    def localize(self, skip_parent=False, mode=None, parent_mode=None, **kwargs):
        """ localize(self, skip_parent=False, mode=None, parent_mode=None, local_tmp=True)
        """
        local_tmp = kwargs.get("local_tmp", True)

        # create the parent
        if not skip_parent:
            self.parent.touch(mode=parent_mode)

        if local_tmp:
            # create a temporary copy
            tmp = self.__class__(is_tmp=self.ext() or True)
            self.copy(tmp)

            # yield the copy
            try:
                yield tmp
            except:
                tmp.remove()
                raise

            # finally move back again
            tmp.move(self, dir_mode=parent_mode)
        else:
            # just yield
            yield self

        self.chmod(mode)


class LocalDirectoryTarget(LocalTarget, FileSystemDirectoryTarget):

    def __init__(self, path=None, format=None, is_tmp=False, unpack=None):
        LocalTarget.__init__(self, path=path, format=format, is_tmp=is_tmp)
        FileSystemDirectoryTarget.__init__(self, self.path)

        if unpack is not None:
            self.unpack(unpack)

    def touch(self, mode=None, recursive=True):
        self.fs.mkdir(self.path, mode=mode, recursive=recursive, silent=True)

    def unpack(self, path, mode=None):
        # check the archive type and read mode
        path = os.path.expandvars(os.path.expanduser(path))
        readmode = "r"
        if path.endswith(".zip"):
            ctx = zipfile.ZipFile
        elif path.endswith(".tar.gz") or path.endswith(".tgz"):
            ctx = tarfile.open
            readmode += ":gz"
        elif path.endswith(".tbz2") or path.endswith(".bz2"):
            ctx = tarfile.open
            readmode += ":bz2"
        else:
            raise ValueError("cannot guess archive type to unpack from '{}'".format(path))

        # create the dir
        self.touch(mode=mode)

        # extract
        with ctx(path, readmode) as f:
            f.extractall(self.path)


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget
