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
from contextlib import contextmanager

import luigi

from law.target.file import FileSystem, FileSystemTarget, FileSystemFileTarget, \
                            FileSystemDirectoryTarget
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

    def mkdir(self, path, mode=0o0770, recursive=True, silent=True):
        if not self.exists(path) or not silent:
            if recursive:
                try:
                    orig = os.umask(0)
                    os.makedirs(path, mode)
                finally:
                    os.umask(orig)
            else:
                os.mkdir(path, mode)
                self.chmod(path, mode, recursive=False)

    def listdir(self, path, pattern=None, type=None):
        elems = os.listdir(path)
        if pattern is not None:
            elems = fnmatch.filter(elems, pattern)
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

    def put(self, srcpath, dstpath):
        srcpaths = make_list(srcpath)
        dstpaths = make_list(dstpath)

        if len(srcpaths) != len(dstpaths):
            raise ValueError("srcpath(s) and dstpath(s) must have equal lengths")

        for srcpath, dstpath in zip(srcpaths, dstpaths):
            shutil.copy2(srcpath, dstpath)

    def fetch(self, srcpath, dstpath):
        self.put(dstpath, srcpath)


class LocalTarget(FileSystemTarget, luigi.LocalTarget):

    fs = LocalFileSystem()

    def __init__(self, path=None, format=None, is_tmp=False, exists=None):
        # handle tmp paths manually since luigi uses the env tmp dir
        if not path:
            if not is_tmp:
                raise Exception("path or is_tmp must be set")
            base = Config.instance().get("core", "target_tmp_dir")
            base = os.path.expandvars(os.path.expanduser(base))
            if not self.fs.exists(base):
                self.fs.mkdir(base, mode=0o0777)
            while True:
                path = os.path.join(base, "luigi-tmp-%09d" % random.randint(0, 999999999))
                if not os.path.exists(path):
                    break

        luigi.LocalTarget.__init__(self, path=path, format=format, is_tmp=is_tmp)
        FileSystemTarget.__init__(self, self.path, exists=exists)

        self.path = self.fs.abspath(os.path.expandvars(os.path.expanduser(self.path)))

    def fetch(self, *args, **kwargs):
        return self.path

    def put(self, *args, **kwargs):
        return self.path

    @contextmanager
    def localize(self, skip_parent=False, mode=0o0660, **kwargs):
        if not skip_parent:
            self.parent.touch()
        yield self
        self.chmod(mode)


class LocalFileTarget(LocalTarget, FileSystemFileTarget):

    def touch(self, content=" ", mode=0o0660):
        parent = self.parent
        if parent is not None:
            parent.touch()
        with self.open("w") as f:
            if content:
                f.write(content)
        self.fs.chmod(self.path, mode)


class LocalDirectoryTarget(LocalTarget, FileSystemDirectoryTarget):

    def touch(self, mode=0o0770, recursive=True):
        self.fs.mkdir(self.path, mode=mode, recursive=recursive, silent=True)


LocalTarget.file_class = LocalFileTarget
LocalTarget.directory_class = LocalDirectoryTarget
