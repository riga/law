# -*- coding: utf-8 -*-

"""
Custom luigi file system and target objects.
"""


__all__ = ["FileSystem", "FileSystemFileTarget", "FileSystemDirectoryTarget"]


import os
from abc import abstractmethod, abstractproperty
from contextlib import contextmanager

import luigi
import luigi.task

from law.target.base import Target
from law.util import colored


class FileSystem(luigi.target.FileSystem):

    def __repr__(self):
        tpl = (self.__class__.__name__, hex(id(self)))
        return "%s(%s)" % tpl

    def dirname(self, path):
        return os.path.dirname(path) if path != "/" else None

    def basename(self, path):
        return os.path.basename(path) if path != "/" else "/"

    @abstractmethod
    def abspath(self, path):
        pass

    @abstractmethod
    def exists(self, path):
        pass

    @abstractmethod
    def stat(self, path):
        pass

    @abstractmethod
    def chmod(self, path, mode, recursive=False):
        pass

    @abstractmethod
    def remove(self, path, recursive=True, silent=True):
        pass

    @abstractmethod
    def isdir(self, path):
        pass

    @abstractmethod
    def mkdir(self, path, mode=0o0770, recursive=True):
        pass

    @abstractmethod
    def listdir(self, path, pattern=None, type=None):
        pass

    @abstractmethod
    def walk(self, path):
        pass

    @abstractmethod
    def glob(self, pattern, cwd=None):
        pass

    @abstractmethod
    def put(self, srcpath, dstpath):
        pass

    @abstractmethod
    def fetch(self, srcpath, dstpath):
        pass


class FileSystemTarget(Target, luigi.target.FileSystemTarget):

    file_class = None
    directory_class = None

    def __init__(self, path, exists=None):
        Target.__init__(self, exists=exists)
        luigi.target.FileSystemTarget.__init__(self, path)

    def __repr__(self):
        tpl = (self.__class__.__name__, self.path, hex(id(self)))
        return "%s(path=%s, %s)" % tpl

    def colored_repr(self):
        tpl = (colored(self.__class__.__name__, "cyan"), colored(self.path, style="bright"),
               hex(id(self)))
        return "%s(path=%s, %s)" % tpl

    def exists(self, ignore_custom=False):
        if not ignore_custom and self.custom_exists is not None:
            return self.custom_exists(self)

        return self.fs.exists(self.path)

    @property
    def parent(self):
        dirname = self.dirname
        return self.directory_class(dirname) if dirname is not None else None

    @property
    def stat(self):
        return self.fs.stat(self.path)

    @property
    def dirname(self):
        return self.fs.dirname(self.path)

    @property
    def basename(self):
        return self.fs.basename(self.path)

    def chmod(self, mode, silent=False, **kwargs):
        if not silent or self.exists():
            self.fs.chmod(self.path, mode, **kwargs)

    @abstractproperty
    def fs(self):
        pass

    @abstractmethod
    def touch(self, mode):
        pass

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def put(self):
        pass

    @abstractmethod
    @contextmanager
    def localize(self, mode=0o0660):
        pass


class FileSystemFileTarget(FileSystemTarget):

    def remove(self, silent=True, **kwargs):
        self.fs.remove(self.path, recursive=False, silent=silent, **kwargs)


class FileSystemDirectoryTarget(FileSystemTarget):

    def child(self, path, type=None):
        if type not in (None, "f", "d"):
            raise ValueError("invalid child type, use 'f' or 'd'")

        path = os.path.join(self.path, path)

        if type == "f":
            cls = self.file_class
        elif type == "d":
            cls = self.__class__
        elif not self.fs.exists(path):
            raise Exception("cannot guess type of non-existing '%s', use the type argument" % path)
        elif self.fs.isdir(path):
            cls = self.__class__
        else:
            cls = self.file_class

        return cls(path)

    def remove(self, recursive=True, silent=True, **kwargs):
        if not silent or self.exists():
            self.fs.remove(self.path, recursive=recursive, silent=silent, **kwargs)

    def listdir(self, pattern=None, type=None, **kwargs):
        return self.fs.listdir(self.path, pattern=pattern, type=type, **kwargs)

    def glob(self, pattern, **kwargs):
        return self.fs.glob(pattern, cwd=self.path, **kwargs)

    def walk(self, **kwargs):
        return self.fs.walk(self.path, **kwargs)


FileSystemTarget.file_class = FileSystemFileTarget
FileSystemTarget.directory_class = FileSystemDirectoryTarget