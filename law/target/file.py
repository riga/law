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
        return "{}({})".format(self.__class__.__name__, hex(id(self)))

    def dirname(self, path):
        return os.path.dirname(path) if path != "/" else None

    def basename(self, path):
        return os.path.basename(path) if path != "/" else "/"

    def ext(self, path, n=1):
        if n < 1:
            return ""

        # split the path
        parts = path.split(".")

        # empty extension in the trivial case or use the last n parts except for the first one
        if len(parts) == 1:
            return ""
        else:
            ext = parts[max(-n, -len(parts) + 1):]
            return ".".join(ext)

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
    def chmod(self, path, mode):
        pass

    @abstractmethod
    def remove(self, path, recursive=True, silent=True):
        pass

    @abstractmethod
    def isdir(self, path):
        pass

    @abstractmethod
    def mkdir(self, path, mode=None, recursive=True):
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
    def copy(self, src, dst, dir_mode=None):
        pass

    @abstractmethod
    def move(self, src, dst, dir_mode=None):
        pass

    @abstractmethod
    def put(self, src, dst, dir_mode=None):
        pass

    @abstractmethod
    def fetch(self, src, dst, dir_mode=None):
        pass


class FileSystemTarget(Target, luigi.target.FileSystemTarget):

    file_class = None
    directory_class = None

    def __init__(self, path):
        Target.__init__(self)
        luigi.target.FileSystemTarget.__init__(self, path)

    def __repr__(self):
        return "{}(path={}, {})".format(self.__class__.__name__, self.path, hex(id(self)))

    def colored_repr(self):
        return "{}({}={})".format(colored(self.__class__.__name__, "cyan"),
            colored("path", "blue", style="bright"), self.path)

    def exists(self, ignore_custom=False):
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

    def chmod(self, mode, silent=False):
        if mode is not None and (not silent or self.exists()):
            self.fs.chmod(self.path, mode)

    @abstractproperty
    def fs(self):
        pass

    @abstractmethod
    def fetch(self, dst, dir_mode=None):
        pass

    @abstractmethod
    def put(self, dst, dir_mode=None):
        pass


class FileSystemFileTarget(FileSystemTarget):

    def remove(self, silent=True):
        self.fs.remove(self.path, recursive=False, silent=silent)

    def copy(self, dst, dir_mode=None):
        self.fs.copy(self.path, get_path(dst), dir_mode=dir_mode)

    def move(self, dst, dir_mode=None):
        self.fs.move(self.path, get_path(dst), dir_mode=dir_mode)

    def ext(self, n=1):
        return self.fs.ext(self.path, n=n)

    @abstractmethod
    def touch(self, content=" ", mode=None, parent_mode=None):
        pass

    @abstractmethod
    @contextmanager
    def localize(self, skip_parent=False, mode=None, parent_mode=None):
        pass


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
            raise Exception("cannot guess type of non-existing '{}'".format(path))
        elif self.fs.isdir(path):
            cls = self.__class__
        else:
            cls = self.file_class

        return cls(path)

    def remove(self, recursive=True, silent=True):
        if not silent or self.exists():
            self.fs.remove(self.path, recursive=recursive, silent=silent)

    def listdir(self, pattern=None, type=None):
        return self.fs.listdir(self.path, pattern=pattern, type=type)

    def glob(self, pattern):
        return self.fs.glob(pattern, cwd=self.path)

    def walk(self):
        return self.fs.walk(self.path)

    @abstractmethod
    def touch(self, mode=None, recursive=True):
        pass


FileSystemTarget.file_class = FileSystemFileTarget
FileSystemTarget.directory_class = FileSystemDirectoryTarget


def get_path(target):
    if isinstance(target, FileSystemTarget):
        return target.path
    else:
        return target
