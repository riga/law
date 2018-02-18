# -*- coding: utf-8 -*-

"""
Custom luigi file system and target objects.
"""


__all__ = ["FileSystem", "FileSystemFileTarget", "FileSystemDirectoryTarget",
           "get_path", "get_scheme", "has_scheme", "add_scheme", "remove_scheme"]


import os
import stat
from abc import abstractmethod, abstractproperty
from contextlib import contextmanager

import six
import luigi
import luigi.task

from law.target.base import Target
from law.util import create_hash


class FileSystem(luigi.target.FileSystem):

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, hex(id(self)))

    def hash(self, path, l=10):
        return create_hash(self.__class__.__name__ + self.abspath(path))

    def dirname(self, path):
        return os.path.dirname(self.abspath(path)) if path != "/" else None

    def basename(self, path):
        return os.path.basename(path) if path != "/" else "/"

    def unique_basename(self, path, l=10):
        return self.hash(path, l=l) + "_" + self.basename(path)

    def ext(self, path, n=1):
        # split the path
        parts = self.basename(path).lstrip(".").split(".")

        # empty extension in the trivial case or use the last n parts except for the first one
        if len(parts) == 1:
            return ""
        else:
            return ".".join(parts[1:][min(-n, 0):])

    def isdir(self, path, **kwargs):
        return stat.S_ISDIR(self.stat(path, **kwargs).st_mode)

    @abstractmethod
    def __eq__(self, other):
        return

    @abstractproperty
    def default_instance(self):
        return

    @abstractmethod
    def abspath(self, path):
        return

    @abstractmethod
    def exists(self, path):
        return

    @abstractmethod
    def stat(self, path):
        return

    @abstractmethod
    def chmod(self, path, perm, silent=True, **kwargs):
        return

    @abstractmethod
    def remove(self, path, recursive=True, silent=True, **kwargs):
        return

    @abstractmethod
    def mkdir(self, path, perm=None, recursive=True, silent=True, **kwargs):
        return

    @abstractmethod
    def listdir(self, path, pattern=None, type=None, **kwargs):
        return

    @abstractmethod
    def walk(self, path, max_depth=-1, **kwargs):
        return

    @abstractmethod
    def glob(self, pattern, cwd=None, **kwargs):
        return

    @abstractmethod
    def copy(self, src, dst, dir_perm=None, **kwargs):
        return

    @abstractmethod
    def move(self, src, dst, dir_perm=None, **kwargs):
        return

    @abstractmethod
    @contextmanager
    def open(self, path, mode, **kwargs):
        return

    @abstractmethod
    def load(self, path, formatter, *args, **kwargs):
        return

    @abstractmethod
    def dump(self, path, formatter, *args, **kwargs):
        return


class FileSystemTarget(Target, luigi.target.FileSystemTarget):

    file_class = None
    directory_class = None

    def __init__(self, path, **kwargs):
        Target.__init__(self, **kwargs)
        luigi.target.FileSystemTarget.__init__(self, path)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and \
            self.fs == other.fs and \
            self.fs.abspath(self.path) == other.fs.abspath(other.path)

    def _repr_pairs(self):
        return Target._repr_pairs(self) + [("path", self.path)]

    @property
    def init_args(self):
        return tuple()

    @property
    def hash(self):
        return self.fs.hash(self.path)

    def exists(self):
        return self.fs.exists(self.path)

    @property
    def parent(self):
        dirname = self.dirname
        return self.directory_class(dirname, *self.init_args) if dirname is not None else None

    @property
    def stat(self):
        return self.fs.stat(self.path)

    @property
    def dirname(self):
        return self.fs.dirname(self.path)

    @property
    def basename(self):
        return self.fs.basename(self.path)

    @property
    def unique_basename(self):
        return self.fs.unique_basename(self.path)

    def remove(self, silent=True, **kwargs):
        self.fs.remove(self.path, recursive=True, silent=silent, **kwargs)

    def chmod(self, perm, silent=False, **kwargs):
        self.fs.chmod(self.path, perm, silent=silent, **kwargs)

    @abstractproperty
    def fs(self):
        return


class FileSystemFileTarget(FileSystemTarget):

    type = "f"

    def ext(self, n=1):
        return self.fs.ext(self.path, n=n)

    def touch(self, content=" ", perm=None, parent_perm=None, **kwargs):
        # create the parent
        parent = self.parent
        if parent is not None:
            parent.touch(perm=parent_perm, **kwargs)

        # create the file via open and write content
        with self.open("w", **kwargs) as f:
            if content:
                f.write(content)

        self.chmod(perm, **kwargs)

    def open(self, mode, **kwargs):
        return self.fs.open(self.path, mode, **kwargs)

    def load(self, *args, **kwargs):
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return self.fs.load(self.path, formatter, *args, **kwargs)

    def dump(self, *args, **kwargs):
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return self.fs.dump(self.path, formatter, *args, **kwargs)

    def copy_to(self, dst, dir_perm=None, **kwargs):
        return self.fs.copy(self.path, get_path(dst), dir_perm=dir_perm, **kwargs)

    def copy_from(self, src, dir_perm=None, **kwargs):
        return self.fs.copy(get_path(src), self.path, dir_perm=dir_perm, **kwargs)

    def move_to(self, dst, dir_perm=None, **kwargs):
        return self.fs.move(self.path, get_path(dst), dir_perm=dir_perm, **kwargs)

    def move_from(self, src, dir_perm=None, **kwargs):
        return self.fs.move(get_path(src), self.path, dir_perm=dir_perm, **kwargs)

    @abstractmethod
    def copy_to_local(self, *args, **kwargs):
        return

    @abstractmethod
    def copy_from_local(self, *args, **kwargs):
        return

    @abstractmethod
    def move_to_local(self, *args, **kwargs):
        return

    @abstractmethod
    def move_from_local(self, *args, **kwargs):
        return

    @abstractmethod
    @contextmanager
    def localize(self, mode="r", perm=None, parent_perm=None, **kwargs):
        return


class FileSystemDirectoryTarget(FileSystemTarget):

    type = "d"

    open = None

    def child(self, path, type=None):
        if type not in (None, "f", "d"):
            raise ValueError("invalid child type, use 'f' or 'd'")

        path = os.path.join(self.path, get_path(path))

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

        return cls(path, *self.init_args)

    def listdir(self, pattern=None, type=None, **kwargs):
        return self.fs.listdir(self.path, pattern=pattern, type=type, **kwargs)

    def glob(self, pattern, **kwargs):
        return self.fs.glob(pattern, cwd=self.path, **kwargs)

    def walk(self, **kwargs):
        return self.fs.walk(self.path, **kwargs)

    def touch(self, perm=None, recursive=True, **kwargs):
        self.fs.mkdir(self.path, perm=perm, recursive=recursive, silent=True, **kwargs)


FileSystemTarget.file_class = FileSystemFileTarget
FileSystemTarget.directory_class = FileSystemDirectoryTarget


def get_path(target):
    return target.path if isinstance(target, FileSystemTarget) else target


def get_scheme(path):
    # ftp://path/to/file -> ftp
    # /path/to/file -> None
    return six.moves.urllib_parse.urlparse(path).scheme or None


def has_scheme(path):
    return get_scheme(path) is not None


def add_scheme(path, scheme):
    # adds a scheme to a path, if it does not already contain one
    return "{}://{}".format(scheme, path) if not has_scheme(path) else path


def remove_scheme(path):
    # ftp://path/to/file -> /path/to/file
    # /path/to/file -> /path/to/file
    return six.moves.urllib_parse.urlparse(path).path or None


# trailing imports
from law.target.formatter import AUTO_FORMATTER
