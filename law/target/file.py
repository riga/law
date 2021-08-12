# coding: utf-8

"""
Custom luigi file system and target objects.
"""

__all__ = [
    "FileSystem", "FileSystemTarget", "FileSystemFileTarget", "FileSystemDirectoryTarget",
    "get_path", "get_scheme", "has_scheme", "add_scheme", "remove_scheme", "localize_file_targets",
]


import os
import sys
import re
from abc import abstractmethod, abstractproperty
from contextlib import contextmanager

import luigi
import luigi.task

from law.config import Config
from law.target.base import Target
from law.util import map_struct, create_random_string


class FileSystem(luigi.target.FileSystem):

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        # reads a law config section and returns parsed file system configs
        cfg = Config.instance()

        if config is None:
            config = {}

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
            if option not in config or overwrite:
                config[option] = func(section, option)

        # read configs
        add("has_permissions", cfg.get_expanded_boolean)
        add("default_file_perm", cfg.get_expanded_int)
        add("default_dir_perm", cfg.get_expanded_int)
        add("create_file_dir", cfg.get_expanded_boolean)

        return config

    def __init__(self, has_permissions=True, default_file_perm=None, default_dir_perm=None,
            create_file_dir=True, **kwargs):
        luigi.target.FileSystem.__init__(self)

        self.has_permissions = has_permissions
        self.default_file_perm = default_file_perm
        self.default_dir_perm = default_dir_perm
        self.create_file_dir = create_file_dir

    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, hex(id(self)))

    def dirname(self, path):
        return os.path.dirname(path) if path != "/" else None

    def basename(self, path):
        return os.path.basename(path) if path != "/" else "/"

    def ext(self, path, n=1):
        # split the path
        parts = self.basename(path).lstrip(".").split(".")

        # empty extension in the trivial case or use the last n parts except for the first one
        if len(parts) == 1:
            return ""
        else:
            return ".".join(parts[1:][min(-n, 0):])

    @abstractproperty
    def default_instance(self):
        return

    @abstractmethod
    def abspath(self, path):
        return

    @abstractmethod
    def stat(self, path, **kwargs):
        return

    @abstractmethod
    def exists(self, path, **kwargs):
        return

    @abstractmethod
    def isdir(self, path, **kwargs):
        return

    @abstractmethod
    def isfile(self, path, **kwargs):
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
    def copy(self, src, dst, perm=None, dir_perm=None, **kwargs):
        return

    @abstractmethod
    def move(self, src, dst, perm=None, dir_perm=None, **kwargs):
        return

    @abstractmethod
    @contextmanager
    def open(self, path, mode, perm=None, dir_perm=None, **kwargs):
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

    def __init__(self, path, fs=None, **kwargs):
        if fs:
            self.fs = fs

        self._path = None
        self._unexpanded_path = None

        Target.__init__(self, **kwargs)
        luigi.target.FileSystemTarget.__init__(self, path)

    def _repr_pairs(self, color=True):
        expand = Config.instance().get_expanded_boolean("target", "expand_path_repr")
        return Target._repr_pairs(self) + [("path", self.path if expand else self.unexpanded_path)]

    def _parent_args(self):
        return (), {}

    @property
    def unexpanded_path(self):
        return self._unexpanded_path

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._unexpanded_path = str(path)
        self._path = os.path.abspath(os.path.expandvars(os.path.expanduser(self.unexpanded_path)))

    @property
    def dirname(self):
        return self.fs.dirname(self.path)

    @property
    def basename(self):
        return self.fs.basename(self.path)

    @property
    def unique_basename(self):
        return "{}_{}".format(hex(self.hash)[2:], self.basename)

    @property
    def parent(self):
        # get the dirname, but favor the unexpanded one to propagate variables
        dirname = self.dirname
        unexpanded_dirname = self.fs.dirname(self.unexpanded_path)
        expanded_dirname = os.path.expandvars(os.path.expanduser(unexpanded_dirname))
        if unexpanded_dirname and dirname == os.path.abspath(expanded_dirname):
            dirname = unexpanded_dirname

        args, kwargs = self._parent_args()
        return self.directory_class(dirname, *args, **kwargs) if dirname is not None else None

    def sibling(self, *args, **kwargs):
        parent = self.parent
        if not parent:
            raise Exception("cannot determine parent of {!r}".format(self))

        return parent.child(*args, **kwargs)

    def exists(self, **kwargs):
        return self.fs.exists(self.path, **kwargs)

    def stat(self, **kwargs):
        return self.fs.stat(self.path, **kwargs)

    def remove(self, silent=True, **kwargs):
        self.fs.remove(self.path, silent=silent, **kwargs)

    def chmod(self, perm, silent=False, **kwargs):
        self.fs.chmod(self.path, perm, silent=silent, **kwargs)

    @abstractproperty
    def fs(self):
        return

    @abstractmethod
    def touch(self, perm=None, dir_perm=None):
        return


class FileSystemFileTarget(FileSystemTarget):

    type = "f"

    def ext(self, n=1):
        return self.fs.ext(self.path, n=n)

    def touch(self, **kwargs):
        # create the file via open without content
        with self.open("w", **kwargs) as f:
            f.write("")

    def open(self, mode, **kwargs):
        return self.fs.open(self.path, mode, **kwargs)

    def load(self, *args, **kwargs):
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return self.fs.load(self.path, formatter, *args, **kwargs)

    def dump(self, *args, **kwargs):
        formatter = kwargs.pop("_formatter", None) or kwargs.pop("formatter", AUTO_FORMATTER)
        return self.fs.dump(self.path, formatter, *args, **kwargs)

    def copy_to(self, dst, perm=None, dir_perm=None, **kwargs):
        return self.fs.copy(self.path, get_path(dst), perm=perm, dir_perm=dir_perm, **kwargs)

    def copy_from(self, src, perm=None, dir_perm=None, **kwargs):
        return self.fs.copy(get_path(src), self.path, perm=perm, dir_perm=dir_perm, **kwargs)

    def move_to(self, dst, perm=None, dir_perm=None, **kwargs):
        return self.fs.move(self.path, get_path(dst), perm=perm, dir_perm=dir_perm, **kwargs)

    def move_from(self, src, perm=None, dir_perm=None, **kwargs):
        return self.fs.move(get_path(src), self.path, perm=perm, dir_perm=dir_perm, **kwargs)

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
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        return


class FileSystemDirectoryTarget(FileSystemTarget):

    type = "d"

    open = None

    def _child_args(self, path):
        return (), {}

    def child(self, path, type=None, mktemp_pattern=False, **kwargs):
        if type not in (None, "f", "d"):
            raise ValueError("invalid child type, use 'f' or 'd'")

        # apply mktemp's feature to replace at least three consecutive 'X' with random characters
        path = get_path(path)
        if mktemp_pattern and "XXX" in path:
            repl = lambda m: create_random_string(l=len(m.group(1)))
            path = re.sub("(X{3,})", repl, path)

        unexpanded_path = os.path.join(self.unexpanded_path, path)
        path = os.path.join(self.path, path)
        if type == "f":
            cls = self.file_class
        elif type == "d":
            cls = self.__class__
        elif not self.fs.exists(path):
            raise Exception("cannot guess type of non-existing path '{}'".format(path))
        elif self.fs.isdir(path):
            cls = self.__class__
        else:
            cls = self.file_class

        args, _kwargs = self._child_args(path)
        _kwargs.update(kwargs)

        return cls(unexpanded_path, *args, **_kwargs)

    def listdir(self, **kwargs):
        return self.fs.listdir(self.path, **kwargs)

    def glob(self, pattern, **kwargs):
        return self.fs.glob(pattern, cwd=self.path, **kwargs)

    def walk(self, **kwargs):
        return self.fs.walk(self.path, **kwargs)

    def touch(self, **kwargs):
        kwargs.setdefault("silent", True)
        self.fs.mkdir(self.path, **kwargs)


FileSystemTarget.file_class = FileSystemFileTarget
FileSystemTarget.directory_class = FileSystemDirectoryTarget


def get_path(target):
    return target.path if isinstance(target, FileSystemTarget) else target


def get_scheme(uri):
    # ftp://path/to/file -> ftp
    # /path/to/file -> None
    m = re.match(r"^(\w+)\:\/\/.*$", uri)
    return m.group(1) if m else None


def has_scheme(uri):
    return get_scheme(uri) is not None


def add_scheme(path, scheme):
    # adds a scheme to a path, if it does not already contain one
    return "{}://{}".format(scheme.rstrip(":/"), path) if not has_scheme(path) else path


def remove_scheme(uri):
    # ftp://path/to/file -> /path/to/file
    # /path/to/file -> /path/to/file
    return re.sub(r"^(\w+\:\/\/)", "", uri)


@contextmanager
def localize_file_targets(struct, *args, **kwargs):
    """
    Takes an arbitrary *struct* of targets, opens the contexts returned by their
    :py:meth:`FileSystemFileTarget.localize` implementations and yields their localized
    representations in the same structure as passed in *struct*. When the context is closed, the
    contexts of all localized targets are closed.
    """
    managers = []

    def enter(target):
        if callable(getattr(target, "localize", None)):
            manager = target.localize(*args, **kwargs)
            managers.append(manager)
            return manager.__enter__()
        else:
            return target

    # localize all targets, maintain the structure
    localized_targets = map_struct(enter, struct)

    # prepare exception info
    exc = None
    exc_info = (None, None, None)

    try:
        yield localized_targets

    except (Exception, KeyboardInterrupt) as e:
        exc = e
        exc_info = sys.exc_info()
        raise

    finally:
        exit_exc = []
        for manager in managers:
            try:
                manager.__exit__(*exc_info)
            except Exception as e:
                exit_exc.append(e)

        # when there was no exception during the actual yield and
        # an exception occured in one of the exit methods, raise the first one
        if not exc and exit_exc:
            raise exit_exc[0]


# trailing imports
from law.target.formatter import AUTO_FORMATTER
