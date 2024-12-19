# coding: utf-8

"""
Target classes that represent remote files and directories and have a local, optionally read-only
mirror (e.g. through a local mount of the remote file system).
"""

__all__ = ["MirroredTarget", "MirroredFileTarget", "MirroredDirectoryTarget"]


import os
import contextlib
import time
import threading

from law.config import Config
from law.target.file import FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.target.remote import RemoteFileTarget, RemoteDirectoryTarget
from law.util import patch_object


local_root_check_lock = threading.Lock()


class MirroredTarget(FileSystemTarget):

    _existing_local_roots = {}

    @classmethod
    def check_local_root(cls, path, depth=1):
        path = str(path)

        # path must start with a separator
        if path == os.sep:
            return True
        if not path or not path.startswith(os.sep):
            return False

        # get the root path or mount point of the path (e.g. "/mnt")
        root_path = os.sep.join(path.split(os.sep)[:depth + 1])
        if root_path not in cls._existing_local_roots:
            with local_root_check_lock:
                cls._existing_local_roots[root_path] = os.path.exists(root_path)

        return cls._existing_local_roots[root_path]

    def __init__(
        self,
        path,
        _is_file,
        remote_target=None,
        remote_target_cls=None,
        remote_fs=None,
        remote_kwargs=None,
        local_target=None,
        local_fs=None,
        local_kwargs=None,
        local_read_only=True,
        local_sync=True,
        **kwargs,
    ):
        path = path.lstrip(os.sep)

        # create a remote target
        if not remote_target:
            if not remote_fs:
                raise ValueError("either remote_target or remote_fs must be given")
            if not remote_target_cls:
                raise ValueError("either remote_target or remote_target_cls must be given")
            if _is_file and not issubclass(remote_target_cls, RemoteFileTarget):
                raise TypeError(
                    "remote_target_cls must be a subclass of RemoteFileTarget: {}".format(
                        remote_target_cls,
                    ),
                )
            if not _is_file and not issubclass(remote_target_cls, RemoteDirectoryTarget):
                raise TypeError(
                    "remote_target_cls must be a subclass of RemoteDirectoryTarget: {}".format(
                        remote_target_cls,
                    ),
                )
            remote_kwargs = remote_kwargs.copy() if remote_kwargs else {}
            remote_kwargs["fs"] = remote_fs
            remote_target = remote_target_cls(os.sep + path, **remote_kwargs)
        else:
            if _is_file and not isinstance(remote_target, RemoteFileTarget):
                raise TypeError(
                    "remote_target must be an instance of RemoteFileTarget: {}".format(
                        remote_target,
                    ),
                )
            if not _is_file and not isinstance(remote_target, RemoteDirectoryTarget):
                raise TypeError(
                    "remote_target must be an instance of RemoteDirectoryTarget: {}".format(
                        remote_target,
                    ),
                )

        # create a local target
        if not local_target:
            local_kwargs = local_kwargs.copy() if local_kwargs else {}
            if not local_fs:
                local_fs = Config.instance().get_expanded("target", "default_local_fs")
            local_kwargs["fs"] = local_fs
            local_target_cls = LocalFileTarget if _is_file else LocalDirectoryTarget
            local_target = local_target_cls(path, **local_kwargs)
        else:
            if _is_file and not isinstance(local_target, LocalFileTarget):
                raise TypeError(
                    "local_target must be an instance of LocalFileTarget: {}".format(
                        local_target,
                    ),
                )
            if not _is_file and not isinstance(local_target, LocalDirectoryTarget):
                raise TypeError(
                    "local_target must be an instance of LocalDirectoryTarget: {}".format(
                        local_target,
                    ),
                )

        # store targets
        self.remote_target = remote_target
        self.local_target = local_target

        # additional attributes
        self.local_read_only = local_read_only
        self.local_sync = local_sync

        # temporary, forced file system
        self._force_fs = None

        super().__init__(path, **kwargs)

    @property
    def _local_root_depth(self):
        return self.local_target.fs.local_root_depth

    def _local_target_exists(self, *args, **kwargs):
        return (
            self.check_local_root(self.local_target.abspath, depth=self._local_root_depth) and
            self.local_target.exists(*args, **kwargs)
        )

    def _parent_args(self):
        parent_kwargs = {
            "remote_target": self.remote_target.parent,
            "local_target": self.local_target.parent,
            "local_read_only": self.local_read_only,
            "local_sync": self.local_sync,
        }
        return (), parent_kwargs

    def _wait_for_local(self, missing=False, timeout=0.5, attempts=90):
        if not self.check_local_root(self.local_target.abspath, depth=self._local_root_depth):
            return

        sleep_counter = 0
        while self.local_target.exists() == missing:
            time.sleep(timeout)
            sleep_counter += 1

            if sleep_counter >= attempts:
                state = "disappear" if missing else "exist"
                raise Exception(
                    "timeout while waiting for local representation {!r} to {}".format(
                        self.local_target, state,
                    ),
                )

    @property
    def fs(self):
        if self._force_fs is not None:
            return self._force_fs

        return (
            self.local_target.fs
            if self._local_target_exists()
            else self.remote_target.fs
        )

    @contextlib.contextmanager
    def force_fs(self, fs):
        with patch_object(self, "_force_fs", fs):
            yield

    @property
    def abspath(self):
        return (
            self.local_target.abspath
            if self._local_target_exists()
            else self.remote_target.abspath
        )

    @property
    def dirname(self):
        return (
            self.local_target.dirname
            if self._local_target_exists()
            else self.remote_target.dirname
        )

    @property
    def absdirname(self):
        return (
            self.local_target.absdirname
            if self._local_target_exists()
            else self.remote_target.absdirname
        )

    @property
    def basename(self):
        return self.local_target.basename

    def stat(self, *args, **kwargs):
        return (
            self.local_target.stat(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.stat(*args, **kwargs)
        )

    def exists(self, *args, **kwargs):
        return (
            self.local_target.exists(*args, **kwargs) or
            self.remote_target.exists(*args, **kwargs)
        )

    def remove(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.remove(*args, **kwargs)

        ret = self.remote_target.remove(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def chmod(self, *args, **kwargs):
        return self.remote_target.chmod(*args, **kwargs)

    def uri(self, *args, **kwargs):
        return (
            self.local_target.uri(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.uri(*args, **kwargs)
        )

    def copy_to(self, *args, **kwargs):
        return (
            self.local_target.copy_to(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.copy_to(*args, **kwargs)
        )

    def copy_from(self, *args, **kwargs):
        ret = self.remote_target.copy_from(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def move_to(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.move_to(*args, **kwargs)

        ret = self.remote_target.move_to(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def move_from(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.move_from(*args, **kwargs)

        ret = self.remote_target.move_from(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def copy_to_local(self, *args, **kwargs):
        return (
            self.local_target.copy_to_local(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.copy_to_local(*args, **kwargs)
        )

    def copy_from_local(self, *args, **kwargs):
        ret = self.remote_target.copy_from_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def move_to_local(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.move_to_local(*args, **kwargs)

        ret = self.remote_target.move_to_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def move_from_local(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.move_from_local(*args, **kwargs)

        ret = self.remote_target.move_from_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    @contextlib.contextmanager
    def localize(self, mode="r", **kwargs):
        with (
            self.local_target.localize(mode, **kwargs)
            if (mode == "r" or not self.local_read_only) and self._local_target_exists()
            else self.remote_target.localize(mode, **kwargs)
        ) as ret:
            yield ret

        if mode == "w" and self.local_read_only and self.local_sync:
            self._wait_for_local(missing=False)

    def touch(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.touch(*args, **kwargs)

        ret = self.remote_target.touch(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def load(self, *args, **kwargs):
        return (
            self.local_target.load(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.load(*args, **kwargs)
        )

    def dump(self, *args, **kwargs):
        if not self.local_read_only:
            return self.local_target.dump(*args, **kwargs)

        ret = self.remote_target.dump(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret


class MirroredFileTarget(FileSystemFileTarget, MirroredTarget):

    def __init__(self, path, **kwargs):
        super().__init__(path, _is_file=True, **kwargs)

    @contextlib.contextmanager
    def open(self, mode, **kwargs):
        with (
            self.local_target.open(mode, **kwargs)
            if (mode == "r" or not self.local_read_only) and self._local_target_exists()
            else self.remote_target.open(mode, **kwargs)
        ) as ret:
            yield ret

        if mode == "w" and self.local_read_only and self.local_sync:
            self._wait_for_local(missing=False)


class MirroredDirectoryTarget(FileSystemDirectoryTarget, MirroredTarget):

    def __init__(self, path, **kwargs):
        super().__init__(path, _is_file=False, **kwargs)

    def _child_args(self, path, type):
        child_kwargs = {
            "remote_target": self.remote_target.child(path, type=type),
            "local_target": self.local_target.child(path, type=type),
            "local_read_only": self.local_read_only,
            "local_sync": self.local_sync,
        }
        return (), child_kwargs

    def listdir(self, *args, **kwargs):
        return (
            self.local_target.listdir(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.listdir(*args, **kwargs)
        )

    def glob(self, *args, **kwargs):
        return (
            self.local_target.glob(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.glob(*args, **kwargs)
        )

    def walk(self, *args, **kwargs):
        return (
            self.local_target.walk(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.walk(*args, **kwargs)
        )


MirroredTarget.file_class = MirroredFileTarget
MirroredTarget.directory_class = MirroredDirectoryTarget
