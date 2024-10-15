# coding: utf-8

"""
Target classes that represent remote files and directories and have a local, optionally read-only
mirror (e.g. through a local mount of the remote file system).
"""

from __future__ import annotations

__all__ = ["MirroredTarget", "MirroredFileTarget", "MirroredDirectoryTarget"]

import os
import contextlib
import time
import pathlib
import threading

from law.config import Config
from law.target.file import FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget
from law.target.local import LocalFileSystem, LocalTarget, LocalFileTarget, LocalDirectoryTarget
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.util import patch_object
from law._types import Any, Type, Generator, AbstractContextManager, IO, Iterator


local_root_check_lock = threading.Lock()


class MirroredTarget(FileSystemTarget):

    _existing_local_roots: dict[str, bool] = {}

    @classmethod
    def check_local_root(cls, path: str | pathlib.Path) -> bool:
        path = str(path)
        if path == os.sep or not path.startswith(os.sep):
            return False

        root_path = os.sep.join(path.split(os.sep, 2)[:2])
        if root_path not in cls._existing_local_roots:
            with local_root_check_lock:
                cls._existing_local_roots[root_path] = os.path.exists(root_path)

        return cls._existing_local_roots[root_path]

    def __init__(
        self,
        path: str | pathlib.Path,
        _is_file: bool,
        remote_target: RemoteTarget | None = None,
        remote_target_cls: Type[RemoteTarget] | None = None,
        remote_fs: RemoteFileSystem | str | None = None,
        remote_kwargs: dict[str, Any] | None = None,
        local_target: LocalTarget | None = None,
        local_fs: LocalFileSystem | str | None = None,
        local_kwargs: dict[str, Any] | None = None,
        local_read_only: bool = True,
        local_sync: bool = True,
        **kwargs,
    ) -> None:
        path = str(path).lstrip(os.sep)

        # create a remote target
        if not remote_target:
            if not remote_fs:
                raise ValueError("either remote_target or remote_fs must be given")
            if not remote_target_cls:
                raise ValueError("either remote_target or remote_target_cls must be given")
            if _is_file and not issubclass(remote_target_cls, RemoteFileTarget):
                raise TypeError(
                    f"remote_target_cls must subclass RemoteFileTarget: {remote_target_cls}",
                )
            if not _is_file and not issubclass(remote_target_cls, RemoteDirectoryTarget):
                raise TypeError(
                    f"remote_target_cls must subclass RemoteDirectoryTarget: {remote_target_cls}",
                )
            remote_kwargs = remote_kwargs.copy() if remote_kwargs else {}
            remote_kwargs["fs"] = remote_fs
            remote_target = remote_target_cls(os.sep + path, **remote_kwargs)
        else:
            if _is_file and not isinstance(remote_target, RemoteFileTarget):
                raise TypeError(
                    f"remote_target must be an instance of RemoteFileTarget: {remote_target}",
                )
            if not _is_file and not isinstance(remote_target, RemoteDirectoryTarget):
                raise TypeError(
                    f"remote_target must be an instance of RemoteDirectoryTarget: {remote_target}",
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
                    f"local_target must be an instance of LocalFileTarget: {local_target}",
                )
            if not _is_file and not isinstance(local_target, LocalDirectoryTarget):
                raise TypeError(
                    f"local_target must be an instance of LocalDirectoryTarget: {local_target}",
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

    def _local_target_exists(self, *args, **kwargs) -> bool:
        return bool(
            self.check_local_root(self.local_target.abspath) and
            self.local_target.exists(*args, **kwargs),
        )

    def _parent_args(self) -> tuple[tuple[Any, ...], dict[str, Any]]:
        parent_kwargs = {
            "remote_target": self.remote_target.parent,
            "local_target": self.local_target.parent,
            "local_read_only": self.local_read_only,
            "local_sync": self.local_sync,
        }
        return (), parent_kwargs

    def _wait_for_local(
        self,
        missing: bool = False,
        timeout: int | float = 0.5,
        attempts: int = 90,
    ) -> None:
        if not self.check_local_root(self.local_target.abspath):
            return

        sleep_counter = 0
        while self.local_target.exists() == missing:
            time.sleep(timeout)
            sleep_counter += 1

            if sleep_counter >= attempts:
                state = "disappear" if missing else "exist"
                raise Exception(
                    f"timeout while waiting for local target representation {self.local_target!r} "
                    f"to {state}",
                )

    @property
    def fs(self) -> LocalFileSystem | RemoteFileSystem:
        if self._force_fs is not None:
            return self._force_fs

        return (
            self.local_target.fs
            if self._local_target_exists()
            else self.remote_target.fs  # type: ignore[return-value]
        )

    @contextlib.contextmanager
    def force_fs(self, fs) -> Generator[None, None, None]:
        with patch_object(self, "_force_fs", fs):
            yield

    @property
    def abspath(self) -> str:
        return (
            self.local_target.abspath
            if self._local_target_exists()
            else self.remote_target.abspath
        )

    @property
    def dirname(self) -> str:
        return (
            self.local_target.dirname
            if self._local_target_exists()
            else self.remote_target.dirname
        )

    @property
    def absdirname(self) -> str:
        return (
            self.local_target.absdirname
            if self._local_target_exists()
            else self.remote_target.absdirname
        )

    @property
    def basename(self) -> str:
        return self.local_target.basename

    def stat(self, *args, **kwargs) -> os.stat_result:
        return (
            self.local_target.stat(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.stat(*args, **kwargs)
        )

    def exists(self, *args, **kwargs) -> bool | os.stat_result | None:  # type: ignore[override]
        return (
            self.local_target.exists(*args, **kwargs) or
            self.remote_target.exists(*args, **kwargs)
        )

    def remove(self, *args, **kwargs) -> bool:
        if not self.local_read_only:
            return self.local_target.remove(*args, **kwargs)

        ret = self.remote_target.remove(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def chmod(self, *args, **kwargs) -> bool:
        return self.remote_target.chmod(*args, **kwargs)

    def uri(self, *args, **kwargs) -> str | list[str]:
        return (
            self.local_target.uri(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.uri(*args, **kwargs)
        )

    def copy_to(self, *args, **kwargs) -> str:
        return (
            self.local_target.copy_to(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.copy_to(*args, **kwargs)
        )

    def copy_from(self, *args, **kwargs) -> str:
        ret = self.remote_target.copy_from(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def move_to(self, *args, **kwargs) -> str:
        if not self.local_read_only:
            return self.local_target.move_to(*args, **kwargs)

        ret = self.remote_target.move_to(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def move_from(self, *args, **kwargs) -> str:
        if not self.local_read_only:
            return self.local_target.move_from(*args, **kwargs)

        ret = self.remote_target.move_from(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def copy_to_local(self, *args, **kwargs) -> str:
        return (
            self.local_target.copy_to_local(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.copy_to_local(*args, **kwargs)
        )

    def copy_from_local(self, *args, **kwargs) -> str:
        ret = self.remote_target.copy_from_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def move_to_local(self, *args, **kwargs) -> str:
        if not self.local_read_only:
            return self.local_target.move_to_local(*args, **kwargs)

        ret = self.remote_target.move_to_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=True)
        return ret

    def move_from_local(self, *args, **kwargs) -> str:
        if not self.local_read_only:
            return self.local_target.move_from_local(*args, **kwargs)

        ret = self.remote_target.move_from_local(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    @contextlib.contextmanager
    def localize(self, mode: str = "r", **kwargs) -> Generator[FileSystemTarget, None, None]:
        with (
            self.local_target.localize(mode=mode, **kwargs)
            if (mode == "r" or not self.local_read_only) and self._local_target_exists()
            else self.remote_target.localize(mode=mode, **kwargs)
        ) as ret:
            yield ret
        if mode == "w" and self.local_read_only and self.local_sync:
            self._wait_for_local(missing=False)

    def touch(self, **kwargs) -> bool:
        if not self.local_read_only:
            return self.local_target.touch(**kwargs)

        ret = self.remote_target.touch(**kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret

    def load(self, *args, **kwargs) -> Any:
        return (
            self.local_target.load(*args, **kwargs)
            if self._local_target_exists()
            else self.remote_target.load(*args, **kwargs)
        )

    def dump(self, *args, **kwargs) -> Any:
        if not self.local_read_only:
            return self.local_target.dump(*args, **kwargs)

        ret = self.remote_target.dump(*args, **kwargs)
        if self.local_sync:
            self._wait_for_local(missing=False)
        return ret


class MirroredFileTarget(FileSystemFileTarget, MirroredTarget):

    def __init__(self, path: str | pathlib.Path, **kwargs) -> None:
        super().__init__(path, _is_file=True, **kwargs)

    def open(self, mode: str, **kwargs) -> AbstractContextManager[IO]:
        ret = (
            self.local_target.open(mode, **kwargs)
            if (mode == "r" or not self.local_read_only) and self._local_target_exists()
            else self.remote_target.open(mode, **kwargs)
        )
        if mode == "w" and self.local_read_only and self.local_sync:
            self._wait_for_local(missing=False)
        return ret


class MirroredDirectoryTarget(FileSystemDirectoryTarget, MirroredTarget):

    def __init__(self, path: str | pathlib.Path, **kwargs) -> None:
        super().__init__(path, _is_file=False, **kwargs)

    def _child_args(
        self,
        path: str | pathlib.Path,
        type: str,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        child_kwargs = {
            "remote_target": self.remote_target.child(path, type=type),  # type: ignore[attr-defined] # noqa
            "local_target": self.local_target.child(path, type=type),  # type: ignore[attr-defined] # noqa
            "local_read_only": self.local_read_only,
            "local_sync": self.local_sync,
        }
        return (), child_kwargs

    def child(self, *args, **kwargs) -> MirroredTarget:
        return (
            self.local_target.child(*args, **kwargs)  # type: ignore[attr-defined]
            if self._local_target_exists()
            else self.remote_target.child(*args, **kwargs)  # type: ignore[attr-defined]
        )

    def listdir(self, *args, **kwargs) -> list[str]:
        return (
            self.local_target.listdir(*args, **kwargs)  # type: ignore[attr-defined]
            if self._local_target_exists()
            else self.remote_target.listdir(*args, **kwargs)  # type: ignore[attr-defined]
        )

    def glob(self, *args, **kwargs) -> list[str]:
        return (
            self.local_target.glob(*args, **kwargs)  # type: ignore[attr-defined]
            if self._local_target_exists()
            else self.remote_target.glob(*args, **kwargs)  # type: ignore[attr-defined]
        )

    def walk(self, *args, **kwargs) -> Iterator[tuple[str, list[str], list[str], int]]:
        return (
            self.local_target.walk(*args, **kwargs)  # type: ignore[attr-defined]
            if self._local_target_exists()
            else self.remote_target.walk(*args, **kwargs)  # type: ignore[attr-defined]
        )


MirroredTarget.file_class = MirroredFileTarget  # type: ignore[type-abstract]
MirroredTarget.directory_class = MirroredDirectoryTarget  # type: ignore[type-abstract]
