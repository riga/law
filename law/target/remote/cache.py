# coding: utf-8

"""
Cache for remote files on local disk.
"""

from __future__ import annotations

__all__ = ["RemoteCache"]

import os
import shutil
import time
import pathlib
import tempfile
import weakref
import contextlib
import atexit

from law.config import Config
import law.target.remote.base as _remote_base
from law.util import (
    makedirs, human_bytes, parse_bytes, parse_duration, create_hash, user_owns_file, io_lock,
)
from law.logger import get_logger
from law._types import Any, Callable, Iterator, AbstractContextManager


logger = get_logger(__name__)


class RemoteCache(object):

    TMP = "__TMP__"

    lock_postfix = ".lock"

    _instances: list[RemoteCache] = []

    def __new__(cls, *args, **kwargs) -> RemoteCache:
        inst = super().__new__(cls)

        # cache instances
        cls._instances.append(inst)

        return inst

    @classmethod
    def cleanup_all(cls) -> None:
        # clear all caches
        for inst in cls._instances:
            try:
                inst._cleanup()
            except:
                pass

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        from law.sandbox.base import _sandbox_switched

        # reads a law config section and returns parsed file system configs
        cfg = Config.instance()

        if config is None:
            config = {}

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option: str, func: Callable[[str, str], Any]) -> None:
            cache_option = "cache_" + option
            if cfg.is_missing_or_none(section, cache_option):
                return
            elif option not in config or overwrite:
                config[option] = func(section, cache_option)

        def get_size(section, cache_option):
            value = cfg.get_expanded(section, cache_option)
            return parse_bytes(value, input_unit="MB", unit="MB")

        def get_time(section, cache_option):
            value = cfg.get_expanded(section, cache_option)
            return parse_duration(value, input_unit="s", unit="s")

        add("root", cfg.get_expanded)
        add("cleanup", cfg.get_expanded_bool)
        add("max_size", get_size)
        add("mtime_patience", cfg.get_expanded_float)
        add("file_perm", cfg.get_expanded_int)
        add("dir_perm", cfg.get_expanded_int)
        add("wait_delay", get_time)
        add("max_waits", cfg.get_expanded_int)
        add("global_lock", cfg.get_expanded_bool)

        # inside sandboxes, never cleanup since the outer process will do that if needed
        if _sandbox_switched:
            config["cleanup"] = False

        return config

    def __init__(
        self,
        fs: _remote_base.RemoteFileSystem,
        *,
        root: str | pathlib.Path | None = TMP,
        cleanup: bool = False,
        max_size: int | float = 0,  # in MB
        mtime_patience: int | float = 1.0,  # in seconds
        file_perm: int = 0o0660,
        dir_perm: int = 0o0770,
        wait_delay: int | float = 5.0,  # in seconds
        max_waits: int = 120,
        global_lock: bool = False,
    ) -> None:
        super().__init__()
        # max_size is in MB, wait_delay is in seconds

        # create a unique name based on fs attributes
        name = f"{fs.__class__.__name__}_{create_hash(fs.base[0])}"

        # create the root dir, handle tmp
        root = os.path.expandvars(os.path.expanduser(str(root))) or self.TMP
        if not os.path.exists(root) and root == self.TMP:
            cfg = Config.instance()
            tmp_dir = cfg.get_expanded("target", "tmp_dir")
            base = tempfile.mkdtemp(dir=tmp_dir)
            cleanup = True
        else:
            base = os.path.join(root, name)
            makedirs(base, dir_perm)

        # save attributes and configs
        self.root = root
        self.fs_ref = weakref.ref(fs)
        self.base = base
        self.name = name
        self.cleanup = cleanup
        self.max_size = float(max_size)
        self.mtime_patience = float(mtime_patience)
        self.dir_perm = dir_perm
        self.file_perm = file_perm
        self.wait_delay = float(wait_delay)
        self.max_waits = max_waits
        self.global_lock = global_lock

        # path to the global lock file which should guard global actions such as cache allocations
        self._global_lock_path = self._lock_path(os.path.join(base, "global"))

        # currently locked cache paths, only used to clean up broken files during cleanup
        self._locked_cpaths: set[str] = set()

        logger.debug(f"created {self.__class__.__name__} at '{self.base}'")

    def __del__(self) -> None:
        try:
            self._cleanup()
        except (OSError, TypeError):
            pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__} '{self.base}' at {hex(id(self))}>"

    def __contains__(self, rpath: str | pathlib.Path) -> bool:
        return os.path.exists(self.cache_path(rpath))

    @property
    def fs(self) -> _remote_base.FileSystem:
        return self.fs_ref()  # type: ignore[return-value]

    def _cleanup(self) -> None:
        # full cleanup or remove open locks
        if getattr(self, "cleanup", False):
            if os.path.exists(self.base):
                shutil.rmtree(self.base)
        else:
            for cpath in set(self._locked_cpaths):
                self._unlock(cpath)
                self._remove(cpath)
            self._locked_cpaths.clear()
            self._unlock_global()
        logger.debug(f"cleanup RemoteCache at '{self.base}'")

    def cache_path(self, rpath: str | pathlib.Path) -> str:
        rpath = str(rpath)
        basename = f"{create_hash(rpath)}_{os.path.basename(rpath)}"
        return os.path.join(self.base, basename)

    def _lock_path(self, cpath: str | pathlib.Path) -> str:
        return f"{cpath}{self.lock_postfix}"

    def is_locked_global(self) -> bool:
        return os.path.exists(self._global_lock_path)

    def _is_locked(self, cpath: str | pathlib.Path) -> bool:
        return os.path.exists(self._lock_path(cpath))

    def is_locked(self, rpath: str | pathlib.Path) -> bool:
        return self._is_locked(self.cache_path(rpath))

    def _unlock_global(self) -> None:
        try:
            os.remove(self._global_lock_path)
        except OSError:
            pass

    def _unlock(self, cpath: str | pathlib.Path) -> None:
        try:
            os.remove(self._lock_path(cpath))
        except OSError:
            pass

    def _await_global(
        self,
        *,
        delay: int | float | None = None,
        max_waits: int | None = None,
        silent: bool = False,
    ) -> bool:
        delay = delay if delay is not None else self.wait_delay
        max_waits = max_waits if max_waits is not None else self.max_waits
        _max_waits = max_waits

        while self.is_locked_global():
            if max_waits <= 0:
                if not silent:
                    raise Exception(
                        f"max_waits of {_max_waits} exceeded while waiting for global lock",
                    )
                return False

            time.sleep(delay)
            max_waits -= 1

        return True

    def _await(
        self,
        cpath: str | pathlib.Path,
        *,
        delay: int | float | None = None,
        max_waits: int | None = None,
        silent: bool = False,
        global_lock: bool | None = None,
    ) -> bool:
        cpath = str(cpath)
        delay = delay if delay is not None else self.wait_delay
        max_waits = max_waits if max_waits is not None else self.max_waits
        _max_waits = max_waits
        global_lock = self.global_lock if global_lock is None else global_lock

        # strategy: wait as long the file is locked and if the file size did not change, reduce
        # max_waits per iteration and raise when 0 is reached
        last_size = -1
        while self._is_locked(cpath) or (global_lock and self.is_locked_global()):
            if max_waits <= 0:
                if not silent:
                    raise Exception(
                        f"max_waits of {_max_waits} exceeded while waiting for file '{cpath}'",
                    )
                return False

            time.sleep(delay)

            # only reduce max_waits when the file size did not change
            # otherwise, set it to its original value again
            if os.path.exists(cpath):
                size = os.stat(cpath).st_size
                if size != last_size:
                    last_size = size
                    max_waits = _max_waits + 1

            max_waits -= 1

        return True

    @contextlib.contextmanager
    def _lock_global(self, **kwargs) -> Iterator[None]:
        self._await_global(**kwargs)

        try:
            with io_lock:
                with open(self._global_lock_path, "w") as f:
                    f.write("")
                os.utime(self._global_lock_path, None)

            yield
        finally:
            self._unlock_global()

    @contextlib.contextmanager
    def _lock(self, cpath: str | pathlib.Path, **kwargs) -> Iterator[None]:
        cpath = str(cpath)
        lock_path = self._lock_path(cpath)

        self._await(cpath, **kwargs)

        try:
            with io_lock:
                with open(lock_path, "w") as f:
                    f.write("")
                self._locked_cpaths.add(cpath)
                try:
                    os.utime(lock_path, None)
                except OSError:
                    pass

            yield
        except:
            # when something went really wrong, conservatively delete the cached file
            self._remove(cpath, lock=False)
            raise
        finally:
            # unlock again
            self._unlock(cpath)
            if cpath in self._locked_cpaths:
                self._locked_cpaths.remove(cpath)

    def lock(self, rpath: str | pathlib.Path) -> AbstractContextManager:
        return self._lock(self.cache_path(rpath))

    def allocate(self, size: int | float) -> bool:
        def _human_bytes(size: int | float) -> tuple[float, str]:
            return human_bytes(size)  # type: ignore[return-value]

        logger.debug("allocating {0[0]:.2f} {0[1]} in cache '{1}'".format(_human_bytes(size), self))

        # determine stats and current cache size
        file_stats = []
        for elem in os.listdir(self.base):
            if elem.endswith(self.lock_postfix):
                continue
            cpath = os.path.join(self.base, elem)
            file_stats.append((cpath, os.stat(cpath)))
        current_size = sum(stat.st_size for _, stat in file_stats)

        # get the available space of the disk that contains the cache in bytes, leave 10%
        fs_stat = os.statvfs(self.base)
        free_size = fs_stat.f_frsize * fs_stat.f_bavail * 0.9

        # determine the maximum size of the cache
        # make sure it is always smaller than what is available
        if self.max_size <= 0:
            max_size = current_size + free_size
        else:
            max_size = min(self.max_size * 1024**2, current_size + free_size)

        # determine the size of files that need to be deleted
        delete_size = current_size + size - max_size
        if delete_size <= 0:
            logger.debug(
                "cache space sufficient, {0[0]:.2f} {0[1]} remaining".format(
                    _human_bytes(-delete_size),
                ),
            )
            return True

        logger.info(
            "need to delete {0[0]:.2f} {0[1]} from cache".format(
                _human_bytes(delete_size),
            ),
        )

        # delete files, ordered by their access time, skip locked ones
        for cpath, cstat in sorted(file_stats, key=lambda tpl: tpl[1].st_atime):
            if self._is_locked(cpath):
                continue
            self._remove(cpath)
            delete_size -= cstat.st_size
            if delete_size <= 0:
                return True

        logger.warning(
            "could not allocate remaining {0[0]:.2f} {0[1]} in cache".format(
                _human_bytes(delete_size),
            ),
        )

        return False

    def _touch(
        self,
        cpath: str | pathlib.Path,
        times: tuple[int | float, int | float] | None = None,
    ) -> None:
        cpath = str(cpath)
        if os.path.exists(cpath):
            if user_owns_file(cpath):
                os.chmod(cpath, self.file_perm)
            os.utime(cpath, times)

    def touch(
        self,
        rpath: str | pathlib.Path,
        times: tuple[int | float, int | float] | None = None,
    ) -> None:
        return self._touch(self.cache_path(rpath), times=times)

    def _mtime(self, cpath: str | pathlib.Path) -> float:
        return os.stat(str(cpath)).st_mtime

    def mtime(self, rpath: str | pathlib.Path) -> float:
        return self._mtime(self.cache_path(rpath))

    def check_mtime(self, rpath: str | pathlib.Path, rmtime: int | float) -> bool:
        if self.mtime_patience < 0:
            return True

        return abs(self.mtime(rpath) - rmtime) <= self.mtime_patience

    def _remove(self, cpath: str | pathlib.Path, lock: bool = True) -> None:
        def remove() -> None:
            try:
                os.remove(str(cpath))
            except OSError:
                pass

        if lock:
            with self._lock(cpath):
                remove()
        else:
            remove()

    def remove(self, rpath: str | pathlib.Path, lock: bool = True) -> None:
        return self._remove(self.cache_path(rpath), lock=lock)


atexit.register(RemoteCache.cleanup_all)
