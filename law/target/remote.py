# -*- coding: utf-8 -*-

"""
Remote filesystem and targets, based on gfal2 bindings.
"""


__all__ = ["RemoteFileSystem", "RemoteFileTarget", "RemoteDirectoryTarget"]


import os
import shutil
import stat
import time
import fnmatch
import tempfile
import weakref
import functools
import atexit
import gc
import random
import threading
import warnings
import logging
from contextlib import contextmanager

import six

from law.config import Config
from law.target.file import FileSystem, FileSystemTarget, FileSystemFileTarget, \
    FileSystemDirectoryTarget, get_path, get_scheme, has_scheme, add_scheme, remove_scheme
from law.target.local import LocalFileSystem, LocalFileTarget
from law.target.formatter import AUTO_FORMATTER, get_formatter, find_formatters
from law.util import make_list, copy_no_perm, makedirs_perm, human_bytes, create_hash, \
    user_owns_file


logger = logging.getLogger(__name__)


# try to import gfal2
try:
    import gfal2

    HAS_GFAL2 = True

    # configure gfal2 logging
    if not getattr(gfal2, "_law_configured_logging", False):
        gfal2._law_configured_logging = True
        gfal2_logger = logging.getLogger("gfal2")
        gfal2_logger.addHandler(logging.StreamHandler())
        level = Config.instance().get("target", "gfal2_log_level")
        if isinstance(level, six.string_types):
            level = getattr(logging, level, logging.WARNING)
        gfal2_logger.setLevel(level)

except ImportError:
    HAS_GFAL2 = False

    class gfal2Dummy(object):

        def __getattr__(self, attr):
            raise Exception("trying to access 'gfal2.{}', but gfal2 is not installed".format(attr))

    gfal2 = gfal2Dummy()


_local_fs = LocalFileSystem.default_instance


def retry(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        retries = kwargs.pop("retries", None)
        if retries is None:
            retries = self.retries

        delay = kwargs.pop("retry_delay", None)
        if delay is None:
            delay = self.retry_delay

        attempt = 0
        try:
            while True:
                try:
                    return func(self, *args, **kwargs)
                except gfal2.GError as e:
                    attempt += 1
                    if attempt > retries:
                        raise e
                    else:
                        logger.debug("gfal2.{}(args: {}, kwargs: {}) failed, retry".format(
                            func.__name__, args, kwargs))
                        time.sleep(delay)
        except Exception as e:
            e.message += "\nfunction: {}\nattempt : {}\nargs    : {}\nkwargs  : {}".format(
                func.__name__, attempt, args, kwargs)
            raise e

    return wrapper


class GFALInterface(object):

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
            atomic_contexts=False, retries=0, retry_delay=0):
        object.__init__(self)

        # cache for gfal context objects and transfer parameters per pid for thread safety
        self._contexts = {}
        self._transfer_parameters = {}

        # convert base(s) to list for round-robin
        self.base = make_list(base)
        self.bases = {k: make_list(v) for k, v in six.iteritems(bases)} if bases else {}

        # prepare gfal options
        self.gfal_options = gfal_options or {}

        # prepare transfer config
        self.transfer_config = transfer_config or {}
        self.transfer_config.setdefault("checksum_check", False)
        self.transfer_config.setdefault("overwrite", True)
        self.transfer_config.setdefault("nbstreams", 1)

        # other configs
        self.atomic_contexts = atomic_contexts
        self.retries = retries
        self.retry_delay = retry_delay

    def __del__(self):
        # clear gfal contexts
        for ctx in self._contexts.values():
            try:
                del ctx
            except:
                pass
        self._contexts.clear()

    @contextmanager
    def context(self):
        # context objects are stored per pid, so create one if it does not exist yet
        pid = os.getpid()

        if pid not in self._contexts:
            self._contexts[pid] = ctx = gfal2.creat_context()
            for _type, args_list in six.iteritems(self.gfal_options):
                for args in args_list:
                    getattr(ctx, "set_opt_" + _type)(*args)

        # yield and optionally close it which frees potentially open connections
        try:
            yield self._contexts[pid]
        finally:
            if self.atomic_contexts and pid in self._contexts:
                del self._contexts[pid]
            gc.collect()

    @contextmanager
    def transfer_parameters(self, ctx):
        pid = os.getpid()

        if pid not in self._transfer_parameters:
            self._transfer_parameters[pid] = params = ctx.transfer_parameters()
            for key, value in six.iteritems(self.transfer_config):
                setattr(params, key, value)

        try:
            yield self._transfer_parameters[pid]
        finally:
            if self.atomic_contexts and pid in self._transfer_parameters:
                del self._transfer_parameters[pid]

    def url(self, path, cmd=None, rnd=True):
        # get potential bases for the given cmd
        bases = self.base
        if cmd:
            for cmd in make_list(cmd):
                if cmd in self.bases:
                    bases = self.bases[cmd]
                    break

        # select one when there are multple
        if not rnd or len(bases) == 1:
            base = bases[0]
        else:
            base = random.choice(bases)

        return os.path.join(base, path.strip("/"))

    def exists(self, path, stat=False):
        cmd = "stat" if stat else ("exists", "stat")
        with self.context() as ctx:
            try:
                _stat = ctx.stat(self.url(path, cmd=cmd))
                return _stat if stat else True
            except gfal2.GError:
                return None if stat else False

    @retry
    def stat(self, path):
        with self.context() as ctx:
            return ctx.stat(self.url(path, "stat"))

    @retry
    def chmod(self, path, perm):
        if perm:
            with self.context() as ctx:
                return ctx.chmod(self.url(path, "chmod"), perm)

    @retry
    def unlink(self, path):
        with self.context() as ctx:
            return ctx.unlink(self.url(path, "unlink"))

    @retry
    def rmdir(self, path):
        with self.context() as ctx:
            return ctx.rmdir(self.url(path, "rmdir"))

    @retry
    def mkdir(self, path, perm):
        with self.context() as ctx:
            return ctx.mkdir(self.url(path, "mkdir"), perm or 0o0770)

    @retry
    def mkdir_rec(self, path, perm):
        with self.context() as ctx:
            return ctx.mkdir_rec(self.url(path, ("mkdir_rec", "mkdir")), perm or 0o0770)

    @retry
    def listdir(self, path):
        with self.context() as ctx:
            return ctx.listdir(self.url(path, "listdir"))

    @retry
    def filecopy(self, src, dst):
        with self.context() as ctx, self.transfer_parameters(ctx) as params:
            return ctx.filecopy(params, src, dst)


class RemoteCache(object):

    TMP = "__TMP__"

    lock_postfix = ".lock"

    _instances = []

    def __new__(cls, *args, **kwargs):
        inst = object.__new__(cls)

        cls._instances.append(inst)

        return inst

    @classmethod
    def cleanup_all(cls):
        for inst in cls._instances:
            try:
                inst.cleanup()
            except:
                pass

    def __init__(self, fs, root=TMP, auto_flush=False, max_size=-1, dir_perm=0o0770,
            file_perm=0o0660, wait_delay=5, max_waits=120):
        object.__init__(self)

        # create a unique name based on fs attributes
        name = "{}_{}".format(fs.__class__.__name__, create_hash(fs.gfal.base[0]))

        # create the root dir, handle tmp
        root = os.path.expandvars(os.path.expanduser(root)) or self.TMP
        if not os.path.exists(root) and root == self.TMP:
            tmp_dir = Config.instance().get_expanded("target", "tmp_dir")
            base = tempfile.mkdtemp(dir=tmp_dir)
            auto_flush = True
        else:
            base = os.path.join(root, name)
            makedirs_perm(base, dir_perm)

        # save attributes and configs
        self.root = root
        self.fs_ref = weakref.ref(fs)
        self.base = base
        self.name = name
        self.auto_flush = auto_flush
        self.max_size = max_size
        self.dir_perm = dir_perm
        self.file_perm = file_perm
        self.wait_delay = wait_delay
        self.max_waits = max_waits

        # path to the global lock file which should guard global actions such as allocations
        self._global_lock_path = self._lock_path(os.path.join(base, "global"))

        # currently locked cache paths, only used to clean up broken files during cleanup
        self._locked_cpaths = set()

        logger.debug("created RemoteCache at '{}'".format(self.base))

    def __del__(self):
        self.cleanup()

    def __repr__(self):
        return "<{} '{}' at {}>".format(self.__class__.__name__, self.base, hex(id(self)))

    def __contains__(self, rpath):
        return os.path.exists(self.cache_path(rpath))

    @property
    def fs(self):
        return self.fs_ref()

    def cleanup(self):
        # full flush or remove open locks
        if getattr(self, "auto_flush", False):
            if os.path.exists(self.base):
                shutil.rmtree(self.base)
        else:
            for cpath in set(self._locked_cpaths):
                self._unlock(cpath)
                self._remove(cpath)
            self._locked_cpaths.clear()
        logger.debug("cleanup RemoteCache at '{}'".format(self.base))

    def cache_path(self, rpath):
        return os.path.join(self.base, self.fs.unique_basename(rpath))

    def _lock_path(self, cpath):
        return cpath + self.lock_postfix

    def is_locked_global(self):
        return os.path.exists(self._global_lock_path)

    def _is_locked(self, cpath):
        return os.path.exists(self._lock_path(cpath))

    def is_locked(self, rpath):
        return self._is_locked(self.cache_path(rpath))

    def _unlock_global(self):
        try:
            os.remove(self._global_lock_path)
        except OSError:
            pass

    def _unlock(self, cpath):
        try:
            os.remove(self._lock_path(cpath))
        except OSError:
            pass

    def _await_global(self, delay=None, max_waits=None, silent=False):
        delay = delay if delay is not None else self.wait_delay
        max_waits = max_waits if max_waits is not None else self.max_waits
        _max_waits = max_waits

        while self.is_locked_global():
            if max_waits <= 0:
                if silent:
                    return False
                else:
                    raise Exception("max_waits of {} exceeded while waiting for global lock".format(
                        _max_waits))

            time.sleep(delay)
            max_waits -= 1

        return True

    def _await(self, cpath, delay=None, max_waits=None, silent=False, skip_global=False):
        delay = delay if delay is not None else self.wait_delay
        max_waits = max_waits if max_waits is not None else self.max_waits
        _max_waits = max_waits

        # strategy: wait as long the file is locked and if the file size did not change, reduce
        # max_waits per iteration and raise when 0 is reached
        last_size = -1
        while self._is_locked(cpath) or (not skip_global and self.is_locked_global()):
            if max_waits <= 0:
                if silent:
                    return False
                else:
                    raise Exception("max_waits of {} exceeded while waiting for file '{}'".format(
                        _max_waits, cpath))

            time.sleep(delay)

            # only reduce max_waits when the file size did not change
            # otherwise, set it to its original value again
            if os.path.exists(cpath):
                size = os.stat(cpath).st_size
                if size != last_size:
                    last_size = size
                    max_waits = _max_waits
                    continue

            max_waits -= 1

        return True

    @contextmanager
    def _lock_global(self):
        self._await_global()

        try:
            with threading.Lock():
                with open(self._global_lock_path, "w") as f:
                    f.write("")
                os.utime(self._global_lock_path, None)

            yield
        finally:
            self._unlock_global()

    @contextmanager
    def _lock(self, cpath):
        lock_path = self._lock_path(cpath)

        self._await(cpath)

        try:
            with threading.Lock():
                with open(lock_path, "w") as f:
                    f.write("")
                self._locked_cpaths.add(cpath)
                os.utime(lock_path, None)

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

    def lock(self, rpath):
        return self._lock(self.cache_path(rpath))

    def allocate(self, size):
        logger.debug("allocating {0[0]:.2f} {0[1]} in cache '{1}'".format(human_bytes(size), self))

        with self._lock_global():
            # determine stats and current cache size
            file_stats = []
            for elem in os.listdir(self.base):
                if elem.endswith(self.lock_postfix):
                    continue
                cpath = os.path.join(self.base, elem)
                file_stats.append((cpath, os.stat(cpath)))
            current_size = sum(stat.st_size for _, stat in file_stats)

            # get the available space of the disk that contains the cache
            fs_stat = os.statvfs(self.base)
            full_size = fs_stat.f_frsize * fs_stat.f_blocks
            free_size = fs_stat.f_frsize * fs_stat.f_bavail

            # leave 10% total free space
            free_size -= 0.1 * full_size
            full_size *= 0.9

            # make sure max_size is always smaller than what is actually possible
            if self.max_size < 0:
                max_size = current_size + free_size
            else:
                max_size = min(self.max_size, current_size + free_size)

            # determine the size of files that need to be deleted
            delete_size = current_size + size - max_size
            if delete_size <= 0:
                logger.debug("cache space sufficient, {0[0]:.2f} {0[1]} bytes remaining".format(
                    human_bytes(-delete_size)))
                return

            logger.debug("need to delete {0[0]:.2f} {0[1]} bytes".format(human_bytes(delete_size)))

            # delete files, ordered by their access time, skip locked ones
            for cpath, cstat in sorted(file_stats, key=lambda tpl: tpl[1].st_atime):
                if self._is_locked(cpath):
                    continue
                self._remove(cpath)
                delete_size -= cstat.st_size
                if delete_size <= 0:
                    break
            else:
                logger.warning("could not allocate remaining {0[0]:.2f} {0[1]}".format(
                    human_bytes(delete_size)))

    def _touch(self, cpath, times=None):
        if os.path.exists(cpath):
            if user_owns_file(cpath):
                os.chmod(cpath, self.file_perm)
            os.utime(cpath, times)

    def touch(self, rpath, times=None):
        self._touch(self.cache_path(rpath), times=times)

    def _mtime(self, cpath):
        return os.stat(cpath).st_mtime

    def mtime(self, rpath):
        return self._mtime(self.cache_path(rpath))

    def _remove(self, cpath, lock=True):
        def remove():
            try:
                os.remove(cpath)
            except OSError:
                pass

        if lock:
            with self._lock(cpath):
                remove()
        else:
            remove()

    def remove(self, rpath, lock=True):
        return self._remove(self.cache_path(rpath), lock=lock)


atexit.register(RemoteCache.cleanup_all)


class RemoteFileSystem(FileSystem):

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
            atomic_contexts=False, retries=0, retry_delay=0, permissions=True, validate_copy=False,
            cache_config=None):
        FileSystem.__init__(self)

        # configure the gfal interface
        self.gfal = GFALInterface(base, bases, gfal_options=gfal_options,
            transfer_config=transfer_config, atomic_contexts=atomic_contexts, retries=retries,
            retry_delay=retry_delay)

        # store other configs
        self.permissions = permissions
        self.validate_copy = validate_copy

        # set the cache
        if cache_config is None:
            self.cache = None
        else:
            self.cache = RemoteCache(self, **cache_config)

    def __del__(self):
        # cleanup the cache
        if getattr(self, "cache", None):
            self.cache.cleanup()

    def __repr__(self):
        return "{}(base={}, {})".format(self.__class__.__name__, self.gfal.base[0], hex(id(self)))

    def __eq__(self, other):
        return self is other

    def is_local(self, path):
        return get_scheme(path) == "file"

    def hash(self, path, l=8):
        return str(abs(hash(self.__class__.__name__ + self.gfal.base[0] + self.abspath(path))))[-l:]

    def abspath(self, path):
        # due to the dynamic definition of remote bases, path is supposed to be already absolute
        # just handle leading and trailing slashes when there is not scheme
        return ("/" + path.strip("/")) if not get_scheme(path) else path

    def dirname(self, path):
        return FileSystem.dirname(self, self.abspath(path))

    def basename(self, path):
        return FileSystem.basename(self, self.abspath(path))

    def exists(self, path, stat=False):
        return self.gfal.exists(self.abspath(path), stat=stat)

    def stat(self, path, **kwargs):
        return self.gfal.stat(self.abspath(path), **kwargs)

    def chmod(self, path, perm, silent=True, **kwargs):
        if self.permissions and perm is not None:
            try:
                self.gfal.chmod(self.abspath(path), perm, **kwargs)
            except gfal2.GError:
                if not silent:
                    raise

    def remove(self, path, recursive=True, silent=True, **kwargs):
        if self.abspath(path) == "/":
            warnings.warn("refused request to remove base directory of {}".format(self))
            return

        # first, check if path refers to a file or directory, or if it exists at all
        try:
            is_dir = self.isdir(path, retries=0)
        except gfal2.GError:
            # path might not exist
            if silent:
                return
            else:
                raise

        if not is_dir:
            # remove the file
            self.gfal.unlink(path, **kwargs)
        else:
            # when recursive, remove content first
            if recursive:
                for elem in self.listdir(path, **kwargs):
                    self.remove(os.path.join(path, elem), recursive=True, silent=silent, **kwargs)
            # remove the directory itself
            self.gfal.rmdir(path, **kwargs)

    def mkdir(self, path, perm=None, recursive=True, silent=True, **kwargs):
        func = self.gfal.mkdir_rec if recursive else self.gfal.mkdir
        try:
            func(self.abspath(path), perm, **kwargs)
        except gfal2.GError:
            if not silent:
                raise

    def listdir(self, path, pattern=None, type=None, **kwargs):
        elems = self.gfal.listdir(self.abspath(path), **kwargs)

        # apply pattern filter
        if pattern is not None:
            elems = fnmatch.filter(elems, pattern)

        # apply type filter
        if type == "f":
            elems = [e for e in elems if not self.isdir(os.path.join(path, e), **kwargs)]
        elif type == "d":
            elems = [e for e in elems if self.isdir(os.path.join(path, e, **kwargs))]

        return elems

    def walk(self, path, max_depth=-1, **kwargs):
        # mimic os.walk with a max_depth and yield the current depth
        search_dirs = [(self.abspath(path), 0)]
        while search_dirs:
            (search_dir, depth) = search_dirs.pop(0)

            # check depth
            if max_depth >= 0 and depth > max_depth:
                continue

            # find dirs and files
            dirs = []
            files = []
            for elem in self.listdir(search_dir, **kwargs):
                if self.isdir(os.path.join(search_dir, elem), **kwargs):
                    dirs.append(elem)
                else:
                    files.append(elem)

            # yield everything
            yield (search_dir, dirs, files, depth)

            # use dirs to update search dirs
            search_dirs.extend((os.path.join(search_dir, d), depth + 1) for d in dirs)

    def glob(self, pattern, cwd=None, **kwargs):
        # helper to check if a string represents a pattern
        def is_pattern(s):
            return "*" in s or "?" in s

        # prepare pattern
        if cwd is not None:
            pattern = os.path.join(cwd, pattern)

        # split the pattern to determine the search path, i.e. the leading part that does not
        # contain any glob chars, e.g. "foo/bar/test*/baz*" -> "foo/bar"
        search_dir = []
        patterns = []
        for part in pattern.split("/"):
            if not patterns and not is_pattern(part):
                search_dir.append(part)
            else:
                patterns.append(part)
        search_dir = self.abspath("/".join(search_dir))

        # walk trough the search path and use fnmatch for comparison
        elems = []
        max_depth = len(patterns) - 1
        for root, dirs, files, depth in self.walk(search_dir, max_depth=max_depth, **kwargs):
            # get the current pattern
            pattern = patterns[depth]

            # when we are still below the max depth, filter dirs
            # otherwise, filter files and dirs to select
            if depth < max_depth:
                dirs[:] = fnmatch.filter(dirs, pattern)
            else:
                elems += [os.path.join(root, e) for e in fnmatch.filter(dirs + files, pattern)]

        # cut the cwd if there was any
        if cwd is not None:
            elems = [os.path.relpath(e, cwd) for e in elems]

        return elems

    # atomic copy
    def _atomic_copy(self, src, dst, validate=None, **kwargs):
        if validate is None:
            validate = self.validate_copy

        src = self.abspath(src)
        dst = self.abspath(dst)

        self.gfal.filecopy(src, dst, **kwargs)

        if validate:
            fs = self if not self.is_local(dst) else _local_fs
            if not fs.exists(dst):
                raise Exception("validation failed after copying {} to {}".format(src, dst))

    # generic copy with caching ability (local paths must have "file" scheme)
    def _cached_copy(self, src, dst, cache=None, validate=None, **kwargs):
        if cache is None:
            cache = self.cache is not None
        elif cache and self.cache is None:
            cache = False

        # ensure absolute paths
        src = self.abspath(src)
        dst = self.abspath(dst) if dst else None

        # determine the copy mode for code readability
        # (remote-remote: "rr", remote-local: "rl", remote-cache: "rc", ...)
        src_local = self.is_local(src)
        dst_local = dst and self.is_local(dst)
        mode = "rl"[src_local] + ("rl"[dst_local] if dst is not None else "c")

        # disable caching when the mode is local-local, local-cache or remote-remote
        if mode in ("ll", "lc", "rr"):
            cache = False

        # dst can be None, but in this case, caching should be enabled
        if dst is None and not cache:
            raise Exception("copy destination must not be empty when caching is disabled")

        # paths including scheme and base
        full_src = src if has_scheme(src) else self.gfal.url(src, cmd="filecopy")
        full_dst = None
        if dst:
            full_dst = dst if has_scheme(dst) else self.gfal.url(dst, cmd="filecopy")

        if cache:
            kwargs_no_retries = kwargs.copy()
            kwargs_no_retries["retries"] = 0
            kwargs_no_retries = kwargs

            # handle 3 cases: lr, rl, rc
            if mode == "lr":
                # strategy: copy to remote, copy to cache, sync stats

                # copy to remote, no need to validate as we need the stat anyway
                self._atomic_copy(src, full_dst, validate=False, **kwargs)
                rstat = self.stat(dst, **kwargs_no_retries)

                # remove the cache entry
                if dst in self.cache:
                    self.cache.remove(dst)

                # allocate cache space and copy to cache
                lstat = _local_fs.stat(src)
                self.cache.allocate(lstat.st_size)
                full_cdst = add_scheme(self.cache.cache_path(dst), "file")
                with self.cache.lock(dst):
                    self._atomic_copy(src, full_cdst, validate=False)
                    self.cache.touch(dst, (int(time.time()), rstat.st_mtime))

                return dst

            else:  # rl, rc
                # strategy: copy to cache when not up to date, sync stats, opt. copy to local

                # check if cached and up to date
                rstat = self.stat(src, **kwargs_no_retries)
                full_csrc = add_scheme(self.cache.cache_path(src), "file")
                with self.cache.lock(src):
                    # in cache and outdated?
                    if src in self.cache and abs(self.cache.mtime(src) - rstat.st_mtime) > 1:
                        self.cache.remove(src, lock=False)
                    # in cache at all?
                    if src not in self.cache:
                        self.cache.allocate(rstat.st_size)
                        self._atomic_copy(full_src, full_csrc, validate=validate, **kwargs)
                        self.cache.touch(src, (int(time.time()), rstat.st_mtime))

                if mode == "rl":
                    # copy to local without permission bits
                    copy_no_perm(remove_scheme(full_csrc), remove_scheme(full_dst))
                    return dst
                else:  # rc
                    return full_csrc

        else:
            # simply copy and return the dst path
            self._atomic_copy(full_src, full_dst, validate=validate, **kwargs)

            return full_dst if dst_local else dst

    def copy(self, src, dst, dir_perm=None, cache=None, validate=None, **kwargs):
        # dst might be an existing directory
        if dst:
            if self.is_local(dst):
                if _local_fs.exists(dst) and _local_fs.isdir(dst):
                    # add src basename to dst
                    dst = os.path.join(dst, os.path.basename(src))
                else:
                    # create missing dirs
                    dst_dir = _local_fs.dirname(dst)
                    if dst_dir and not _local_fs.exists(dst_dir):
                        _local_fs.mkdir(dst_dir, perm=dir_perm, recursive=True)
            else:
                rstat = self.exists(dst, stat=True)
                if rstat and stat.S_ISDIR(rstat.st_mode):
                    # add src basename to dst
                    dst = os.path.join(dst, os.path.basename(src))
                else:
                    # create missing dirs
                    dst_dir = self.dirname(dst)
                    if dst_dir and not self.exists(dst_dir):
                        self.mkdir(dst_dir, perm=dir_perm, recursive=True, **kwargs)

        # copy the file
        return self._cached_copy(src, dst, cache=cache, validate=validate, **kwargs)

    def move(self, src, dst, dir_perm=None, validate=None, **kwargs):
        if not dst:
            raise Exception("move requires dst to be set")

        # copy the file
        dst = self.copy(src, dst, dir_perm=dir_perm, cache=False, validate=validate, **kwargs)

        # remove the src
        if self.is_local(src):
            _local_fs.remove(src)
        else:
            self.remove(src, **kwargs)

        return dst

    @contextmanager
    def open(self, path, mode, cache=None, **kwargs):
        if cache is None:
            cache = self.cache is not None
        elif cache and self.cache is None:
            cache = False

        path = self.abspath(path)

        yield_path = kwargs.pop("_yield_path", False)

        if mode == "r":
            if cache:
                lpath = self._cached_copy(path, None, cache=True, **kwargs)
                lpath = remove_scheme(lpath)
            else:
                tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
                lpath = tmp.path

                self._cached_copy(path, add_scheme(lpath, "file"), cache=False, **kwargs)
            try:
                if yield_path:
                    yield lpath
                else:
                    f = open(lpath, "r")
                    yield f
                    if not f.closed:
                        f.close()
            finally:
                if not cache:
                    del tmp

        elif mode == "w":
            tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
            lpath = tmp.path

            try:
                if yield_path:
                    yield lpath
                else:
                    f = open(lpath, "w")
                    yield f
                    if not f.closed:
                        f.close()

                if tmp.exists():
                    self._cached_copy(add_scheme(lpath, "file"), path, cache=cache, **kwargs)
            finally:
                del tmp

        else:
            raise Exception("unknown mode {}, use r or w".format(mode))

    def load(self, path, formatter, *args, **kwargs):
        with self.open(path, "r", _yield_path=True) as lpath:
            if formatter == AUTO_FORMATTER:
                errors = []
                for f in find_formatters(lpath, silent=False):
                    try:
                        return f.load(lpath, *args, **kwargs)
                    except ImportError as e:
                        errors.append(str(e))
                else:
                    raise Exception("could not automatically load '{}', errors:\n{}".format(
                        lpath, "\n".join(errors)))
            else:
                return get_formatter(formatter, silent=False).load(lpath, *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        with self.open(path, "w", _yield_path=True) as lpath:
            if formatter == AUTO_FORMATTER:
                errors = []
                for f in find_formatters(lpath, silent=False):
                    try:
                        return f.dump(lpath, *args, **kwargs)
                    except ImportError as e:
                        errors.append(str(e))
                else:
                    raise Exception("could not automatically dump '{}', errors:\n{}".format(
                        lpath, "\n".join(errors)))
            else:
                return get_formatter(formatter, silent=False).dump(lpath, *args, **kwargs)


class RemoteTarget(FileSystemTarget):

    fs = None

    def __init__(self, path, fs, **kwargs):
        if not isinstance(fs, RemoteFileSystem):
            raise TypeError("fs must be a {} instance, is {}".format(RemoteFileSystem, fs))

        self.fs = fs
        self._path = None

        FileSystemTarget.__init__(self, path, **kwargs)

    @property
    def init_args(self):
        return (self.fs,)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        if os.path.normpath(path).startswith(".."):
            raise ValueError("path {} forbidden, surpasses file system root".format(path))

        self._path = self.fs.abspath(path)

    def url(self, *args, **kwargs):
        return self.fs.gfal.url(self.path, *args, **kwargs)


class RemoteFileTarget(RemoteTarget, FileSystemFileTarget):

    def copy_to_local(self, dst=None, dir_perm=None, **kwargs):
        if dst:
            dst = add_scheme(_local_fs.abspath(get_path(dst)), "file")
        return FileSystemFileTarget.copy_to(self, dst, dir_perm=dir_perm, **kwargs)

    def copy_from_local(self, src=None, dir_perm=None, **kwargs):
        src = add_scheme(_local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.copy_from(self, src, dir_perm=dir_perm, **kwargs)

    def move_to_local(self, dst=None, dir_perm=None, **kwargs):
        if dst:
            dst = add_scheme(_local_fs.abspath(get_path(dst)), "file")
        return FileSystemFileTarget.move_to(self, dst, dir_perm=dir_perm, **kwargs)

    def move_from_local(self, src=None, dir_perm=None, **kwargs):
        src = add_scheme(_local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.move_from(self, src, dir_perm=dir_perm, **kwargs)

    @contextmanager
    def localize(self, mode="r", perm=None, parent_perm=None, **kwargs):
        if mode not in ("r", "w"):
            raise Exception("unknown mode '{}', use r or w".format(mode))

        if mode == "r":
            with self.fs.open(self.path, "r", _yield_path=True, **kwargs) as lpath:
                yield LocalFileTarget(lpath)

        else:  # w
            tmp = LocalFileTarget(is_tmp=self.ext() or True)

            try:
                yield tmp

                if tmp.exists():
                    self.copy_from_local(tmp, dir_perm=parent_perm, **kwargs)
                    self.chmod(perm)
                else:
                    logger.warning("cannot move non-existing localized file target {!r}".format(
                        self))
            finally:
                del tmp


class RemoteDirectoryTarget(RemoteTarget, FileSystemDirectoryTarget):

    pass


RemoteTarget.file_class = RemoteFileTarget
RemoteTarget.directory_class = RemoteDirectoryTarget
