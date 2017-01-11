# -*- coding: utf-8 -*-

"""
Remote filesystem and targets, based on gfal2 bindings.
"""


__all__ = []


import os
import shutil
import stat
import time
import fnmatch
import tempfile
import weakref
import urlparse
import random
import math
import re
import functools
import atexit
import gc
from contextlib import contextmanager
from multiprocessing.pool import ThreadPool

from law.config import Config
from law.target.file import FileSystem, FileSystemTarget, FileSystemFileTarget, \
                            FileSystemDirectoryTarget
from law.target.local import LocalFileTarget, LocalDirectoryTarget
from law.util import make_list


try:
    import gfal2
    HAS_GFAL2 = True

    # configure gfal2 logging
    import logging
    logger = logging.getLogger("gfal2")
    logger.addHandler(logging.StreamHandler())
    level = Config.instance().get("target", "gfal2_log_level")
    logger.setLevel(getattr(logging, level, logging.NOTSET))

except ImportError:
    class gfal2Dummy(object):
        def __getattr__(self, attr):
            raise Exception("trying to access 'gfal2.%s', but gfal2 is not installed" % attr)
    gfal2 = gfal2Dummy()
    HAS_GFAL2 = False


def retry(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        retry = kwargs.pop("retry", None)
        if retry is None:
            retry = self.retry

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
                    if attempt > retry:
                        raise e
                    else:
                        time.sleep(delay)
        except Exception as e:
            tpl = (func.__name__,  attempt, args, kwargs)
            e.message += "\nfailed: %s, attempt: %i, args: %s, kwargs: %s" % tpl
            raise e

    return wrapper


class Gfal2Context(object):

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
                 reset_context=False, retry=0, retry_delay=0):
        super(Gfal2Context, self).__init__()

        # cache for gfal context objects per pid for thread safety
        self._contexts = {}

        # convert base(s) to list for round-robin
        if not isinstance(base, (list, tuple)):
            base = [base]
        self.base = base

        if bases is None:
            bases = {}
        else:
            for key, value in bases.items():
                if not isinstance(value, (list, tuple)):
                    bases[key] = [value]
        self.bases = bases

        # prepare gfal options
        if gfal_options is None:
            gfal_options = {}
        self.gfal_options = gfal_options

        # prepare transfer config
        if transfer_config is None:
            transfer_config = {}
        transfer_config.setdefault("checksum_check", False)
        transfer_config.setdefault("overwrite", True)
        transfer_config.setdefault("nbstreams", 1)
        self.transfer_config = transfer_config

        # other configs
        self.reset_context = reset_context
        self.retry = retry
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
        pid = os.getpid()
        if pid not in self._contexts:
            self._contexts[pid] = gfal2.creat_context()
            # apply options
            for _type, args_list in self.gfal_options.items():
                for args in args_list:
                    getattr(self._contexts[pid], "set_opt_" + _type)(*args)
        try:
            yield self._contexts[pid]
        finally:
            if self.reset_context and pid in self._contexts:
                del self._contexts[pid]
            gc.collect()

    def url(self, rpath, cmd=None):
        base = random.choice(self.bases.get(cmd, self.base))
        return os.path.join(base, rpath.strip("/"))

    @retry
    def exists(self, rpath):
        with self.context() as ctx:
            try:
                ctx.stat(self.url(rpath, cmd="stat"))
                return True
            except gfal2.GError:
                return False

    @retry
    def stat(self, rpath):
        with self.context() as ctx:
            return ctx.stat(self.url(rpath, cmd="stat"))

    @retry
    def chmod(self, rpath, mode):
        with self.context() as ctx:
            return ctx.chmod(self.url(rpath, cmd="chmod"), mode)

    @retry
    def unlink(self, rpath):
        with self.context() as ctx:
            return ctx.unlink(self.url(rpath, cmd="unlink"))

    @retry
    def rmdir(self, rpath):
        with self.context() as ctx:
            return ctx.rmdir(self.url(rpath, cmd="rmdir"))

    @retry
    def mkdir(self, rpath, mode):
        with self.context() as ctx:
            return ctx.mkdir(self.url(rpath, cmd="mkdir"), mode)

    @retry
    def mkdir_rec(self, rpath, mode):
        with self.context() as ctx:
            return ctx.mkdir_rec(self.url(rpath, cmd="mkdir_rec"), mode)

    @retry
    def listdir(self, rpath):
        with self.context() as ctx:
            return ctx.listdir(self.url(rpath, cmd="listdir"))

    @retry
    def filecopy(self, source, target):
        with self.context() as ctx:
            params = ctx.transfer_parameters()
            for key, value in self.transfer_config.items():
                setattr(params, key, value)
            return ctx.filecopy(params, source, target)


class RemoteFileSystem(FileSystem):
    """
    Possible keys/cmds for remoteBases:
        stat, chmod, unlink, rmdir, mkdir, mkdir_rec, listdir, filecopy
    """

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
                 reset_context=False, retry=0, retry_delay=0, permissions=True, threads=0,
                 validate_copy=False, cache_config=None):
        super(RemoteFileSystem, self).__init__()

        # create a gfal context wrapper
        self.gfal = Gfal2Context(base, bases, gfal_options=gfal_options,
                                 transfer_config=transfer_config, reset_context=reset_context,
                                 retry=retry, retry_delay=retry_delay)

        # thread pools per pid
        self.threads = 0 #threads, disabled temporarily
        self._pools = {}

        # store args
        self.permissions  = permissions
        self.validate_copy = validate_copy

        # set the cache
        if cache_config is None:
            self.cache = None
        else:
            self.cache = RemoteCache(self, **cache_config)

    def __del__(self):
        try:
            del self.gfal
        except:
            pass
        self.gfal = None

        try:
            del self.cache
        except:
            pass
        self.cache = None

    def __repr__(self):
        tpl = (self.__class__.__name__, self.gfal.base[0], hex(id(self)))
        return "<%s '%s' at %s>" % tpl

    @property
    def pool(self):
        if self.threads <= 0:
            return None

        pid = os.getpid()
        if pid not in self._pools:
            self._pools[pid] = ThreadPool(self.threads)

        return self._pools[pid]

    def abspath(self, rpath):
        return "/" + rpath.strip("/")

    def dirname(self, rpath):
        return super(RemoteFileSystem, self).dirname(self.abspath(rpath))

    def basename(self, rpath):
        return super(RemoteFileSystem, self).basename(self.abspath(rpath))

    def get_scheme(self, path):
        scheme = urlparse.urlparse(path).scheme
        return scheme if re.match("^[a-zA-Z]+$", scheme) else None

    def add_scheme(self, path, scheme):
        return path if self.get_scheme(path) else "%s://%s" % (scheme, path)

    def exists(self, rpath):
        return self.gfal.exists(self.abspath(rpath), retry=None)

    def stat(self, rpath, retry=None):
        return self.gfal.stat(self.abspath(rpath), retry=retry)

    def chmod(self, rpath, mode, retry=None):
        if self.permissions:
            self.gfal.chmod(self.abspath(rpath), mode, retry=retry)

    def isdir(self, rpath, retry=None):
        return stat.S_ISDIR(self.stat(rpath, retry=retry).st_mode)

    def remove(self, rpath, recursive=True, silent=True, retry=None):
        if self.abspath(rpath) == "/":
            return

        try:
            is_dir = self.isdir(rpath, retry=retry)
        except gfal2.GError:
            if silent:
                return
            else:
                raise

        if not is_dir:
            self.gfal.unlink(rpath, retry=retry)
        else:
            if recursive:
                for elem in self.listdir(rpath, retry=retry):
                    self.remove(os.path.join(rpath, elem), recursive=True, silent=silent,
                                retry=retry)
            self.gfal.rmdir(rpath, retry=retry)

    def mkdir(self, rpath, mode=0o0770, recursive=True, silent=True, retry=None):
        try:
            if recursive:
                self.gfal.mkdir_rec(self.abspath(rpath), mode, retry=retry)
            else:
                self.gfal.mkdir(self.abspath(rpath), mode, retry=retry)
        except gfal2.GError:
            if not silent:
                raise

    def listdir(self, rpath, pattern=None, type=None, retry=None):
        elems = self.gfal.listdir(self.abspath(rpath), retry=retry)
        if pattern is not None:
            elems = [elem for elem in elems if fnmatch.fnmatchcase(elem, pattern)]
        if type == "f":
            elems = [elem for elem in elems if not self.isdir(os.path.join(rpath, elem), retry=retry)]
        elif type == "d":
            elems = [elem for elem in elems if self.isdir(os.path.join(rpath, elem), retry=retry)]
        return elems

    def walk(self, rpath, retry=None):
        search_dirs = [self.abspath(rpath)]
        while len(search_dirs):
            search_dir = search_dirs.pop(0)
            dirs, files = [], []
            for elem in self.listdir(search_dir, retry=retry):
                if self.isdir(os.path.join(search_dir, elem), retry=retry):
                    dirs.append(elem)
                else:
                    files.append(elem)
            yield (search_dir, dirs, files)
            search_dirs.extend(os.path.join(search_dir, d) for d in dirs)

    def glob(self, pattern, cwd=None, retry=None):
        pattern = pattern.strip("/")
        if not pattern:
            return []

        if cwd is not None:
            cwd = cwd.strip("/")
            pattern = os.path.join(cwd, pattern)

        def is_pattern(s):
            return "*" in s or "?" in s

        parts = pattern.split("/")

        # determine the search path, i.e. the part of the path that does not
        # contain any glob chars, e.g. "foo/bar/test*/baz*" -> "foo/bar"
        glob_idxs   = [i for i, part in enumerate(parts) if is_pattern(part)]
        search_path = "" if not len(glob_idxs) else os.path.join("", *parts[:glob_idxs[0]])

        # walk trough the search path and use fnmatchcase for comparison
        targets = []
        for root, dirs, files in self.walk(search_path, retry=retry):
            level = root.count("/") + (0 if root == search_path else 1)
            if level >= len(parts):
                break

            part = parts[level]

            if level < len(parts) - 1:
                part = parts[level]
                if not is_pattern(part):
                    dirs[:] = [part] if part in dirs else []
                else:
                    dirs[:] = [d for d in dirs if fnmatch.fnmatchcase(d, part)]
            else:
                part = parts[level]
                if not is_pattern(part):
                    elems = [part] if part in dirs + files else []
                else:
                    elems = [elem for elem in dirs + files if fnmatch.fnmatchcase(elem, part)]
                targets.extend(os.path.join(root, elem) for elem in elems)

        # cut the cwd if there was any
        if cwd is not None:
            targets = [target[len(cwd):] for target in targets]

        return [target.strip("/") for target in targets]

    def copy(self, source, target, validate=None, retry=None):
        if validate is None:
            validate = self.validate_copy

        self.gfal.filecopy(source, target, retry=retry)

        if validate:
            try:
                with self.gfal.context() as ctx:
                    ctx.stat(target)
            except gfal2.GError:
                raise Exception("copy from %s to %s fails validation" % (source, target))

    def _fetch(self, rpath, lpath=None, cache=True, **kwargs):
        # overwrite cache when global setting is False
        cache = cache if self.cache else False

        # check arguments
        if lpath is not None and self.get_scheme(lpath) not in ("file", None):
            raise ValueError("cannot fetch to a non-local destination")
        elif not self.cache and lpath is None:
            raise ValueError("lpath is required when caching is disabled")

        rpath = self.abspath(rpath)
        rurl = self.gfal.url(rpath, cmd="filecopy")
        lpath = lpath if lpath is None else os.path.abspath(lpath)

        if cache:
            cpath = self.cache.cache_path(rpath)
            full_cpath = self.addScheme(cpath, "file")

            with self.cache.require_lock(rpath):
                # determine stats
                rstat = self.stat(rpath)

                # when already cached, remove when outdated, otherwise prevent new fetching
                fetch = True
                if rpath in self.cache:
                    lmtime = os.stat(cpath).st_mtime
                    rmtime = rstat.st_mtime
                    if abs(lmtime - rmtime) > 1:
                        self.cache.remove(rpath, lock=False)
                    else:
                        fetch = False

                # fetch
                if fetch:
                    # allocate cache size
                    self.cache.allocate(rstat.st_size)

                    # actual copy
                    self.copy(rurl, full_cpath, **kwargs)

                # change atime and mtime
                self.cache.touch(rpath, (int(time.time()), rstat.st_mtime))

            # when we don't need to copy the file to an lpath, we're done here
            # otherwise, copy again
            if lpath is None:
                return cpath
            else:
                self.copy(full_cpath, self.add_scheme(lpath, "file"), **kwargs)
                return lpath

        else:
            self.copy(rurl, self.add_scheme(lpath, "file"), **kwargs)
            return lpath

    def fetch(self, rpaths, lpaths=None, cache=True, **kwargs):
        # alias
        caches = cache if self.cache else False

        multi = isinstance(rpaths, (list, tuple, set))

        # check arguments
        rpaths = make_list(rpaths)
        n = len(rpaths)

        if lpaths is None:
            lpaths = [None] * n
        else:
            lpaths = make_list(lpaths)

        caches = make_list(caches)
        if len(caches) == 1:
            caches *= n

        if n != len(lpaths):
            raise ValueError("rpaths and lpaths count must match")
        if n != len(caches):
            raise ValueError("rpaths and cache count must match")
        if not n:
            return [] if multi else None

        fetch = lambda tpl: self._fetch(tpl[0], tpl[1], cache=tpl[2], **kwargs)
        tpls = zip(rpaths, lpaths, caches)

        if not self.pool:
            results = map(fetch, tpls)
        else:
            results = self.pool.map(fetch, tpls)

        return results if multi else results[0]

    def _put(self, lpath, rpath, cache=True, **kwargs):
        # overwrite cache when global setting is False
        cache = cache if self.cache else False

        # check arguments
        if self.get_scheme(lpath) not in ("file", None):
            raise ValueError("cannot put from a non-local destination")

        full_lpath = self.add_scheme(os.path.abspath(lpath), "file")
        rpath = self.abspath(rpath)
        rurl = self.gfal.url(rpath, cmd="filecopy")

        if not cache:
            self.copy(full_lpath, rurl, **kwargs)
            rstat = self.stat(rpath)
        else:
            cpath = self.cache.cache_path(rpath)
            full_cpath = self.add_scheme(cpath, "file")

            with self.cache.require_lock(rpath):
                # allocate cache size
                self.cache.allocate(os.stat(lpath).st_size)

                # copy to cache
                self.copy(full_lpath, full_cpath, **kwargs)

                # actual copy
                self.copy(full_cpath, rurl, **kwargs)

                # determine stats to update atime and mtime of cache file
                rstat = self.stat(rpath)
                self.cache.touch(rpath, (int(time.time()), rstat.st_mtime))

    def put(self, lpaths, rpaths, cache=True, **kwargs):
        # alias
        caches = cache if self.cache else False

        # check arguments
        rpaths = make_list(rpaths)
        lpaths = make_list(lpaths)
        n = len(rpaths)
        caches = make_list(caches)
        if len(caches) == 1:
            caches *= n

        if n != len(lpaths):
            raise ValueError("rpaths and lpaths count must match")
        if n != len(caches):
            raise ValueError("rpaths and cache count must match")
        if not n:
            return

        put = lambda tpl: self._put(tpl[0], tpl[1], cache=tpl[2], **kwargs)
        tpls = zip(lpaths, rpaths, caches)
        if not self.pool:
            map(put, tpls)
        else:
            self.pool.map(put, tpls)

    def open(self, rpath, mode, retry=None):
        if mode == "r":
            lpath = self.fetch(rpath, retry=retry)
            return open(lpath, "r")
        elif mode == "w":
            return RemoteFileProxy(self, rpath, retry=retry)
        else:
            raise Exception("invalid mode '%s'" % mode)


class RemoteCache(object):

    TMP = "__TMP__"

    lock_postfix = ".lock"

    _instances = []

    def __new__(cls, *args, **kwargs):
        inst = super(RemoteCache, cls).__new__(cls, *args, **kwargs)

        cls._instances.append(inst)

        return inst

    @classmethod
    def cleanup(cls):
        for inst in cls._instances:
            try:
                inst.__del__()
            except:
                pass

    def __init__(self, fs, root=TMP, cleanup=True, max_size=-1, dir_mode=0o0770,
                 file_mode=0o0660, wait_delay=5, max_waits=120):
        super(RemoteCache, self).__init__()

        base = fs.gfal.base[0]
        name = "%s_%s" % (fs.__class__.__name__, hex(abs(hash(base)))[2:])

        # handle tmp root
        root = os.path.expandvars(os.path.expanduser(root))
        if not os.path.exists(root) and root == self.TMP:
            base = tempfile.mkdtemp()
            cleanup = True
        else:
            base = os.path.join(root, name)
            if not os.path.exists(base):
                umask = os.umask(0o0000)
                os.makedirs(base, dir_mode)
                os.umask(umask)

        self.root       = root
        self.fs         = weakref.ref(fs)
        self.base       = base
        self.name       = name
        self.cleanup    = cleanup
        self.max_size   = max_size
        self.dir_mode   = dir_mode
        self.file_mode  = file_mode
        self.wait_delay = wait_delay
        self.max_waits  = max_waits

        self._global_lock_path = self._lock_path(os.path.join(base, "global"))

        # currently locked cache paths, only used to clean up broken files in destructor
        self._locked_cpaths = set()

    def __del__(self):
        # cleanup
        if getattr(self, "cleanup", False):
            if os.path.exists(self.base):
                shutil.rmtree(self.base)
        else:
            # remove all files that belong to cpaths that are currently in use
            for cpath in set(self._locked_cpaths):
                self._unlock(cpath)
                self._remove(cpath)
            self._locked_cpaths.clear()

            # release the global lock
            self._unlock_global()

    def __repr__(self):
        tpl = (self.__class__.__name__, self.base, hex(id(self)))
        return "<%s '%s' at %s>" % tpl

    def __contains__(self, rpath):
        return os.path.exists(self.cache_path(rpath))

    def path_hash(self, rpath):
        h = hex(abs(hash(self.fs().gfal.url(rpath))))[2:]
        return "%s_%s" % (h, os.path.basename(rpath))

    def _lock_path(self, cpath):
        return cpath + self.lock_postfix

    def cache_path(self, rpath):
        return os.path.join(self.base, self.path_hash(rpath))

    def lock_path(self, rpath):
        return self._lock_path(self.cache_path(rpath))

    def _unlock(self, cpath):
        lock_path = self._lock_path(cpath)
        if os.path.exists(lock_path):
            try:
                os.remove(lock_path)
            except:
                pass

    def _unlock_global(self):
        if os.path.exists(self._global_lock_path):
            try:
                os.remove(self._global_lock_path)
            except:
                pass

    def _locked(self, cpath):
        return os.path.exists(self._lock_path(cpath))

    def locked(self, rpath):
        return self._locked(self.cache_path(rpath))

    def _locked_global(self):
        return os.path.exists(self._global_lock_path)

    @contextmanager
    def _require_lock(self, cpath):
        self._wait(cpath)

        lockPath = self._lock_path(cpath)

        try:
            with open(lockPath, "w") as f:
                self._locked_cpaths.add(cpath)
                os.utime(lockPath, None)
            yield
        except:
            self._remove(cpath, lock=False)
            raise
        finally:
            if self._locked(cpath):
                os.remove(self._lock_path(cpath))
            if cpath in self._locked_cpaths:
                self._locked_cpaths.remove(cpath)

    def require_lock(self, rpath):
        return self._require_lock(self.cache_path(rpath))

    @contextmanager
    def _require_lock_global(self):
        self._wait_global()

        try:
            with open(self._global_lock_path, "w") as f:
                os.utime(self._global_lock_path, None)
            yield
        finally:
            if self._locked_global():
                os.remove(self._global_lock_path)

    def _wait(self, cpath, delay=None, max_waits=None, silent=False):
        if delay is None:
            delay = self.wait_delay
        if max_waits is None:
            max_waits = self.max_waits
        _max_waits = max_waits

        last_size = -1
        while self._locked(cpath):
            time.sleep(0.05 + delay * (1. - math.exp((max_waits - _max_waits)/5.)))

            if os.path.exists(cpath):
                size = os.stat(cpath).st_size
                if size != last_size:
                    last_size = size
                    max_waits = _max_waits
                    continue

            max_waits -= 1
            if max_waits <= 0:
                if silent:
                    return False
                else:
                    raise Exception("max_waits exceeded while waiting for lock on '%s'" % cpath)

        return True

    def _wait_global(self, delay=None, max_waits=None, silent=False):
        if delay is None:
            delay = self.wait_delay
        if max_waits is None:
            max_waits = self.max_waits
        _max_waits = max_waits

        while self._locked_global():
            time.sleep(0.05 + delay * (1. - math.exp((max_waits - _max_waits)/5.)))

            max_waits -= 1
            if max_waits <= 0:
                if silent:
                    return False
                else:
                    raise Exception("max_waits exceeded while waiting for global lock")

        return True

    def allocate(self, size):
        with self._require_lock_global():
            # determine stats and current cache size
            file_stats = []
            for elem in os.listdir(self.base):
                if elem.endswith(self.lock_postfix):
                    continue
                cpath = os.path.join(self.base, elem)
                file_stats.append((cpath, os.stat(cpath)))
            current_size = sum(stat.st_size for _, stat in file_stats)

            fs_stat = os.statvfs(self.base)
            free_size = fs_stat.f_bfree * fs_stat.f_bsize * 0.9 # leave 10% free space

            if self.max_size < 0:
                max_size = current_size + free_size
            else:
                max_size = min(self.max_size, current_size + free_size)

            delete_size = size + current_size - max_size
            if delete_size <= 0:
                return

            for cpath, stat in sorted(file_stats, key=lambda tpl: tpl[1].st_atime):
                if self._locked(cpath):
                    continue
                self._remove(cpath)
                delete_size -= stat.st_size
                if delete_size <= 0:
                    break
            else:
                print("warning, could not allocate size %d" % size)

    def touch(self, rpath, times=None):
        cpath = self.cache_path(rpath)
        if os.path.exists(cpath):
            os.chmod(cpath, self.file_mode)
            if times is not None:
                os.utime(cpath, times)

    def _remove(self, cpath, lock=True):
        def remove():
            if os.path.exists(cpath):
                os.remove(cpath)
        if lock:
            with self._require_lock(cpath):
                remove()
        else:
            remove()

    def remove(self, rpath, lock=True):
        return self._remove(self.cache_path(rpath), lock=lock)


atexit.register(RemoteCache.cleanup)


class RemoteTarget(FileSystemTarget):

    fs = None

    def __init__(self, path, fs, exists=None):
        if not isinstance(fs, RemoteFileSystem):
            raise ValueError("fs is not a RemoteFileSystem instance")

        self.fs = fs
        self._path = None

        FileSystemTarget.__init__(self, path, exists=exists)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, rpath):
        if os.path.normpath(rpath).startswith(".."):
            raise ValueError("paths above the base path of the file system are forbidden")

        self._path = self.fs.abspath(rpath)

    @property
    def parent(self):
        dirname = self.dirname
        return self.directory_class(dirname, self.fs) if dirname is not None else None


class RemoteFileTarget(RemoteTarget, FileSystemFileTarget):

    def touch(self, content=" ", mode=0o0660):
        parent = self.parent
        if parent is not None:
            parent.touch()
        with self.open("w") as f:
            if content:
                f.write(content)
        self.fs.chmod(self.path, mode)

    def open(self, mode):
        return self.fs.open(self.path, mode)

    def fetch(self, lpath=None, mode=0o0660, **kwargs):
        if isinstance(lpath, FileSystemTarget):
            lpath = lpath.path

        if lpath is not None:
            lpath = os.path.expandvars(os.path.expanduser(lpath))
            if os.path.isdir(lpath):
                lpath = os.path.join(lpath, self.basename)

        lpath = self.fs.fetch(self.path, lpaths=lpath, **kwargs)
        os.chmod(lpath, mode)

        return lpath

    def put(self, lpath, **kwargs):
        if isinstance(lpath, FileSystemTarget):
            lpath = lpath.path

        return self.fs.put(lpath, self.path, **kwargs)

    @contextmanager
    def localize(self, path=None, is_tmp=True, skip_parent=False, mode=0o0660, cache=True,
                 retry=None):
        tmp = LocalFileTarget(path, is_tmp=is_tmp)
        try:
            yield tmp
            if not skip_parent:
                self.parent.touch()
            self.put(tmp, cache=cache, retry=retry)
            self.chmod(mode, retry=retry)
        finally:
            del tmp


class RemoteDirectoryTarget(RemoteTarget, FileSystemDirectoryTarget):

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

        return cls(path, self.fs)

    def touch(self, mode=0o0770, **kwargs):
        self.fs.mkdir(self.path, mode=mode, recursive=True, **kwargs)

    def open(self):
        raise NotImplementedError()

    def fetch(self, rpaths=None, pattern=None, lpaths=None, lbase=None, **kwargs):
        if rpaths is None:
            rpaths = self.listdir(pattern=pattern)
            lpaths = None
        else:
            rpaths = [rpath.strip("/") for rpath in make_list(rpaths)]

        full_rpaths = [os.path.join(self.path, rpath) for rpath in rpaths]

        if lpaths is None:
            if lbase is not None:
                lpaths = [os.path.join(lbase, rpath) for rpath in rpaths]
        else:
            lpaths = make_list(lpaths)
            for i, lpath in enumerate(lpaths):
                if isinstance(lpath, FileSystemTarget):
                    lpaths[i] = lpath.path

        lpaths = self.fs.fetch(full_rpaths, lpaths=lpaths, **kwargs)

        return dict(zip(rpaths, lpaths))

    def put(self, lpaths, rpaths=None, **kwargs):
        lpaths = make_list(lpaths)

        for i, lpath in enumerate(lpaths):
            if isinstance(lpath, FileSystemTarget):
                lpaths[i] = lpath.path

        if rpaths is None:
            full_rpaths = [os.path.join(self.path, os.path.basename(lpath.rstrip("/"))) \
                         for lpath in lpaths]
        else:
            rpaths = make_list(rpaths)
            full_rpaths = []
            for i, rpath in enumerate(rpaths):
                if isinstance(rpath, RemoteTarget):
                    full_rpaths.append(rpath.path)
                else:
                    full_rpaths.append(os.path.join(self.path, rpath.strip("/")))

        self.fs.put(lpaths, full_rpaths, **kwargs)

    @contextmanager
    def localize(self, path=None, is_tmp=True, skip_parent=False, mode=0o0770, cache=True,
                 retry=None):
        tmp = LocalDirectoryTarget(path, is_tmp=is_tmp)
        try:
            yield tmp
            if not skip_parent:
                self.parent.touch()
            # determine files to put
            lpaths, rpaths = [], []
            for base, dirs, files in tmp.walk():
                for f in files:
                    full_f = os.path.join(base, f)
                    lpaths.append(full_f)
                    rpaths.append(os.path.relpath(full_f, tmp.path))
            self.put(lpaths, rpaths, cache=cache, retry=retry)
            self.chmod(mode, retry=retry)
        finally:
            del tmp


RemoteTarget.file_class = RemoteFileTarget
RemoteTarget.directory_class = RemoteDirectoryTarget


class RemoteFileProxy(object):

    def __init__(self, fs, rpath, retry=0):
        super(RemoteFileProxy, self).__init__()

        self.fs    = fs
        self.rpath = rpath
        self.retry = retry

        self._tmp = tempfile.mkstemp()[1]
        self._file = open(self._tmp, "w")

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @property
    def closed(self):
        return self._file.closed

    def close(self):
        if not self.closed:
            self._file.close()
            self.fs.put(self._tmp, self.rpath, retry=self.retry)
            os.remove(self._tmp)

    def seek(self, *args, **kwargs):
        return self._file.seek(*args, **kwargs)

    def write(self, *args, **kwargs):
        return self._file.write(*args, **kwargs)
