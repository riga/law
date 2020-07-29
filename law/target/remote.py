# coding: utf-8

"""
Remote filesystem and targets, based on gfal2 bindings.
"""


__all__ = ["RemoteFileSystem", "RemoteTarget", "RemoteFileTarget", "RemoteDirectoryTarget"]


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
import random as _random
import logging
from contextlib import contextmanager

import six

from law.config import Config
from law.target.file import (
    FileSystem, FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, get_path,
    get_scheme, has_scheme, add_scheme, remove_scheme, split_transfer_kwargs,
)
from law.target.local import LocalFileSystem, LocalFileTarget
from law.target.formatter import find_formatter
from law.util import (
    make_list, brace_expand, makedirs_perm, human_bytes, parse_bytes, parse_duration, create_hash,
    user_owns_file, io_lock, is_lazy_iterable,
)


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

except ImportError:
    HAS_GFAL2 = False

    class gfal2Dummy(object):

        def __getattr__(self, attr):
            raise Exception("trying to access 'gfal2.{}', but gfal2 is not installed".format(attr))

    gfal2 = gfal2Dummy()


_local_fs = LocalFileSystem.default_instance

global_retries = None

global_retry_delay = None


def retry(func=None, uri_cmd=None):
    def wrapper(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # get the number of retries
            if global_retries is not None:
                retries = global_retries
            else:
                retries = kwargs.pop("retries", None)
                if retries is None:
                    retries = self.retries

            # get the retry delay
            if global_retry_delay is not None:
                delay = global_retry_delay
            else:
                delay = kwargs.pop("retry_delay", None)
                if delay is None:
                    delay = self.retry_delay

            # get the random base setting
            random_base = kwargs.pop("random_base", None)
            if random_base is None:
                random_base = self.random_base

            attempt = 0
            try:
                base = None
                base_set = bool(kwargs.get("base"))
                skip_indices = []
                while True:
                    # when no base was set initially and an uri cmd is given, get a random
                    # uri base under consideration of bases (or their indices) to skip
                    if not base_set and uri_cmd:
                        base, idx = self.get_base(cmd=uri_cmd, random=random_base,
                            skip_indices=skip_indices, return_index=True)
                        kwargs["base"] = base
                        skip_indices.append(idx)

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

    return wrapper(func) if func else wrapper


class GFALInterface(object):

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
            atomic_contexts=False, retries=0, retry_delay=0, random_base=True):
        object.__init__(self)

        # cache for gfal context objects and transfer parameters per pid for thread safety
        self._contexts = {}
        self._transfer_parameters = {}

        # convert base(s) to list for random selection
        self.base = make_list(base)
        self.bases = {k: make_list(b) for k, b in six.iteritems(bases)} if bases else {}

        # expand variables in base and bases
        self.base = list(map(os.path.expandvars, self.base))
        self.bases = {k: list(map(os.path.expandvars, b)) for k, b in six.iteritems(self.bases)}

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
        self.random_base = random_base

    @classmethod
    def gfal_str(cls, s):
        # in python 2, the gfal2-bindings do not support unicode but expect strings
        return str(s) if isinstance(s, six.string_types) else s

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

    def uri(self, path, *args, **kwargs):
        """ uri(path, base=None, *args, **kwargs)
        """
        # get the base
        base = kwargs.pop("base", None)
        if not base:
            kwargs["return_index"] = False
            base = self.get_base(*args, **kwargs)

        # helper to join the path to a base b
        uri = lambda b: os.path.join(b, self.gfal_str(path).lstrip("/")).rstrip("/")

        if isinstance(base, (list, tuple)) or is_lazy_iterable(base):
            return [uri(b) for b in base]
        else:
            return uri(base)

    def get_base(self, cmd=None, random=None, skip_indices=None, return_index=False,
            return_all=False):
        if random is None:
            random = self.random_base

        # get potential bases for the given cmd
        bases = make_list(self.base)
        if cmd:
            for cmd in make_list(cmd):
                if cmd in self.bases:
                    bases = self.bases[cmd]
                    break

        if not bases:
            raise Exception("no bases available for command '{}'".format(cmd))

        # are there indices to skip?
        all_bases = bases
        if skip_indices:
            _bases = [b for i, b in enumerate(bases) if i not in skip_indices]
            if _bases:
                bases = _bases

        # return all?
        if return_all:
            return bases

        # select one
        if len(bases) == 1 or not random:
            # select the first base
            base = bases[0]
        else:
            # select a random base
            base = _random.choice(bases)

        return base if not return_index else (base, all_bases.index(base))

    def exists(self, path, stat=False, base=None):
        uri = self.uri(path, cmd="stat" if stat else ("exists", "stat"), base=base)
        with self.context() as ctx:
            try:
                rstat = ctx.stat(uri)
                return rstat if stat else True
            except gfal2.GError:
                return None if stat else False

    @retry(uri_cmd="stat")
    def stat(self, path, base=None):
        uri = self.uri(path, cmd="stat", base=base)
        with self.context() as ctx:
            return ctx.stat(uri)

    @retry(uri_cmd="chmod")
    def chmod(self, path, perm, base=None):
        if perm:
            uri = self.uri(path, cmd="chmod", base=base)
            with self.context() as ctx:
                return ctx.chmod(uri, perm)

    @retry(uri_cmd="unlink")
    def unlink(self, path, base=None):
        uri = self.uri(path, cmd="unlink", base=base)
        with self.context() as ctx:
            return ctx.unlink(uri)

    @retry(uri_cmd="rmdir")
    def rmdir(self, path, base=None):
        uri = self.uri(path, cmd="rmdir", base=base)
        with self.context() as ctx:
            return ctx.rmdir(uri)

    @retry(uri_cmd="mkdir")
    def mkdir(self, path, perm, base=None):
        uri = self.uri(path, cmd="mkdir", base=base)
        with self.context() as ctx:
            return ctx.mkdir(uri, perm or 0o0770)

    @retry(uri_cmd=["mkdir_rec", "mkdir"])
    def mkdir_rec(self, path, perm, base=None):
        uri = self.uri(path, cmd=["mkdir_rec", "mkdir"], base=base)
        with self.context() as ctx:
            return ctx.mkdir_rec(uri, perm or 0o0770)

    @retry(uri_cmd="listdir")
    def listdir(self, path, base=None):
        uri = self.uri(path, cmd="listdir", base=base)
        with self.context() as ctx:
            return ctx.listdir(uri)

    @retry(uri_cmd="filecopy")
    def filecopy(self, src, dst, base=None):
        if has_scheme(src):
            src = self.gfal_str(src)
        else:
            src = self.uri(src, cmd="filecopy", base=base)

        if has_scheme(dst):
            dst = self.gfal_str(dst)
        else:
            dst = self.uri(dst, cmd="filecopy", base=base)

        with self.context() as ctx, self.transfer_parameters(ctx) as params:
            ctx.filecopy(params, src, dst)

        return src, dst


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
                inst._cleanup()
            except:
                pass

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        # reads a law config section and returns parsed file system configs
        cfg = Config.instance()

        if config is None:
            config = {}

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
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
        add("cleaup", cfg.get_expanded_boolean)
        add("max_size", get_size)
        add("file_perm", cfg.get_expanded_int)
        add("dir_perm", cfg.get_expanded_int)
        add("wait_delay", get_time)
        add("max_waits", cfg.get_expanded_int)
        add("global_lock", cfg.get_expanded_boolean)

        return config

    def __init__(self, fs, root=TMP, cleanup=False, max_size=0, file_perm=0o0660, dir_perm=0o0770,
            wait_delay=5., max_waits=120, global_lock=False):
        object.__init__(self)
        # max_size is in MB, wait_delay is in seconds

        # create a unique name based on fs attributes
        name = "{}_{}".format(fs.__class__.__name__, create_hash(fs.gfal.base[0]))

        # create the root dir, handle tmp
        root = os.path.expandvars(os.path.expanduser(root)) or self.TMP
        if not os.path.exists(root) and root == self.TMP:
            cfg = Config.instance()
            tmp_dir = cfg.get_expanded("target", "tmp_dir")
            base = tempfile.mkdtemp(dir=tmp_dir)
            cleanup = True
        else:
            base = os.path.join(root, name)
            makedirs_perm(base, dir_perm)

        # save attributes and configs
        self.root = root
        self.fs_ref = weakref.ref(fs)
        self.base = base
        self.name = name
        self.cleanup = cleanup
        self.max_size = max_size
        self.dir_perm = dir_perm
        self.file_perm = file_perm
        self.wait_delay = wait_delay
        self.max_waits = max_waits
        self.global_lock = global_lock

        # path to the global lock file which should guard global actions such as allocations
        self._global_lock_path = self._lock_path(os.path.join(base, "global"))

        # currently locked cache paths, only used to clean up broken files during cleanup
        self._locked_cpaths = set()

        logger.debug("created RemoteCache at '{}'".format(self.base))

    def __del__(self):
        self._cleanup()

    def __repr__(self):
        return "<{} '{}' at {}>".format(self.__class__.__name__, self.base, hex(id(self)))

    def __contains__(self, rpath):
        return os.path.exists(self.cache_path(rpath))

    @property
    def fs(self):
        return self.fs_ref()

    def _cleanup(self):
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

    def _await(self, cpath, delay=None, max_waits=None, silent=False, global_lock=None):
        delay = delay if delay is not None else self.wait_delay
        max_waits = max_waits if max_waits is not None else self.max_waits
        _max_waits = max_waits
        global_lock = self.global_lock if global_lock is None else global_lock

        # strategy: wait as long the file is locked and if the file size did not change, reduce
        # max_waits per iteration and raise when 0 is reached
        last_size = -1
        while self._is_locked(cpath) or (global_lock and self.is_locked_global()):
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
                    max_waits = _max_waits + 1

            max_waits -= 1

        return True

    @contextmanager
    def _lock_global(self, **kwargs):
        self._await_global(**kwargs)

        try:
            with io_lock:
                with open(self._global_lock_path, "w") as f:
                    f.write("")
                os.utime(self._global_lock_path, None)

            yield
        finally:
            self._unlock_global()

    @contextmanager
    def _lock(self, cpath, **kwargs):
        lock_path = self._lock_path(cpath)

        self._await(cpath, **kwargs)

        try:
            with io_lock:
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
            logger.debug("cache space sufficient, {0[0]:.2f} {0[1]} remaining".format(
                human_bytes(-delete_size)))
            return True

        logger.info("need to delete {0[0]:.2f} {0[1]} from cache".format(
            human_bytes(delete_size)))

        # delete files, ordered by their access time, skip locked ones
        for cpath, cstat in sorted(file_stats, key=lambda tpl: tpl[1].st_atime):
            if self._is_locked(cpath):
                continue
            self._remove(cpath)
            delete_size -= cstat.st_size
            if delete_size <= 0:
                return True

        logger.warning("could not allocate remaining {0[0]:.2f} {0[1]} in cache".format(
            human_bytes(delete_size)))

        return False

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

    local_fs = _local_fs

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        config = super(RemoteFileSystem, cls).parse_config(section, config=config)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
            if option not in config or overwrite:
                config[option] = func(section, option)

        def get_expanded_list(section, option):
            # get config value, run brace expansion taking into account csv splitting
            value = cfg.get_expanded(section, option)
            return value and [v.strip() for v in brace_expand(value.strip(), split_csv=True)]

        def get_time(section, option):
            value = cfg.get_expanded(section, option)
            return parse_duration(value, input_unit="s", unit="s")

        # base path(s)
        add("base", get_expanded_list)

        # base path(s) per operation
        options = cfg.options(section, prefix="base_")
        add("bases", lambda *_: {
            option[5:]: get_expanded_list(section, option)
            for option in options
            if not cfg.is_missing_or_none(section, option)
        })

        # use atomic contexts per operation
        add("atomic_contexts", cfg.get_expanded_boolean)

        # default number of retries
        add("retries", cfg.get_expanded_int)

        # default delay between retries
        add("retry_delay", get_time)

        # default setting for the random base selection
        add("random_base", cfg.get_expanded_boolean)

        # default setting for validation after copy
        add("validate_copy", cfg.get_expanded_boolean)

        # default setting for recursive directory removal
        add("recursive_remove", cfg.get_expanded_boolean)

        # default setting for using the cache
        add("use_cache", cfg.get_expanded_boolean)

        # cache options
        if cfg.options(section, prefix="cache_"):
            RemoteCache.parse_config(section, config.setdefault("cache_config", {}),
                overwrite=overwrite)

        return config

    def __init__(self, base, bases=None, gfal_options=None, transfer_config=None,
            atomic_contexts=False, retries=0, retry_delay=0, random_base=True, validate_copy=False,
            recursive_remove=False, use_cache=False, cache_config=None, local_fs=None, **kwargs):
        FileSystem.__init__(self, **kwargs)

        # configure the gfal interface
        self.gfal = GFALInterface(base, bases, gfal_options=gfal_options,
            transfer_config=transfer_config, atomic_contexts=atomic_contexts, retries=retries,
            retry_delay=retry_delay, random_base=random_base)

        # store other configs
        self.validate_copy = validate_copy
        self.recursive_remove = recursive_remove
        self.use_cache = use_cache

        # set the cache when a cache root is set
        if cache_config and cache_config.get("root"):
            self.cache = RemoteCache(self, **cache_config)
        else:
            self.cache = None

        if local_fs:
            self.local_fs = local_fs

    def __del__(self):
        # cleanup the cache
        if getattr(self, "cache", None):
            self.cache._cleanup()

    def __repr__(self):
        return "{}(base={}, {})".format(self.__class__.__name__, self.gfal.base[0], hex(id(self)))

    def __eq__(self, other):
        return self is other

    def _eval_use_cache(self, use_cache):
        if self.cache is None:
            return False
        elif use_cache is None:
            return self.use_cache
        else:
            return bool(use_cache)

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

    def stat(self, path, **kwargs):
        return self.gfal.stat(self.abspath(path), **kwargs)

    def exists(self, path, stat=False):
        return self.gfal.exists(self.abspath(path), stat=stat)

    def _s_isdir(self, st_mode):
        return stat.S_ISDIR(st_mode)

    def isdir(self, path, rstat=None, **kwargs):
        if rstat is None:
            rstat = self.exists(path, stat=True)
        return self._s_isdir(rstat.st_mode) if rstat else False

    def isfile(self, path, rstat=None, **kwargs):
        if rstat is None:
            rstat = self.exists(path, stat=True)
        return not self._s_isdir(rstat.st_mode) if rstat else False

    def chmod(self, path, perm, silent=True, **kwargs):
        if self.has_perms and perm is not None:
            try:
                self.gfal.chmod(self.abspath(path), perm, **kwargs)
            except gfal2.GError:
                if not silent:
                    raise

    def remove(self, path, recursive=None, silent=True, **kwargs):
        if recursive is None:
            recursive = self.recursive_remove

        if self.abspath(path) == "/":
            logger.warning("refused request to remove base directory of {!r}".format(self))
            return

        # first get the remote stat object
        rstat = self.exists(path, stat=True) if silent else self.stat(path, retries=0)

        if self.isfile(path, rstat=rstat):
            # remove the file
            self.gfal.unlink(path, **kwargs)
        elif self.isdir(path, rstat=rstat):
            # when recursive, remove content first
            if recursive:
                for elem in self.listdir(path, **kwargs):
                    self.remove(os.path.join(path, elem), recursive=True, silent=silent, **kwargs)
            # remove the directory itself
            self.gfal.rmdir(path, **kwargs)

    def mkdir(self, path, perm=None, recursive=True, silent=True, **kwargs):
        func = self.gfal.mkdir_rec if recursive else self.gfal.mkdir

        if perm is None:
            perm = self.default_dir_perm

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
    def _atomic_copy(self, src, dst, perm=None, validate=None, **kwargs):
        if validate is None:
            validate = self.validate_copy

        src = self.abspath(src)
        dst = self.abspath(dst)

        # actual copy
        src, dst = self.gfal.filecopy(src, dst, **kwargs)

        # copy validation
        dst_fs = self.local_fs if self.is_local(dst) else self
        if validate:
            if not dst_fs.exists(dst):
                raise Exception("validation failed after copying {} to {}".format(src, dst))

        # handle permissions
        dst_fs.chmod(dst, perm if perm is not None else dst_fs.default_file_perm)

        return dst

    # generic copy with caching ability (local paths must have a "file://" scheme)
    def _cached_copy(self, src, dst, perm=None, cache=None, prefer_cache=False, validate=None,
            **kwargs):
        cache = self._eval_use_cache(cache)

        # ensure absolute paths
        src = self.abspath(src)
        dst = dst and self.abspath(dst) or None

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

        # append the basename of src when dst is a directory
        if dst:
            dst_fs = self.local_fs if self.is_local(dst) else self
            if dst_fs.isdir(dst):
                dst = os.path.join(dst, os.path.basename(src))

        if cache:
            kwargs_no_retries = kwargs.copy()
            kwargs_no_retries["retries"] = 0
            kwargs_no_retries = kwargs

            # handle 3 cases: lr, rl, rc
            if mode == "lr":
                # strategy: copy to remote, copy to cache, sync stats

                # copy to remote, no need to validate as we compute the stat anyway
                dst_uri = self._atomic_copy(src, dst, perm=perm, validate=False, **kwargs)
                rstat = self.stat(dst, **kwargs_no_retries)

                # remove the cache entry
                if dst in self.cache:
                    self.cache.remove(dst)

                # allocate cache space and copy to cache
                lstat = self.local_fs.stat(src)
                self.cache.allocate(lstat.st_size)
                cdst_uri = add_scheme(self.cache.cache_path(dst), "file")
                with self.cache.lock(dst):
                    self._atomic_copy(src, cdst_uri, validate=False)
                    self.cache.touch(dst, (int(time.time()), rstat.st_mtime))

                return dst_uri

            else:  # rl, rc
                # strategy: copy to cache when not up to date, sync stats, opt. copy to local

                # build the uri to the cache path of the src file
                csrc_uri = add_scheme(self.cache.cache_path(src), "file")

                # if the file is cached and prefer_cache is true,
                # return the cache path, no questions asked
                # otherwise, check if the file is there and up to date
                if not prefer_cache or src not in self.cache:
                    with self.cache.lock(src):
                        # in cache and outdated?
                        rstat = self.stat(src, **kwargs_no_retries)
                        if src in self.cache and abs(self.cache.mtime(src) - rstat.st_mtime) > 1:
                            self.cache.remove(src, lock=False)
                        # in cache at all?
                        if src not in self.cache:
                            self.cache.allocate(rstat.st_size)
                            self._atomic_copy(src, csrc_uri, validate=validate, **kwargs)
                            self.cache.touch(src, (int(time.time()), rstat.st_mtime))

                if mode == "rl":
                    # simply use the local_fs for copying
                    self.local_fs.copy(csrc_uri, dst, perm=perm)
                    return dst
                else:  # rc
                    return csrc_uri

        else:
            # simply copy and return the dst path
            return self._atomic_copy(src, dst, perm=perm, validate=validate, **kwargs)

    def _prepare_dst_dir(self, dst, src=None, perm=None, **kwargs):
        """
        Prepares the directory of a target located at *dst* and returns its full location as
        specified below. *src* can be the location of a source file target, which is (e.g.) used by
        a file copy or move operation. When *dst* is already a directory, calling this method has no
        effect and the *dst* path is returned, optionally joined with the basename of *src*. When
        *dst* is a file, *dst* path is returned unchanged. Otherwise, when *dst* does not exist yet,
        it is interpreted as a file path and missing directories are created when
        :py:attr:`create_file_dir` is *True*, using *perm* to set the directory permission. *dst* is
        returned.
        """
        rstat = self.exists(dst, stat=True)

        if rstat:
            if self._s_isdir(rstat.st_mode) and src:
                full_dst = os.path.join(dst, os.path.basename(src))
            else:
                full_dst = dst

        else:
            # interpret dst as a file name, create missing dirs
            dst_dir = self.dirname(dst)
            if dst_dir and self.create_file_dir and not self.isdir(dst_dir):
                self.mkdir(dst_dir, perm=perm, recursive=True, **kwargs)
            full_dst = dst

        return full_dst

    def copy(self, src, dst, perm=None, dir_perm=None, **kwargs):
        # dst might be an existing directory
        if dst:
            if self.is_local(dst):
                self.local_fs._prepare_dst_dir(dst, src=src, perm=dir_perm)
            else:
                self._prepare_dst_dir(dst, src=src, perm=dir_perm, **kwargs)

        # copy the file
        return self._cached_copy(src, dst, perm=perm, **kwargs)

    def move(self, src, dst, perm=None, dir_perm=None, **kwargs):
        if not dst:
            raise Exception("move requires dst to be set")

        # copy the file
        kwargs.pop("cache", None)
        dst = self.copy(src, dst, perm=perm, dir_perm=dir_perm, cache=False, **kwargs)

        # remove the src
        if self.is_local(src):
            self.local_fs.remove(src)
        else:
            self.remove(src, **kwargs)

        return dst

    def open(self, path, mode, cache=None, **kwargs):
        cache = self._eval_use_cache(cache)

        yield_path = kwargs.pop("_yield_path", False)

        path = self.abspath(path)
        tmp = None

        if mode == "r":
            if cache:
                lpath = self._cached_copy(path, None, cache=True, **kwargs)
                lpath = remove_scheme(lpath)
            else:
                tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
                self.copy(path, tmp.uri(), cache=False, **kwargs)

            def cleanup():
                if not cache and tmp.exists():
                    tmp.remove()

            f = lpath if yield_path else open(lpath, "r")
            return RemoteFileProxy(f, success_fn=cleanup, failure_fn=cleanup)

        elif mode == "w":
            tmp = LocalFileTarget(is_tmp=self.ext(path, n=0) or True)
            lpath = tmp.path

            def cleanup():
                if tmp.exists():
                    tmp.remove()

            def copy_and_cleanup():
                try:
                    if tmp.exists():
                        self.copy(tmp.uri(), path, cache=cache, **kwargs)
                finally:
                    cleanup()

            f = lpath if yield_path else open(lpath, "w")
            return RemoteFileProxy(f, success_fn=copy_and_cleanup, failure_fn=cleanup)

        else:
            raise Exception("unknown mode {}, use r or w".format(mode))

    def load(self, path, formatter, *args, **kwargs):
        fmt = find_formatter(path, "load", formatter)
        transfer_kwargs, kwargs = split_transfer_kwargs(kwargs)
        with self.open(path, "r", _yield_path=True, **transfer_kwargs) as lpath:
            return fmt.load(lpath, *args, **kwargs)

    def dump(self, path, formatter, *args, **kwargs):
        fmt = find_formatter(path, "dump", formatter)
        transfer_kwargs, kwargs = split_transfer_kwargs(kwargs)
        with self.open(path, "w", _yield_path=True, **transfer_kwargs) as lpath:
            return fmt.dump(lpath, *args, **kwargs)


class RemoteTarget(FileSystemTarget):

    fs = None

    def __init__(self, path, fs, **kwargs):
        if not isinstance(fs, RemoteFileSystem):
            raise TypeError("fs must be a {} instance, is {}".format(RemoteFileSystem, fs))

        self.fs = fs
        self._path = None

        FileSystemTarget.__init__(self, path, **kwargs)

    def _parent_args(self):
        args, kwargs = FileSystemTarget._parent_args(self)
        args += (self.fs,)
        return args, kwargs

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        if os.path.normpath(path).startswith(".."):
            raise ValueError("path {} forbidden, surpasses file system root".format(path))

        self._path = self.fs.abspath(path)

    def uri(self, *args, **kwargs):
        return self.fs.gfal.uri(self.path, *args, **kwargs)

    def url(self, *args, **kwargs):
        # deprecation warning until v0.1
        logger.warning("the use of {0}.url() is deprecated, please use {0}.uri() instead".format(
            self.__class__.__name__))

        return self.uri(*args, **kwargs)


class RemoteFileTarget(RemoteTarget, FileSystemFileTarget):

    def copy_to_local(self, dst=None, perm=None, dir_perm=None, **kwargs):
        if dst:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")
        dst = FileSystemFileTarget.copy_to(self, dst, perm=perm, dir_perm=dir_perm, **kwargs)
        return remove_scheme(dst)

    def copy_from_local(self, src=None, perm=None, dir_perm=None, **kwargs):
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.copy_from(self, src, perm=perm, dir_perm=dir_perm, **kwargs)

    def move_to_local(self, dst=None, perm=None, dir_perm=None, **kwargs):
        if dst:
            dst = add_scheme(self.fs.local_fs.abspath(get_path(dst)), "file")
        dst = FileSystemFileTarget.move_to(self, dst, perm=perm, dir_perm=dir_perm, **kwargs)
        return remove_scheme(dst)

    def move_from_local(self, src=None, perm=None, dir_perm=None, **kwargs):
        src = add_scheme(self.fs.local_fs.abspath(get_path(src)), "file")
        return FileSystemFileTarget.move_from(self, src, perm=perm, dir_perm=dir_perm, **kwargs)

    @contextmanager
    def localize(self, mode="r", perm=None, dir_perm=None, tmp_dir=None, **kwargs):
        if mode not in ("r", "w", "a"):
            raise Exception("unknown mode '{}', use 'r', 'w' or 'a'".format(mode))

        logger.debug("localizing file target {!r} with mode '{}'".format(self, mode))

        if mode == "r":
            with self.fs.open(self.path, "r", _yield_path=True, perm=perm, **kwargs) as lpath:
                yield LocalFileTarget(lpath)

        else:  # mode "w" or "a"
            tmp = LocalFileTarget(is_tmp=self.ext(n=1) or True, tmp_dir=tmp_dir)

            # copy to local in append mode
            if mode == "a" and self.exists():
                self.copy_to_local(tmp)

            try:
                yield tmp

                if tmp.exists():
                    self.copy_from_local(tmp, perm=perm, dir_perm=dir_perm, **kwargs)
                else:
                    logger.warning("cannot move non-existing localized file target {!r}".format(
                        self))
            finally:
                tmp.remove()


class RemoteDirectoryTarget(RemoteTarget, FileSystemDirectoryTarget):

    def _child_args(self):
        args, kwargs = FileSystemDirectoryTarget._child_args(self)
        args += (self.fs,)
        return args, kwargs


RemoteTarget.file_class = RemoteFileTarget
RemoteTarget.directory_class = RemoteDirectoryTarget


class RemoteFileProxy(object):

    def __init__(self, f, close_fn=None, success_fn=None, failure_fn=None):
        super(RemoteFileProxy, self).__init__()

        self.f = f
        self.is_file = not isinstance(f, six.string_types)

        self.close_fn = close_fn
        self.success_fn = success_fn
        self.failure_fn = failure_fn

    def __call__(self):
        return self.f

    def __getattr__(self, attr):
        return getattr(self.f, attr)

    def __enter__(self):
        return self.f.__enter__() if self.is_file else self.f

    def __exit__(self, exc_type, exc_value, traceback):
        if getattr(self.f, "__exit__", None) is not None:
            success = self.f.__exit__(exc_type, exc_value, traceback)
        else:
            success = exc_type is None

        if success:
            if callable(self.success_fn):
                self.success_fn()
        else:
            if callable(self.failure_fn):
                self.failure_fn()

        return success

    def close(self, *args, **kwargs):
        ret = self.f.close(*args, **kwargs)

        if callable(self.close_fn):
            self.close_fn()

        return ret
