# coding: utf-8

"""
Interface for communicating with a remote file service.
"""

__all__ = ["RemoteFileInterface"]


import os
import sys
import time
import abc
import functools
import random as _random

import six

from law.config import Config
from law.target.file import remove_scheme
from law.util import make_list, is_lazy_iterable, brace_expand, parse_duration
from law.logger import get_logger


logger = get_logger(__name__)


class RetryException(Exception):

    def __init__(self, msg="", orig=()):
        self.orig_type, self.orig_exception, self.orig_traceback = orig or sys.exc_info()
        super(RetryException, self).__init__(msg or str(self.orig_exception))

    def reraise(self):
        return six.reraise(self.orig_type, self.orig_exception, self.orig_traceback)


class RemoteFileInterface(six.with_metaclass(abc.ABCMeta, object)):

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        cfg = Config.instance()

        if config is None:
            config = {}

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

        # default number of retries
        add("retries", cfg.get_expanded_int)

        # default delay between retries
        add("retry_delay", get_time)

        # default setting for the random base selection
        add("random_base", cfg.get_expanded_boolean)

        return config

    @classmethod
    def retry(cls, func=None, uri_cmd=None):
        def decorator(func):
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs):
                # function name for logs
                func_name = func.__name__

                # get retry configs with defaults from the interface itself
                retries = kwargs.pop("retries", self.retries)
                delay = kwargs.pop("retry_delay", self.retry_delay)
                random_base = kwargs.pop("random_base", self.random_base)

                attempt = 0
                try:
                    base = None
                    base_set = bool(kwargs.get("base"))
                    skip_indices = []
                    while True:
                        # when no base was set initially and an uri_cmd is given, get a random
                        # uri base under consideration of bases (given by their indices) to skip
                        if not base_set and uri_cmd:
                            base, idx = self.get_base(cmd=uri_cmd, random=random_base,
                                skip_indices=skip_indices, return_index=True)
                            kwargs["base"] = base
                            skip_indices.append(idx)

                        try:
                            return func(self, *args, **kwargs)
                        except RetryException as e:
                            attempt += 1

                            # raise to the outer try-except block when there are no attempts left
                            if attempt > retries:
                                e.reraise()

                            # log and sleep
                            logger.debug("{}.{}(args: {}, kwargs: {}) failed: {}, retry".format(
                                self.__class__.__name__, func_name, args, kwargs, e))
                            time.sleep(delay)
                except:
                    # at this point, no more retry attempts are available,
                    # so update the exception to reflect that, then reraise
                    e_type, e, traceback = sys.exc_info()
                    msg = str(e)
                    msg += "\nfunction: {}.{}".format(self.__class__.__name__, func_name)
                    msg += "\nattempts: {}".format(attempt)
                    msg += "\nargs    : {}".format(args)
                    msg += "\nkwargs  : {}".format(kwargs)
                    msg += "\nerror   : {}: '{}'".format(e_type.__name__, e)
                    six.reraise(e_type, e_type(msg, *e.args[1:]), traceback)

            return wrapper

        return decorator(func) if func else decorator

    def __init__(self, base=None, bases=None, retries=0, retry_delay=0, random_base=True, **kwargs):
        super(RemoteFileInterface, self).__init__()

        # convert base(s) to list for random selection
        base = make_list(base or [])
        bases = {k: make_list(b) for k, b in six.iteritems(bases)} if bases else {}

        # at least one base in expected
        if len(base) == 0:
            raise Exception("{} expected at least one base path, received none".format(
                self.__class__.__name__))

        # expand variables in base and bases
        self.base = list(map(os.path.expandvars, base))
        self.bases = {k: list(map(os.path.expandvars, b)) for k, b in six.iteritems(bases)}

        # store other attributes
        self.retries = retries
        self.retry_delay = retry_delay
        self.random_base = random_base

    def sanitize_path(self, p):
        return str(p)

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

    def uri(self, path, base=None, return_all=False, scheme=True, **kwargs):
        # get a base path when not given
        if not base:
            kwargs["return_index"] = False
            base = self.get_base(return_all=return_all, **kwargs)

        # helper to join the path to a base b and remove the scheme if requested
        def uri(b):
            uri = os.path.join(b, self.sanitize_path(path).lstrip("/")).rstrip("/")
            if not scheme:
                uri = remove_scheme(uri)
            return uri

        if isinstance(base, (list, tuple)) or is_lazy_iterable(base):
            return [uri(b) for b in base]
        elif return_all:
            return [uri(base)]
        else:
            return uri(base)

    @abc.abstractmethod
    def exists(self, path, base=None, stat=False, **kwargs):
        """
        Returns *True* when the *path* exists and *False* otherwise. When *stat* is *True*, returns
        the stat object or *None*.
        """
        return

    @abc.abstractmethod
    def stat(self, path, base=None, **kwargs):
        """
        Returns a stat object or raises an exception when *path* does not exist.
        """
        return

    @abc.abstractmethod
    def isdir(self, path, stat=None, base=None, **kwargs):
        """
        Returns *True* when *path* refers to an existing directory, optionally using a precomputed
        stat object instead, and *False* otherwise.
        """
        return

    @abc.abstractmethod
    def isfile(self, path, stat=None, base=None, **kwargs):
        """
        Returns *True* when *path* refers to a existing file, optionally using a precomputed stat
        object instead, and *False* otherwise.
        """
        return

    @abc.abstractmethod
    def chmod(self, path, perm, base=None, silent=False, **kwargs):
        """
        Changes the permission of a *path* to *perm*. Raises an exception when *path* does not exist
        or returns *False* when *silent* is *True*, and returns *True* on success.
        """
        return

    @abc.abstractmethod
    def unlink(self, path, base=None, silent=True, **kwargs):
        """
        Removes a file at *path*. Raises an exception when *path* does not exist or returns *False*
        when *silent* is *True*, and returns *True* on success.
        """
        return

    @abc.abstractmethod
    def rmdir(self, path, base=None, silent=True, **kwargs):
        """
        Removes a directory at *path*. Raises an exception when *path* does not exist or returns
        *False* when *silent* is *True*, and returns *True* on success.
        """
        return

    @abc.abstractmethod
    def remove(self, path, base=None, silent=True, **kwargs):
        """
        Removes any file or directory at *path*. Directories are removed recursively. Raises an
        exception when *path* does not exist or returns *False* when *silent* is *True*, and returns
        *True* on success.
        """
        return

    @abc.abstractmethod
    def mkdir(self, path, perm, base=None, silent=True, **kwargs):
        """
        Creates a directory at *path* with permissions *perm*. Raises an exception when *path*
        already exists or returns *False* when *silent* is *True*, and returns *True* on success.
        """
        return

    @abc.abstractmethod
    def mkdir_rec(self, path, perm, base=None, **kwargs):
        """
        Recursively creates a directory and intermediate missing directories at *path* with
        permissions *perm*. Raises an exception when *path* already exists or returns *False* when
        *silent* is *True*, and returns *True* on success.
        """
        return

    @abc.abstractmethod
    def listdir(self, path, base=None, **kwargs):
        """
        Returns a list of elements in and relative to *path*.
        """
        return

    @abc.abstractmethod
    def filecopy(self, src, dst, base=None, **kwargs):
        """
        Copies a file from *src* to *dst*. Returns the full, schemed *src* and *dst* URIs used for
        copying in a 2-tuple.
        """
        return
