# coding: utf-8

"""
Interface for communicating with a remote file service.
"""

from __future__ import annotations

__all__ = ["RemoteFileInterface"]

import os
import sys
import time
import abc
import pathlib
import functools
import random as _random

from law.config import Config
from law.target.file import remove_scheme
from law.util import make_list, is_lazy_iterable, brace_expand, parse_duration
from law.logger import get_logger, Logger
from law._types import Any, TracebackType, Callable, Sequence


logger: Logger = get_logger(__name__)  # type: ignore[assignment]


class RetryException(Exception):

    def __init__(
        self,
        msg: str = "",
        exc: tuple[type, BaseException, TracebackType] | None = None,
    ) -> None:
        _exc = sys.exc_info() if exc is None else exc
        self.exc_type = _exc[0]
        self.exc_value = _exc[1]
        self.exc_traceback = _exc[2]

        super().__init__(msg or str(self.exc_value))


class RemoteFileInterface(object, metaclass=abc.ABCMeta):

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        cfg = Config.instance()

        if config is None:
            config = {}

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option: str, func: Callable[[str, str], Any]) -> None:
            if option not in config or overwrite:
                config[option] = func(section, option)

        def get_expanded_list(section: str, option: str) -> list[str] | None:
            # get config value, run brace expansion taking into account csv splitting
            value = cfg.get_expanded(section, option, None)
            return value and [v.strip() for v in brace_expand(value.strip(), split_csv=True)]

        def get_time(section: str, option: str) -> float:
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
        add("random_base", cfg.get_expanded_bool)

        return config

    @classmethod
    def retry(
        cls,
        func: Callable | None = None,
        uri_base_name: str | Sequence[str] | None = None,
    ) -> Callable:
        def decorator(func: Callable) -> Callable:
            @functools.wraps(func)
            def wrapper(self, *args, **kwargs) -> Any:
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
                    skip_indices: list[int] = []
                    while True:
                        # when no base was set initially and a uri_base_name is given, get a random
                        # uri base under consideration of bases (given by their indices) to skip
                        if not base_set and uri_base_name:
                            base, idx = self.get_base(
                                base_name=uri_base_name,
                                random=random_base,
                                skip_indices=skip_indices,
                                return_index=True,
                            )
                            kwargs["base"] = base
                            skip_indices.append(idx)

                        try:
                            return func(self, *args, **kwargs)

                        except RetryException as e:
                            attempt += 1

                            # raise to the outer try-except block when there are no attempts left
                            if attempt > retries:
                                raise e

                            # log and sleep
                            logger.debug(
                                f"{self.__class__.__name__}.{func_name}(args: {args}, kwargs: "
                                f"{kwargs}) failed: {e}, retry",
                            )
                            time.sleep(delay)
                except:
                    # at this point, no more retry attempts are available,
                    # so update the exception to reflect that, then reraise
                    exc: tuple[type, BaseException, TracebackType] = sys.exc_info()  # type: ignore[assignment] # noqa
                    exc_type, exc_value, exc_traceback = exc
                    msg = (
                        f"{exc_value}\n"
                        f"function: {self.__class__.__name__}.{func_name}\n"
                        f"attempts: {attempt}\n"
                        f"args    : {args}\n"
                        f"kwargs  : {kwargs}\n"
                        f"error   : {exc_type.__name__}: '{exc_value}'"
                    )
                    exc_value.args = (msg,) + exc_value.args[1:]
                    raise exc_value

            return wrapper

        return decorator(func) if func else decorator  # type: ignore[return-value]

    def __init__(
        self,
        base: str | Sequence[str] | None = None,
        *,
        bases: dict[str, str | Sequence[str]] | None = None,
        retries: int = 0,
        retry_delay: int | float = 0,
        random_base: bool = True,
        **kwargs,
    ) -> None:
        super().__init__()

        # convert base(s) to list for random selection
        base = make_list(base or [])
        bases = {k: make_list(b) for k, b in bases.items()} if bases else {}

        # at least one base in expected
        if len(base) == 0:
            raise Exception(
                f"{self.__class__.__name__} expected at least one base path, received none",
            )

        # expand variables in base and bases
        expand = lambda p: os.path.expandvars(str(p))
        self.base = list(map(expand, base))
        self.bases = {k: list(map(expand, b)) for k, b in bases.items()}

        # store other attributes
        self.retries = retries
        self.retry_delay = retry_delay
        self.random_base = random_base

    def sanitize_path(self, p: str | pathlib.Path) -> str:
        return str(p)

    def get_base(
        self,
        base_name: str | Sequence[str] | None = None,
        *,
        random: bool | None = None,
        skip_indices: Sequence[int] | None = None,
        return_index: bool = False,
        return_all: bool = False,
    ):
        if random is None:
            random = self.random_base

        # get potential bases for the given base_name
        bases = make_list(self.base)
        if base_name:
            for _base_name in make_list(base_name):
                if _base_name in self.bases:
                    bases = self.bases[_base_name]
                    break

        if not bases:
            raise Exception(f"no bases available for command '{base_name}'")

        # are there indices to skip?
        all_bases = bases
        if skip_indices is not None:
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

    def uri(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        return_all: bool = False,
        scheme: bool = True,
        **kwargs,
    ) -> str | list[str]:
        # get a base path when not given
        if not base:
            kwargs["return_index"] = False
            base = self.get_base(return_all=return_all, **kwargs)

        # helper to join the path to some base b and remove the scheme if requested
        def uri(b):
            uri = os.path.join(b, self.sanitize_path(path).lstrip("/")).rstrip("/")
            return uri if scheme else remove_scheme(uri)

        if isinstance(base, (list, tuple)) or is_lazy_iterable(base):
            return [uri(b) for b in base]  # type: ignore[union-attr]

        if return_all:
            return [uri(base)]

        return uri(base)

    @abc.abstractmethod
    def exists(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        stat: bool = False,
        **kwargs,
    ) -> bool | os.stat_result | None:
        """
        Returns *True* when the *path* exists and *False* otherwise. When *stat* is *True*, returns
        the stat object or *None*.
        """
        ...

    @abc.abstractmethod
    def stat(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> os.stat_result:
        """
        Returns a stat object or raises an exception when *path* does not exist.
        """
        ...

    @abc.abstractmethod
    def isdir(
        self,
        path: str | pathlib.Path,
        *,
        stat: os.stat_result | None = None,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool:
        """
        Returns *True* when *path* refers to an existing directory, optionally using a precomputed
        stat object instead, and *False* otherwise.
        """
        ...

    @abc.abstractmethod
    def isfile(
        self,
        path: str | pathlib.Path,
        *,
        stat: os.stat_result | None = None,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool:
        """
        Returns *True* when *path* refers to a existing file, optionally using a precomputed stat
        object instead, and *False* otherwise.
        """
        ...

    @abc.abstractmethod
    def chmod(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = False,
        **kwargs,
    ) -> bool:
        """
        Changes the permission of a *path* to *perm*. Raises an exception when *path* does not exist
        or returns *False* when *silent* is *True*, and returns *True* on success.
        """
        ...

    @abc.abstractmethod
    def unlink(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Removes a file at *path*. Raises an exception when *path* does not exist or returns *False*
        when *silent* is *True*, and returns *True* on success.
        """
        ...

    @abc.abstractmethod
    def rmdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Removes a directory at *path*. Raises an exception when *path* does not exist or returns
        *False* when *silent* is *True*, and returns *True* on success.
        """
        ...

    @abc.abstractmethod
    def remove(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Removes any file or directory at *path*. Directories are removed recursively. Raises an
        exception when *path* does not exist or returns *False* when *silent* is *True*, and returns
        *True* on success.
        """
        ...

    @abc.abstractmethod
    def mkdir(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Creates a directory at *path* with permissions *perm*. Raises an exception when *path*
        already exists or returns *False* when *silent* is *True*, and returns *True* on success.
        """
        ...

    @abc.abstractmethod
    def mkdir_rec(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool:
        """
        Recursively creates a directory and intermediate missing directories at *path* with
        permissions *perm*. Raises an exception when *path* already exists or returns *False* when
        *silent* is *True*, and returns *True* on success.
        """
        ...

    @abc.abstractmethod
    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> list[str]:
        """
        Returns a list of elements in and relative to *path*.
        """
        ...

    @abc.abstractmethod
    def filecopy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> tuple[str, str]:
        """
        Copies a file from *src* to *dst*. Returns the full, schemed *src* and *dst* URIs used for
        copying in a 2-tuple.
        """
        ...
