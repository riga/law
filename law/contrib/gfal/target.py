# coding: utf-8

"""
Implementation of a file interface using GFAL.
"""

from __future__ import annotations

__all__ = ["GFALFileInterface"]

import os
import sys
import gc
import pathlib
import contextlib
import stat as _stat

from law.config import Config
from law.target.file import has_scheme, get_scheme
from law.target.remote.interface import RemoteFileInterface, RetryException
from law.logger import get_logger
from law._types import Any, Callable, Iterator, Sequence, TracebackType


logger = get_logger(__name__)


# try to import gfal2
try:
    import gfal2  # type: ignore[import-untyped, import-not-found]
    HAS_GFAL2 = True

except (ImportError, TypeError):
    HAS_GFAL2 = False

    class GFAL2Dummy(object):

        def __getattr__(self, attr):
            raise Exception(f"trying to access 'gfal2.{attr}', but gfal2 is not installed")

    gfal2 = GFAL2Dummy()


class GFALFileInterface(RemoteFileInterface):

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        _config = super().parse_config(section, config=config, overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(
            option: str,
            func: Callable[[str, str], Any],
            postfix: str = "gfal_",
            _config: dict[str, Any] = _config,
        ) -> None:
            if option not in _config or overwrite:
                _config[option] = func(section, postfix + option)

        # use atomic contexts per operation
        add("atomic_contexts", cfg.get_expanded_bool)

        # transfer config
        _config.setdefault("transfer_config", {})
        transfer_specs = [
            ("timeout", cfg.get_expanded_int),
            ("checksum_check", cfg.get_expanded_bool),
            ("nbstreams", cfg.get_expanded_int),
            ("overwrite", cfg.get_expanded_bool),
            ("create_parent", cfg.get_expanded_bool),
            ("strict_copy", cfg.get_expanded_bool),
        ]
        for name, func in transfer_specs:
            add(name, func, "gfal_transfer_", _config["transfer_config"])

        return _config

    def __init__(
        self,
        *,
        atomic_contexts: bool = False,
        gfal_options: dict[str, Any] | None = None,
        transfer_config: dict[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super(GFALFileInterface, self).__init__(**kwargs)

        # cache for gfal context objects and transfer parameters per pid for thread safety
        self._contexts: dict[int, gfal2.Gfal2Context] = {}
        self._transfer_parameters: dict[int, gfal2.TransferParameters] = {}

        # store gfal options and transfer configs
        self.gfal_options = gfal_options or {}
        self.transfer_config = transfer_config or {}

        # other configs
        self.atomic_contexts = atomic_contexts

    @contextlib.contextmanager
    def context(self) -> Iterator[gfal2.Gfal2Context]:
        # context objects are stored per pid, so create one if it does not exist yet
        pid = os.getpid()

        if pid not in self._contexts:
            self._contexts[pid] = ctx = gfal2.creat_context()
            for _type, args_list in self.gfal_options.items():
                for args in args_list:
                    getattr(ctx, f"set_opt_{_type}")(*args)

        # yield and optionally close it which frees potentially open connections
        try:
            yield self._contexts[pid]
        finally:
            if self.atomic_contexts and pid in self._contexts:
                del self._contexts[pid]
            gc.collect()

    @contextlib.contextmanager
    def transfer_parameters(self, ctx: gfal2.Gfal2Context) -> Iterator[gfal2.TransferParameters]:
        pid = os.getpid()

        if pid not in self._transfer_parameters:
            self._transfer_parameters[pid] = ctx.transfer_parameters()
            for key, value in self.transfer_config.items():
                setattr(self._transfer_parameters[pid], key, value)

        try:
            yield self._transfer_parameters[pid]
        finally:
            if self.atomic_contexts and pid in self._transfer_parameters:
                del self._transfer_parameters[pid]

    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool | os.stat_result | None:
        uri = self.uri(path, base_name="stat" if stat else ("exists", "stat"), base=base)
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 exists({uri})")
                rstat = ctx.stat(uri)
                return rstat if stat else True
            except gfal2.GError:
                return None if stat else False

    @RemoteFileInterface.retry(uri_base_name="stat")
    def stat(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> os.stat_result:
        uri = self.uri(path, base_name="stat", base=base)
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 stat({uri})")
                return ctx.stat(uri)

            except gfal2.GError:
                raise RetryException()

    def isdir(
        self,
        path: str | pathlib.Path,
        *,
        stat: os.stat_result | None = None,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool:
        if not stat:
            stat = self.exists(path, stat=True, base=base)  # type: ignore[assignment]

        if not stat:
            return False

        # some file protocols do not return standard st_mode values in stat requests,
        # e.g. SRM returns file type bits 0o50000 for directories instead of 0o40000,
        # these differences are rather distinct and can be taken into account here,
        # see http://man7.org/linux/man-pages/man7/inode.7.html for info on st_mode values
        return _stat.S_ISDIR(stat.st_mode) or _stat.S_IFMT(stat.st_mode) == 0o50000

    def isfile(
        self,
        path: str | pathlib.Path,
        *,
        stat: os.stat_result | None = None,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool:
        if not stat:
            stat = self.exists(path, stat=True, base=base)  # type: ignore[assignment]

        if not stat:
            return False

        return not self.isdir(path, stat=stat, base=base)

    @RemoteFileInterface.retry(uri_base_name="chmod")
    def chmod(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = False,
        **kwargs,
    ) -> bool:
        if perm is None:
            return True

        uri: str = self.uri(path, base_name="chmod", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 chmod({uri}, {perm})")
                ctx.chmod(uri, perm)
                return True

            except gfal2.GError:
                e = GFALError_chmod(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason in (e.NOT_FOUND, e.NOT_SUPPORTED) and silent:
                    return False
                raise e

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def unlink(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri: str = self.uri(path, base_name="unlink", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 unlink({uri})")
                ctx.unlink(uri)
                return True

            except gfal2.GError:
                e = GFALError_unlink(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason == e.NOT_FOUND and silent:
                    return False
                raise e

    @RemoteFileInterface.retry(uri_base_name="rmdir")
    def rmdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri: str = self.uri(path, base_name="rmdir", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 rmdir({uri})")
                ctx.rmdir(uri)
                return True

            except gfal2.GError:
                e = GFALError_rmdir(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason == e.NOT_FOUND and silent:
                    return False
                raise e

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def remove(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        """
        Recursive removal is potentially expensive in terms of remote file operations, so this
        method is designed to reduce them as much as possible.
        """
        # most common use case is file removal, so try this first and in case there is an error
        # interpret its message to get more info on the object without further operations
        uri: str = self.uri(path, base_name="unlink", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 unlink({uri})")
                ctx.unlink(uri)
                return True

            except gfal2.GError:
                e = GFALError_unlink(uri)
                # handle all cases, except when uri is a directory
                if e.reason != e.IS_DIRECTORY:
                    if e.reason == e.UNKNOWN:
                        raise e
                    if e.reason == e.NOT_FOUND and silent:
                        return False
                    raise e

        # at this point, we are dealing with a directory so try to delete it right away
        uri = self.uri(path, base_name="rmdir", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 rmdir({uri})")
                ctx.rmdir(uri)
                return True

            except gfal2.GError:
                e2 = GFALError_rmdir(uri)
                # handle all cases, except when the directory is not empty
                if e2.reason != e2.NOT_EMPTY:
                    if e2.reason == e2.UNKNOWN:
                        raise e
                    if e2.reason == e2.NOT_FOUND and silent:
                        return False
                    raise e2

        # the directory is not empty, so there is no other way than deleting contents recursively
        # first, and then removing the directory itself
        path = str(path)
        for elem in self.listdir(path, base=base, retries=0):
            self.remove(os.path.join(path, elem), base=base, silent=silent, retries=0)

        return self.rmdir(path, base=base, silent=silent, retries=0)

    @RemoteFileInterface.retry(uri_base_name="mkdir")
    def mkdir(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri: str = self.uri(path, base_name="mkdir", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 mkdir({uri}, {perm})")
                ctx.mkdir(uri, perm)
                return True

            except gfal2.GError:
                e = GFALError_mkdir(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason == e.EXISTS and silent:
                    # fail silently only when uri is really a dictionary
                    if self.isdir(path, base=base):
                        return False
                raise e

    @RemoteFileInterface.retry(uri_base_name=["mkdir_rec", "mkdir"])
    def mkdir_rec(
        self,
        path: str | pathlib.Path,
        perm: int,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri: str = self.uri(path, base_name="mkdir", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 mkdir_rec({uri}, {perm})")
                ctx.mkdir_rec(uri, perm)
                return True

            except gfal2.GError:
                e = GFALError_mkdir(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason == e.EXISTS and silent:
                    # fail silently only when uri is really a dictionary
                    if self.isdir(path, base=base):
                        return False
                raise e

    @RemoteFileInterface.retry(uri_base_name="listdir")
    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> list[str]:
        uri: str = self.uri(path, base_name="listdir", base=base, return_all=False)  # type: ignore[assignment] # noqa
        with self.context() as ctx:
            try:
                logger.debug(f"invoking gfal2 listdir({uri})")
                return ctx.listdir(uri)

            except gfal2.GError:
                e = GFALError_listdir(uri)
                # some protocols throw an error upon listdir on empty directories
                if e.EMPTY:
                    return []
                raise e

    @RemoteFileInterface.retry(uri_base_name="filecopy")
    def filecopy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> tuple[str, str]:
        if has_scheme(src):
            src_uri = self.sanitize_path(src)
        else:
            src_uri = self.uri(src, base_name="filecopy", base=base, return_all=False)  # type: ignore[assignment] # noqa

        if has_scheme(dst):
            dst_uri = self.sanitize_path(dst)
        else:
            dst_uri = self.uri(dst, base_name="filecopy", base=base, return_all=False)  # type: ignore[assignment] # noqa

        with self.context() as ctx, self.transfer_parameters(ctx) as params:
            try:
                logger.debug(f"invoking gfal2 filecopy({src_uri}, {dst_uri})")
                ctx.filecopy(params, src_uri, dst_uri)

            except gfal2.GError:
                e = GFALError_filecopy(src_uri, dst_uri)
                # check if the operation should be retried or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                raise e

        return src_uri, dst_uri


class GFALOperationError(RetryException):

    UNKNOWN = "unknown reason"

    def __init__(
        self,
        uri: str | pathlib.Path,
        exc: tuple[type, BaseException, TracebackType] | None = None,
    ) -> None:
        # store uri and scheme
        self.uri = str(uri)
        self.scheme = get_scheme(uri) or "file"

        # get the original error objects and find the error reason
        _exc = sys.exc_info() if exc is None else exc
        self.reason = self._get_reason(str(_exc[1]), self.uri, self.scheme)

        # add the error reason to the message
        msg = f"{_exc[1]} ({self.__class__.__name__}: {self.reason})"

        super().__init__(msg=msg, exc=exc)

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        raise NotImplementedError()


class GFALError_chmod(GFALOperationError):

    NOT_FOUND = "no such file or directory"
    NOT_SUPPORTED = "chmod operation not supported"

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        lmsg = msg.lower()
        if scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "srm":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "operation not supported" in lmsg:
                return cls.NOT_SUPPORTED

        elif scheme in ("dav", "davs"):
            if "protocol not supported" in lmsg:
                return cls.NOT_SUPPORTED

        elif scheme == "dropbox":
            if "protocol not supported" in lmsg:
                return cls.NOT_SUPPORTED

        else:
            logger.warning(f"scheme '{scheme}' not known to {cls.__name__}, cannot parse '{msg}'")

        return cls.UNKNOWN


class GFALError_unlink(GFALOperationError):

    NOT_FOUND = "target not found"
    IS_DIRECTORY = "target is a directory"

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        lmsg = msg.lower()
        if scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "is a directory" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a file" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "srm":
            if "no such file" in lmsg:
                return cls.NOT_FOUND
            if "not a file" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme in ("dav", "davs"):
            if "file not found" in lmsg:
                return cls.NOT_FOUND
            if "is a directory" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "dropbox":
            if "not_found" in lmsg:
                return cls.NOT_FOUND

        else:
            logger.warning(f"scheme '{scheme}' not known to {cls.__name__}, cannot parse '{msg}'")

        return cls.UNKNOWN


class GFALError_rmdir(GFALOperationError):

    NOT_FOUND = "target not found"
    IS_FILE = "target is a file"
    NOT_EMPTY = "directory is not empty"

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        lmsg = msg.lower()
        if scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a directory" in lmsg:
                return cls.IS_FILE
            if "no such device" in lmsg:
                # cryptic message for non-empty directory
                return cls.NOT_EMPTY

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a directory" in lmsg:
                return cls.IS_FILE
            if "directory is not empty" in lmsg:
                return cls.NOT_EMPTY

        elif scheme == "srm":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "this file is not a directory" in lmsg:
                return cls.IS_FILE
            if "directory not empty" in lmsg:
                return cls.NOT_EMPTY

        elif scheme in ("dav", "davs"):
            if "file not found" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "dropbox":
            if "not_found" in lmsg:
                return cls.NOT_FOUND

        else:
            logger.warning(f"scheme '{scheme}' not known to {cls.__name__}, cannot parse '{msg}'")

        return cls.UNKNOWN


class GFALError_mkdir(GFALOperationError):

    EXISTS = "target already exists"

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        lmsg = msg.lower()
        if scheme == "root":
            if "file exists" in lmsg:
                return cls.EXISTS

        elif scheme == "gsiftp":
            if "directory already exists" in lmsg:
                return cls.EXISTS

        elif scheme == "srm":
            if "directory already exist" in lmsg:
                return cls.EXISTS

        elif scheme == "dropbox":
            if "the directory already exists" in lmsg:
                return cls.EXISTS

        else:
            logger.warning(f"scheme '{scheme}' not known to {cls.__name__}, cannot parse '{msg}'")

        return cls.UNKNOWN


class GFALError_listdir(GFALOperationError):

    EMPTY = "directory is empty"

    @classmethod
    def _get_reason(cls, msg: str, uri: str, scheme: str) -> str:
        lmsg = msg.lower()
        if scheme == "root":
            # xrootd throws an expcetion when a directory is empty
            if lmsg.strip().endswith("invalid response (unknown error 303)"):
                return cls.EMPTY

        else:
            logger.warning(f"scheme '{scheme}' not known to {cls.__name__}, cannot parse '{msg}'")

        return cls.UNKNOWN


class GFALError_filecopy(GFALOperationError):

    SRC_NOT_FOUND = "source not found"
    DST_EXISTS = "target already exists"

    def __init__(
        self,
        src_uri: str,
        dst_uri: str,
        exc=None,
    ) -> None:
        # store uri and scheme
        self.src_uri = str(src_uri)
        self.dst_uri = str(dst_uri)
        self.src_scheme = get_scheme(src_uri) or "file"
        self.dst_scheme = get_scheme(dst_uri) or "file"

        # get the original error objects and find the error reason
        _exc = sys.exc_info() if exc is None else exc
        self.reason = self._get_reason(
            str(_exc[1]),
            self.src_uri,
            self.dst_uri,
            self.src_scheme,
            self.dst_scheme,
        )

        # add the error reason to the message
        msg = f"{_exc[1]} ({self.__class__.__name__}: {self.reason})"

        # bypass the GFALOperationError init
        RetryException.__init__(self, msg=msg, exc=exc)

    @classmethod
    def _get_reason(  # type: ignore[override]
        cls,
        msg: str,
        src_uri: str,
        dst_uri: str,
        src_scheme: str,
        dst_scheme: str,
    ) -> str:
        # in gfal, error messages on missing source files or existing target files depend on both
        # source and destination protocols, so all cases need to be handled separately
        lmsg = msg.lower()
        if (src_scheme, dst_scheme) == ("file", "file"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "root"):
            if "no such file or directory (source)" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists (destination)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "gsiftp"):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "srm"):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("file", "dav"), ("file", "davs")):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND

        elif (src_scheme, dst_scheme) == ("file", "dropbox"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("gsiftp", "file"):
            if "no such file or directory on url" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("gsiftp", "gsiftp"):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND
            if "destination already exist" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("gsiftp", "dav"), ("gsiftp", "davs")):
            if "is a directory" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "file"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "root"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "srm"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("srm", "dav"), ("srm", "davs")):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "file"):
            if "no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists (destination)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "srm"):
            if "no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "root"):
            if "destination does not support delegation." in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("root", "dav"), ("root", "davs")):
            if "failed to open file (block device required)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("dav", "file"), ("davs", "file")):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND

        elif (src_scheme, dst_scheme) in (("dav", "gsiftp"), ("davs", "gsiftp")):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND
            if "not a file" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("dav", "root"), ("davs", "root")):
            # it appears that there is a bug in gfal when copying via davix to xrootd in that
            # the full dst path is repeated, e.g. "root://url.tld:1090/pnfs/.../root://url..."
            # which causes weird behavior, and as long as this issue persists, there should be no
            # error parsing in law
            pass

        elif (src_scheme, dst_scheme) in (("dav", "srm"), ("davs", "srm")):
            # same issue as for davix -> xrootd, wait until this is resolved
            pass

        elif (src_scheme, dst_scheme) == ("dropbox", "file"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("dropbox", "dropbox"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        else:
            logger.warning(
                f"combination of source scheme '{src_scheme}' and target scheme '{dst_scheme}' not "
                f"known to {cls.__name__}, cannot parse '{msg}'",
            )

        return cls.UNKNOWN
