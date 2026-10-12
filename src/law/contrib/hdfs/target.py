"""
HDFS file interface, remote file system and targets, based on pyarrow.
"""

from __future__ import annotations

__all__ = ["HDFSDirectoryTarget", "HDFSFileInterface", "HDFSFileSystem", "HDFSFileTarget", "HDFSTarget"]

import os
import pathlib
import stat as _stat
import urllib.parse

from law._types import Any, Callable, Sequence
from law.config import Config
from law.errors import ConfigError
from law.logger import get_logger
from law.target.file import get_scheme, has_scheme, remove_scheme
from law.target.remote import (
    RemoteDirectoryTarget,
    RemoteFileSystem,
    RemoteFileTarget,
    RemoteTarget,
)
from law.target.remote.interface import RemoteFileInterface, RetryException

logger = get_logger(__name__)


# try to import pyarrow.fs
try:
    import pyarrow.fs as pafs
    HAS_PYARROW = True

except ImportError:
    HAS_PYARROW = False

    class PyArrowFSDummy:

        def __getattr__(self, attr):
            raise ImportError(f"trying to access 'pyarrow.fs.{attr}', but pyarrow is not installed")

    pafs = PyArrowFSDummy()


class HDFSFileInterface(RemoteFileInterface):
    """
    Remote file interface for the Hadoop Distributed File System (HDFS), using :py:class:`pyarrow.fs.HadoopFileSystem`.
    Base uris should have the form ``hdfs://host[:port]/path``, where a missing host refers to the default file system
    configured in the Hadoop client configuration (``fs.defaultFS``). Paths with a ``file://`` scheme are handled by
    :py:class:`pyarrow.fs.LocalFileSystem`, which is used for transfers between the local and remote file system.

    *user* and *kerb_ticket* are forwarded to :py:class:`pyarrow.fs.HadoopFileSystem`. Note that it requires Java and
    the Hadoop client libraries (see the pyarrow documentation for the required environment variables), and that it
    starts a JVM in the current process. Connections are therefore only established on first use and are stored per
    process. However, processes forked after a connection was established, e.g. by luigi workers, might not be able to
    use the JVM properly. All other *kwargs* are forwarded to :py:class:`~law.target.remote.RemoteFileInterface`.
    """

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
        def add(option: str, func: Callable[[str, str], Any]) -> None:
            if option not in _config or overwrite:
                _config[option] = func(section, "hdfs_" + option)

        add("user", cfg.get_expanded)
        add("kerb_ticket", cfg.get_expanded)

        return _config

    def __init__(
        self,
        *,
        user: str | None = None,
        kerb_ticket: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.user = user
        self.kerb_ticket = kerb_ticket

        # cache for file system objects per pid and location
        self._file_systems: dict[tuple[int, str], pafs.FileSystem] = {}

    def resolve(self, uri: str | pathlib.Path) -> tuple[pafs.FileSystem, str]:
        """
        Returns a 2-tuple with the pyarrow file system that handles *uri*, and the path within that file system. Local
        paths without scheme or with a ``file://`` scheme are handled by :py:class:`pyarrow.fs.LocalFileSystem`.

        :param uri: The uri.
        :raises ValueError: When the scheme of *uri* is not supported.
        :return: The file system and the path.
        """
        uri = str(uri)
        scheme = get_scheme(uri)

        if scheme in (None, "file"):
            return pafs.LocalFileSystem(), remove_scheme(uri)

        if scheme != "hdfs":
            raise ValueError(f"{self.__class__.__name__} cannot handle uri '{uri}' with scheme '{scheme}'")

        # file system objects are stored per pid and location, so create one if it does not exist yet
        parsed = urllib.parse.urlparse(uri)
        key = (os.getpid(), parsed.netloc)
        if key not in self._file_systems:
            logger.debug(f"connecting to hdfs://{parsed.netloc}")
            self._file_systems[key] = pafs.HadoopFileSystem(
                parsed.hostname or "default",
                port=parsed.port or 0,
                user=self.user,
                kerb_ticket=self.kerb_ticket,
            )

        return self._file_systems[key], parsed.path or "/"

    def _info(self, path: str | pathlib.Path, base_name: str | Sequence[str], base: str | Sequence[str] | None):
        uri: str = self.uri(path, base_name=base_name, base=base, return_all=False)  # type: ignore[assignment]
        fs, _path = self.resolve(uri)
        logger.debug(f"invoking pyarrow get_file_info({uri})")
        return uri, fs, _path, fs.get_file_info(_path)

    @classmethod
    def _to_stat(cls, info: pafs.FileInfo) -> os.stat_result:
        # permissions are not exposed by pyarrow, so only the file type bits are set
        mode = _stat.S_IFDIR if info.type == pafs.FileType.Directory else _stat.S_IFREG
        size = info.size or 0
        mtime = info.mtime_ns / 1e9 if info.mtime_ns is not None else 0.0
        return os.stat_result((mode, 0, 0, 1, 0, 0, size, mtime, mtime, mtime))

    def exists(
        self,
        path: str | pathlib.Path,
        *,
        stat: bool = False,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> bool | os.stat_result | None:
        _, _, _, info = self._info(path, "stat" if stat else ("exists", "stat"), base)
        if info.type == pafs.FileType.NotFound:
            return None if stat else False
        return self._to_stat(info) if stat else True

    @RemoteFileInterface.retry(uri_base_name="stat")
    def stat(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> os.stat_result:
        try:
            uri, _, _, info = self._info(path, "stat", base)
        except OSError as e:
            raise RetryException() from e

        if info.type == pafs.FileType.NotFound:
            raise FileNotFoundError(f"no such file or directory: {uri}")

        return self._to_stat(info)

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

        return bool(stat) and _stat.S_ISDIR(stat.st_mode)  # type: ignore[union-attr]

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

        return bool(stat) and _stat.S_ISREG(stat.st_mode)  # type: ignore[union-attr]

    def chmod(
        self,
        path: str | pathlib.Path,
        perm: int | None,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = False,
        **kwargs,
    ) -> bool:
        # permissions are not supported by pyarrow
        if perm is None or silent:
            return perm is None
        raise NotImplementedError(f"{self.__class__.__name__} does not support chmod operations")

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def unlink(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri, fs, _path, info = self._info(path, "unlink", base)
        if info.type == pafs.FileType.NotFound:
            if silent:
                return False
            raise FileNotFoundError(f"no such file: {uri}")
        if info.type == pafs.FileType.Directory:
            raise IsADirectoryError(f"cannot unlink directory: {uri}")

        try:
            logger.debug(f"invoking pyarrow delete_file({uri})")
            fs.delete_file(_path)
        except OSError as e:
            raise RetryException() from e

        return True

    @RemoteFileInterface.retry(uri_base_name="rmdir")
    def rmdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri, fs, _path, info = self._info(path, "rmdir", base)
        if info.type == pafs.FileType.NotFound:
            if silent:
                return False
            raise FileNotFoundError(f"no such directory: {uri}")
        if info.type != pafs.FileType.Directory:
            raise NotADirectoryError(f"not a directory: {uri}")

        # pyarrow removes directories recursively, so check that it is empty first
        if fs.get_file_info(pafs.FileSelector(_path)):
            raise OSError(f"directory not empty: {uri}")

        try:
            logger.debug(f"invoking pyarrow delete_dir({uri})")
            fs.delete_dir(_path)
        except OSError as e:
            raise RetryException() from e

        return True

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def remove(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        silent: bool = True,
        **kwargs,
    ) -> bool:
        uri, fs, _path, info = self._info(path, "unlink", base)
        if info.type == pafs.FileType.NotFound:
            if silent:
                return False
            raise FileNotFoundError(f"no such file or directory: {uri}")

        try:
            if info.type == pafs.FileType.Directory:
                logger.debug(f"invoking pyarrow delete_dir({uri})")
                fs.delete_dir(_path)
            else:
                logger.debug(f"invoking pyarrow delete_file({uri})")
                fs.delete_file(_path)
        except OSError as e:
            raise RetryException() from e

        return True

    def _mkdir(
        self,
        path: str | pathlib.Path,
        base_name: str | Sequence[str],
        base: str | Sequence[str] | None,
        silent: bool,
        recursive: bool,
    ) -> bool:
        uri, fs, _path, info = self._info(path, base_name, base)
        if info.type != pafs.FileType.NotFound:
            # fail silently only when uri is really a directory
            if silent and info.type == pafs.FileType.Directory:
                return False
            raise FileExistsError(f"file or directory exists: {uri}")

        try:
            logger.debug(f"invoking pyarrow create_dir({uri}, recursive={recursive})")
            fs.create_dir(_path, recursive=recursive)
        except OSError as e:
            raise RetryException() from e

        return True

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
        return self._mkdir(path, "mkdir", base, silent, False)

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
        return self._mkdir(path, ["mkdir_rec", "mkdir"], base, silent, True)

    @RemoteFileInterface.retry(uri_base_name="listdir")
    def listdir(
        self,
        path: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> list[str]:
        uri: str = self.uri(path, base_name="listdir", base=base, return_all=False)  # type: ignore[assignment]
        fs, _path = self.resolve(uri)
        try:
            logger.debug(f"invoking pyarrow get_file_info(FileSelector({uri}))")
            infos = fs.get_file_info(pafs.FileSelector(_path))
        except FileNotFoundError:
            raise
        except OSError as e:
            raise RetryException() from e

        return [info.base_name for info in infos]

    @RemoteFileInterface.retry(uri_base_name="filecopy")
    def filecopy(
        self,
        src: str | pathlib.Path,
        dst: str | pathlib.Path,
        *,
        base: str | Sequence[str] | None = None,
        **kwargs,
    ) -> tuple[str, str]:
        if has_scheme(src):
            src_uri = self.sanitize_path(src)
        else:
            src_uri = self.uri(src, base_name="filecopy", base=base, return_all=False)  # type: ignore[assignment]

        if has_scheme(dst):
            dst_uri = self.sanitize_path(dst)
        else:
            dst_uri = self.uri(dst, base_name="filecopy", base=base, return_all=False)  # type: ignore[assignment]

        src_fs, src_path = self.resolve(src_uri)
        dst_fs, dst_path = self.resolve(dst_uri)

        if src_fs.get_file_info(src_path).type != pafs.FileType.File:
            raise FileNotFoundError(f"no such file: {src_uri}")

        try:
            logger.debug(f"invoking pyarrow copy_files({src_uri}, {dst_uri})")
            dst_fs.create_dir(os.path.dirname(dst_path), recursive=True)
            pafs.copy_files(src_path, dst_path, source_filesystem=src_fs, destination_filesystem=dst_fs)
        except OSError as e:
            raise RetryException() from e

        return src_uri, dst_uri


class HDFSFileSystem(RemoteFileSystem):
    """
    Remote file system for the Hadoop Distributed File System (HDFS), using :py:class:`HDFSFileInterface` for file
    operations. Its options are read from the config section *section*, defaulting to the one configured in
    ``[target] default_hdfs_fs``, and can be overwritten by *kwargs*. The ``base`` option, i.e., the base uri(s) of the
    file system, is mandatory. Permissions are not supported.
    """

    file_interface_cls = HDFSFileInterface  # type: ignore[assignment]

    def __init__(self, section: str | None = None, **kwargs) -> None:
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(section, "default_hdfs_fs", "_hdfs_fs_defaults", kwargs)

        # store the config section
        self.config_section = section
        fs_config.setdefault("name", self.config_section)

        # base path is mandatory
        if not fi_config.get("base"):
            raise ConfigError(
                "attribute 'base' must not be empty, set it either directly in the "
                f"{self.__class__.__name__} constructor, or add the option 'base' to your config "
                f"section '{self.config_section}'",
            )

        # enforce some configs
        fs_config["has_permissions"] = False

        # create the file interface
        file_interface = self.file_interface_cls(**fi_config)

        # initialize the file system itself
        super().__init__(file_interface, **fs_config)


# try to set the default fs instance
try:
    HDFSFileSystem.default_instance = HDFSFileSystem()
    logger.debug(f"created default HDFSFileSystem instance '{HDFSFileSystem.default_instance}'")
except Exception as e:
    logger.debug(f"could not create default HDFSFileSystem instance: {e}")


class HDFSTarget(RemoteTarget):
    """
    Base class of targets on a :py:class:`HDFSFileSystem` *fs*, which can be an instance or the name of a config
    section, and defaults to the default instance. All *kwargs* are forwarded to
    :py:class:`~law.target.remote.RemoteTarget`.
    """

    def __init__(
        self,
        path: str | pathlib.Path,
        fs: str | pathlib.Path | HDFSFileSystem | None = HDFSFileSystem.default_instance,  # type: ignore[assignment]
        **kwargs,
    ) -> None:
        if fs is None:
            fs = HDFSFileSystem.default_instance  # type: ignore[assignment]
        elif not isinstance(fs, HDFSFileSystem):
            fs = HDFSFileSystem(str(fs))

        super().__init__(path, fs, **kwargs)  # type: ignore[arg-type]


class HDFSFileTarget(HDFSTarget, RemoteFileTarget):
    """
    Target that refers to a file on HDFS.
    """


class HDFSDirectoryTarget(HDFSTarget, RemoteDirectoryTarget):
    """
    Target that refers to a directory on HDFS.
    """


HDFSTarget.file_class = HDFSFileTarget
HDFSTarget.directory_class = HDFSDirectoryTarget
