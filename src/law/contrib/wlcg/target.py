"""
WLCG remote file system and targets.
"""

from __future__ import annotations

__all__ = ["WLCGDirectoryTarget", "WLCGFileSystem", "WLCGFileTarget", "WLCGTarget"]

import pathlib

import law
from law.errors import ConfigError
from law.logger import get_logger
from law.target.remote import (
    RemoteDirectoryTarget,
    RemoteFileSystem,
    RemoteFileTarget,
    RemoteTarget,
)

logger = get_logger(__name__)


class WLCGFileSystem(RemoteFileSystem):
    """
    Remote file system for storage elements of the Worldwide LHC Computing Grid (WLCG), using
    :py:class:`law.gfal.GFALFileInterface` for file operations. Its options are read from the config section *section*,
    defaulting to the one configured in ``[target] default_wlcg_fs``, and can be overwritten by *kwargs*. The ``base``
    option, i.e., the base uri(s) of the file system, is mandatory. Permissions are not supported.
    """

    file_interface_cls = law.gfal.GFALFileInterface  # type: ignore[attr-defined]

    def __init__(self, section: str | None = None, **kwargs) -> None:
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(section, "default_wlcg_fs", "_wlcg_fs_defaults", kwargs)

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
    WLCGFileSystem.default_instance = WLCGFileSystem()
    logger.debug(f"created default WLCGFileSystem instance '{WLCGFileSystem.default_instance}'")
except Exception as e:
    logger.debug(f"could not create default WLCGFileSystem instance: {e}")


class WLCGTarget(RemoteTarget):
    """
    Base class of targets on a :py:class:`WLCGFileSystem` *fs*, which can be an instance or the name of a config
    section, and defaults to the default instance. All *kwargs* are forwarded to
    :py:class:`~law.target.remote.RemoteTarget`.
    """

    def __init__(
        self,
        path: str | pathlib.Path,
        fs: str | pathlib.Path | WLCGFileSystem | None = WLCGFileSystem.default_instance,  # type: ignore[assignment]
        **kwargs,
    ) -> None:
        if fs is None:
            fs = WLCGFileSystem.default_instance  # type: ignore[assignment]
        elif not isinstance(fs, WLCGFileSystem):
            fs = WLCGFileSystem(str(fs))

        super().__init__(path, fs, **kwargs)  # type: ignore[arg-type]


class WLCGFileTarget(WLCGTarget, RemoteFileTarget):
    """
    Target that refers to a file on a WLCG storage element.
    """


class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):
    """
    Target that refers to a directory on a WLCG storage element.
    """


WLCGTarget.file_class = WLCGFileTarget
WLCGTarget.directory_class = WLCGDirectoryTarget
