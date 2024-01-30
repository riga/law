# coding: utf-8

"""
WLCG remote file system and targets.
"""

from __future__ import annotations

__all__ = ["WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget"]

import pathlib

import law
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.logger import get_logger


logger = get_logger(__name__)


class WLCGFileSystem(RemoteFileSystem):

    file_interface_cls = law.gfal.GFALFileInterface  # type: ignore[attr-defined]

    def __init__(self, section: str | None = None, **kwargs) -> None:
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(
            section,
            "default_wlcg_fs",
            "wlcg_fs",
            kwargs,
        )

        # store the config section
        self.config_section = section
        fs_config.setdefault("name", self.config_section)

        # base path is mandatory
        if not fi_config.get("base"):
            raise Exception(
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

    def __init__(
        self,
        path: str | pathlib.Path,
        fs: str | pathlib.Path | WLCGFileSystem | None = WLCGFileSystem.default_instance,
        **kwargs,
    ) -> None:
        if fs is None:
            fs = WLCGFileSystem.default_instance
        elif not isinstance(fs, WLCGFileSystem):
            fs = WLCGFileSystem(str(fs))

        super().__init__(path, fs, **kwargs)


class WLCGFileTarget(WLCGTarget, RemoteFileTarget):

    pass


class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):

    pass


WLCGTarget.file_class = WLCGFileTarget  # type: ignore[type-abstract]
WLCGTarget.directory_class = WLCGDirectoryTarget  # type: ignore[type-abstract]
