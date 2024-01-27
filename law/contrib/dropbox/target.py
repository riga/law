# coding: utf-8

"""
Dropbox file system and targets based on the GFAL file interface.
"""

from __future__ import annotations

__all__ = ["DropboxFileSystem", "DropboxTarget", "DropboxFileTarget", "DropboxDirectoryTarget"]

import pathlib

import law
from law.config import Config
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.logger import get_logger
from law._types import Any, Callable


logger = get_logger(__name__)

law.contrib.load("gfal")


class DropboxFileSystem(RemoteFileSystem):

    file_interface_cls = law.gfal.GFALFileInterface  # type: ignore[attr-defined]

    @classmethod
    def parse_config(
        cls,
        section: str,
        config: dict[str, Any] | None = None,
        *,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        config = super().parse_config(section, config=config, overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option: str, func: Callable[[str, str], Any], prefix: str = "dropbox_") -> None:
            if option not in config or overwrite:
                config[option] = func(section, prefix + option)

        # app key
        add("app_key", cfg.get_expanded)

        # app secret
        add("app_secret", cfg.get_expanded)

        # access token
        add("access_token", cfg.get_expanded)

        return config

    def __init__(
        self,
        section: str | None = None,
        *,
        app_key: str | None = None,
        app_secret: str | None = None,
        access_token: str | None = None,
        **kwargs,
    ) -> None:
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(
            section,
            "default_dropbox_fs",
            "dropbox_fs",
            kwargs,
        )

        # store the config section
        self.config_section = section

        # overwrite dropbox credentials with passed values
        if app_key:
            fs_config["app_key"] = app_key
        if app_secret:
            fs_config["app_secret"] = app_secret
        if access_token:
            fs_config["access_token"] = access_token

        # base path, app key, app secret and access token are mandatory
        msg_tmpl = (
            "attribute '{{0}}' must not be empty, set it either directly in the {} constructor, "
            "or add the option '{{0}}' to your config section '{}'"
        ).format(self.__class__.__name__, self.config_section)
        if not fi_config.get("base"):
            raise Exception(msg_tmpl.format("base"))
        for attr in ["app_key", "app_secret", "access_token"]:
            if not fs_config.get(attr):
                raise Exception(msg_tmpl.format(attr))

        # enforce some configs
        fs_config["has_permissions"] = False

        # pass dropbox credentials via gfal options
        gfal_options = fi_config.setdefault("gfal_options", {})
        gfal_options.setdefault("integer", []).append(("DROPBOX", "OAUTH", 2))
        gfal_options.setdefault("string", []).extend([
            ("DROPBOX", "APP_KEY", str(fs_config.pop("app_key"))),
            ("DROPBOX", "APP_SECRET", str(fs_config.pop("app_secret"))),
            ("DROPBOX", "ACCESS_TOKEN", str(fs_config.pop("access_token"))),
        ])

        # create the file interface
        file_interface = self.file_interface_cls(**fi_config)

        # initialize the file system itself
        super().__init__(file_interface, **fs_config)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
    logger.debug(
        f"created default DropboxFileSystem instance '{DropboxFileSystem.default_instance}'",
    )
except Exception as e:
    logger.debug(f"could not create default DropboxFileSystem instance: {e}")


class DropboxTarget(RemoteTarget):
    """ __init__(path, fs=DropboxFileSystem.default_instance, **kwargs)
    """

    def __init__(
        self,
        path: str | pathlib.Path,
        fs: str | pathlib.Path | DropboxFileSystem | None = DropboxFileSystem.default_instance,
        **kwargs,
    ) -> None:
        if fs is None:
            fs = DropboxFileSystem.default_instance
        elif not isinstance(fs, DropboxFileSystem):
            fs = DropboxFileSystem(str(fs))

        super().__init__(path, fs, **kwargs)


class DropboxFileTarget(DropboxTarget, RemoteFileTarget):

    pass


class DropboxDirectoryTarget(DropboxTarget, RemoteDirectoryTarget):

    pass


DropboxTarget.file_class = DropboxFileTarget  # type: ignore[type-abstract]
DropboxTarget.directory_class = DropboxDirectoryTarget  # type: ignore[type-abstract]
