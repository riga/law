# coding: utf-8

"""
Dropbox file system and targets based on the GFAL file interface.
"""

__all__ = ["DropboxFileSystem", "DropboxTarget", "DropboxFileTarget", "DropboxDirectoryTarget"]


import six

import law
from law.config import Config
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.logger import get_logger


logger = get_logger(__name__)

law.contrib.load("gfal")


class DropboxFileSystem(RemoteFileSystem):

    file_interface_cls = law.gfal.GFALFileInterface

    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        config = super(DropboxFileSystem, cls).parse_config(section, config=config,
            overwrite=overwrite)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func, prefix="dropbox_"):
            if option not in config or overwrite:
                config[option] = func(section, prefix + option)

        # app key
        add("app_key", cfg.get_expanded)

        # app secret
        add("app_secret", cfg.get_expanded)

        # access token
        add("access_token", cfg.get_expanded)

        return config

    def __init__(self, section=None, app_key=None, app_secret=None, access_token=None, **kwargs):
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(section, "default_dropbox_fs",
            "dropbox_fs", kwargs)

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
        msg = ("attribute '{{0}}' must not be empty, set it either directly in the {} constructor, "
            "or add the option '{{0}}' to your config section '{}'".format(
                self.__class__.__name__, self.config_section))
        if not fi_config.get("base"):
            raise Exception(msg.format("base"))
        for attr in ["app_key", "app_secret", "access_token"]:
            if not fs_config.get(attr):
                raise Exception(msg.format(attr))

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
        super(DropboxFileSystem, self).__init__(file_interface, **fs_config)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
    logger.debug("created default DropboxFileSystem instance '{}'".format(
        DropboxFileSystem.default_instance))
except Exception as e:
    logger.warning("could not create default DropboxFileSystem instance: {}".format(e))


class DropboxTarget(RemoteTarget):
    """ __init__(path, fs=DropboxFileSystem.default_instance, **kwargs)
    """

    def __init__(self, path, fs=DropboxFileSystem.default_instance, **kwargs):
        if fs is None:
            fs = DropboxFileSystem.default_instance
        elif isinstance(fs, six.string_types):
            fs = DropboxFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class DropboxFileTarget(DropboxTarget, RemoteFileTarget):

    pass


class DropboxDirectoryTarget(DropboxTarget, RemoteDirectoryTarget):

    pass


DropboxTarget.file_class = DropboxFileTarget
DropboxTarget.directory_class = DropboxDirectoryTarget
