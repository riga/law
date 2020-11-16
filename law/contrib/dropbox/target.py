# coding: utf-8

"""
Dropbox file system and targets.
"""


__all__ = ["DropboxFileSystem", "DropboxTarget", "DropboxFileTarget", "DropboxDirectoryTarget"]


import logging

import six

from law.config import Config
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)


logger = logging.getLogger(__name__)


class DropboxFileSystem(RemoteFileSystem):

    default_instance = None

    @classmethod
    def parse_config(cls, section, config=None):
        config = super(DropboxFileSystem, cls).parse_config(section, config=config)

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func):
            if option not in config:
                config[option] = func(section, option)

        # app key
        add("app_key", cfg.get_expanded)

        # app secret
        add("app_secret", cfg.get_expanded)

        # access token
        add("access_token", cfg.get_expanded)

        return config

    def __init__(self, section=None, **kwargs):
        # default gfal transfer config
        kwargs.setdefault("transfer_config", {})
        kwargs["transfer_config"].setdefault("checksum_check", False)

        # if present, read options from the section in the law config
        self.config_section = None
        cfg = Config.instance()
        if not section:
            section = cfg.get_expanded("target", "default_dropbox_fs")
        if isinstance(section, six.string_types):
            if cfg.has_section(section):
                # extend options of sections other than "dropbox_fs" with its defaults
                if section != "dropbox_fs":
                    data = dict(cfg.items("dropbox_fs", expand_vars=False, expand_user=False))
                    cfg.update({section: data}, overwrite_sections=True, overwrite_options=False)
                kwargs = self.parse_config(section, kwargs)
                self.config_section = section
            else:
                raise Exception("law config has no section '{}' to read {} options".format(
                    section, self.__class__.__name__))

        # base path, app key, app secret and access token are mandatory
        for attr in ["base", "app_key", "app_secret", "access_token"]:
            if not kwargs.get(attr):
                raise Exception("{0}.{1} is missing, set either 'section', '{1}', or change the "
                    "target.default_dropbox_fs option in your law config".format(
                        self.__class__.__name__, attr))

        # pass dropbox options to gfal options
        gfal_options = {
            "integer": [("DROPBOX", "OAUTH", 2)],
            "string": [
                ("DROPBOX", "APP_KEY", str(kwargs.pop("app_key"))),
                ("DROPBOX", "APP_SECRET", str(kwargs.pop("app_secret"))),
                ("DROPBOX", "ACCESS_TOKEN", str(kwargs.pop("access_token"))),
            ],
        }

        base = kwargs.pop("base")
        RemoteFileSystem.__init__(self, base, gfal_options=gfal_options, **kwargs)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
    logger.debug("created default DropboxFileSystem instance '{}'".format(
        DropboxFileSystem.default_instance))
except Exception as e:
    logger.warning("could not create default DropboxFileSystem instance: {}".format(e))


class DropboxTarget(RemoteTarget):

    def __init__(self, path, fs=DropboxFileSystem.default_instance, **kwargs):
        """ __init__(path, fs=DropboxFileSystem.default_instance, **kwargs)
        """
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
