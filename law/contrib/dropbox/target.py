# -*- coding: utf-8 -*-

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

    def __init__(self, config=None, base=None, app_key=None, app_secret=None, access_token=None,
            **kwargs):
        # default configs
        kwargs.setdefault("retries", 1)
        kwargs.setdefault("retry_delay", 5)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("validate_copy", False)
        kwargs.setdefault("cache_config", {})
        kwargs.setdefault("permissions", False)

        # prepare the gfal options
        # resolution order: config, key+secret+token, default dropbox fs section
        cfg = Config.instance()
        if not config:
            config = cfg.get("target", "default_dropbox_fs")

        # config might be a section in the law config
        if cfg.has_section(config):
            # parse it
            self.parse_config(config, kwargs)

            # set base explicitely
            _base = kwargs.pop("base", None)
            if base is None:
                base = _base

            # set dropbox options explicitely
            if not app_key:
                app_key = cfg.get_default(config, "app_key")
            if not app_secret:
                app_secret = cfg.get_default(config, "app_secret")
            if not access_token:
                access_token = cfg.get_default(config, "access_token")

        # base is required
        if base is None:
            raise Exception("no base directory set")

        # dropbox options are required
        if not app_key or not app_secret or not access_token:
            raise Exception("invalid arguments, set either config, app_key+app_secret+access_token "
                "or the target.default_dropbox_fs option in your law config")

        # pass dropbox options to gfal options
        gfal_options = {
            "integer": [("DROPBOX", "OAUTH", 2)],
            "string": [
                ("DROPBOX", "APP_KEY", str(app_key)),
                ("DROPBOX", "APP_SECRET", str(app_secret)),
                ("DROPBOX", "ACCESS_TOKEN", str(access_token)),
            ],
        }

        RemoteFileSystem.__init__(self, base, gfal_options=gfal_options, **kwargs)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
    logger.debug("created default DropboxFileSystem instance '{}'".format(
        DropboxFileSystem.default_instance))
except Exception as e:
    logger.debug("could not create default DropboxFileSystem instance: {}".format(e))


class DropboxTarget(RemoteTarget):

    def __init__(self, path, fs=DropboxFileSystem.default_instance, **kwargs):
        """ __init__(path, fs=DropboxFileSystem.default_instance, **kwargs)
        """
        if isinstance(fs, six.string_types):
            fs = DropboxFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class DropboxFileTarget(DropboxTarget, RemoteFileTarget):

    pass


class DropboxDirectoryTarget(DropboxTarget, RemoteDirectoryTarget):

    pass


DropboxTarget.file_class = DropboxFileTarget
DropboxTarget.directory_class = DropboxDirectoryTarget
