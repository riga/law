# -*- coding: utf-8 -*-

"""
Dropbox file system and targets.
"""


__all__ = ["DropboxFileSystem", "DropboxFileTarget", "DropboxDirectoryTarget"]


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
        # resolution order: config, key+secret+token, default dropbox section
        cfg = Config.instance()
        if not config and not app_key and not app_secret and not access_token:
            config = cfg.get("target", "default_dropbox")

        if config and cfg.has_section(config):
            # load options from the config
            opts = {attr: cfg.get_default(config, attr)
                    for attr in ("app_key", "app_secret", "access_token")}
            if base is None:
                base = cfg.get_default(config, "base")

            # loop through items and load optional configs
            cache_prefix = "cache_"
            others = ("retries", "retry_delay", "validate_copy", "atomic_contexts", "permissions")
            for key, value in cfg.items(config):
                if key.startswith(cache_prefix):
                    kwargs["cache_config"][key[len(cache_prefix):]] = value
                elif key in others:
                    kwargs[key] = value

        elif app_key and app_secret and access_token:
            opts = {"app_key": app_key, "app_secret": app_secret, "access_token": access_token}

        else:
            raise Exception("invalid arguments, set either config, app_key+app_secret+access_token "
                "or the target.default_dropbox option in your law config")

        # base is mandatory
        if base is None:
            raise Exception("no base directory set")

        # special dropbox options
        gfal_options = {
            "integer": [("DROPBOX", "OAUTH", 2)],
            "string": [("DROPBOX", key.upper(), str(value)) for key, value in opts.items()],
        }

        base_url = "dropbox://dropbox.com/" + base.strip("/")
        RemoteFileSystem.__init__(self, base_url, gfal_options=gfal_options, **kwargs)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
    logger.debug("created default DropboxFileSystem instance '{}'".format(
        DropboxFileSystem.default_instance))
except:
    logger.debug("could not create default DropboxFileSystem instance")


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
