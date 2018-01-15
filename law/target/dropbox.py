# -*- coding: utf-8 -*-

"""
Dropbox file system and targets.
"""


__all__ = ["DropboxFileSystem", "DropboxFileTarget", "DropboxDirectoryTarget"]


import os

from law.config import Config
from law.target.remote import RemoteFileSystem, RemoteTarget, RemoteTarget, RemoteFileTarget, \
    RemoteDirectoryTarget


class DropboxFileSystem(RemoteFileSystem):

    default_instance = None

    def __init__(self, config=None, base=None, app_key=None, app_secret=None, access_token=None,
            **kwargs):
        # prepare the gfal options
        # resolution order: config, key+secret+token, default dropbox section
        if not config and not app_key and not app_secret and not access_token:
            config = Config.instance().get("target", "default_dropbox")
        if config and Config.instance().has_section(config):
            opts = dict(Config.instance().items(config))
            if base is None and "base" in opts:
                base = opts["base"]
        elif app_key and app_secret and access_token:
            opts = {"app_key": app_key, "app_secret": app_secret, "access_token": access_token}
        else:
            raise Exception("invalid arguments, set either config, app_key+app_secret+access_token "
                "or the target.default_dropbox option in your law config")

        if base is None:
            raise Exception("no base directory set")

        gfal_options = {
            "integer": [("DROPBOX", "OAUTH", 2)],
            "string" : [("DROPBOX", key.upper(), str(value)) for key, value in opts.items()],
        }

        # default configs
        kwargs.setdefault("retries", 1)
        kwargs.setdefault("retry_delay", 5)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("validate_copy", False)
        kwargs.setdefault("cache_config", {})
        kwargs.setdefault("permissions", False)

        base_url = "dropbox://dropbox.com/" + base.strip("/")
        super(DropboxFileSystem, self).__init__(base_url, gfal_options=gfal_options, **kwargs)


# try to set the default fs instance
try:
    DropboxFileSystem.default_instance = DropboxFileSystem()
except:
    pass


class DropboxTarget(RemoteTarget):

    def __init__(self, path, fs=DropboxFileSystem.default_instance):
        """ __init__(path, fs=DropboxFileSystem.default_instance)
        """
        RemoteTarget.__init__(self, path, fs)


class DropboxFileTarget(DropboxTarget, RemoteFileTarget):

    pass


class DropboxDirectoryTarget(DropboxTarget, RemoteDirectoryTarget):

    pass


DropboxTarget.file_class = DropboxFileTarget
DropboxTarget.directory_class = DropboxDirectoryTarget
