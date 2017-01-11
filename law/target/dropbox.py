# -*- coding: utf-8 -*-

"""
Dropbox remote file system and targets.
"""


__all__ = ["DropboxFileSystem", "DropboxFileTarget", "DropboxDirectoryTarget"]


import os
import json

from law.config import Config
from law.target.remote import RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteFileTarget, \
                              RemoteDirectoryTarget


class DropboxFileSystem(RemoteFileSystem):

    default = None

    def __init__(self, base=None, app_key=None, app_secret=None, access_token=None,
                 config_file="$LAW_DROPBOX_CONFIG_FILE", config_section="dropbox", **kwargs):
        config = self.create_config(base=base, app_key=app_key, app_secret=app_secret,
                                    access_token=access_token, config_file=config_file,
                                    config_section=config_section)
        if config is None:
            raise Exception("could not create a valid dropbox config")

        gfal_options = {
            "integer": [("DROPBOX", "OAUTH", 2)],
            "string" : [
                ("DROPBOX", "APP_KEY", str(config["app_key"])),
                ("DROPBOX", "APP_SECRET", str(config["app_secret"])),
                ("DROPBOX", "ACCESS_TOKEN", str(config["access_token"]))
            ]
        }

        # configure the remote file system with defaults
        kwargs.setdefault("retry", 1)
        kwargs.setdefault("retry_delay", 10)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("cache_config", {})

        base = str("dropbox://dropbox.com/" + config["base"].strip("/"))

        super(DropboxFileSystem, self).__init__(base, permissions=False, gfal_options=gfal_options,
                                                **kwargs)

    @staticmethod
    def create_config(base=None, app_key=None, app_secret=None, access_token=None,
                      config_file="$LAW_DROPBOX_CONFIG_FILE", config_section=None):
        if config_file is not None:
            config_file = os.path.expandvars(os.path.expanduser(config_file))

        _config = {"base": base, "app_key": app_key, "app_secret": app_secret,
                   "access_token": access_token}

        def validate(config):
            missing_keys = [key for key in _config if key not in config]
            if missing_keys:
                raise Exception("dropbox config misses key(s) '%s'" % ",".join(missing_keys))

        if any(value is not None for value in _config.values()):
            for key, value in _config.items():
                if value is None:
                    raise Exception("%s must not be None when at least one of %s isn't" \
                                    % (key, ",".join(_config.keys())))
            return _config

        elif config_file is not None and os.path.exists(config_file):
            with open(config_file, "r") as f:
                config = json.load(f)
            validate(config)
            return config

        else:
            if config_section is None:
                config_section = Config.instance().get("target", "default_dropbox")

            config = dict(Config.instance().items(config_section))
            validate(config)
            return config


# set a default dropbox fs when a default config is available
if DropboxFileSystem.create_config():
    DropboxFileSystem.default = DropboxFileSystem()


class DropboxTarget(RemoteTarget):

    def __init__(self, path, fs=None, exists=None):
        if fs is None:
            fs = DropboxFileSystem.default

        RemoteTarget.__init__(self, path, fs, exists=exists)


class DropboxFileTarget(DropboxTarget, RemoteFileTarget):
    pass


class DropboxDirectoryTarget(DropboxTarget, RemoteDirectoryTarget):
    pass


DropboxTarget.file_class = DropboxFileTarget
DropboxTarget.directory_class = DropboxDirectoryTarget
