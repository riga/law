# -*- coding: utf-8 -*-

"""
DCache remote file system and targets.
"""


__all__ = ["DCacheFileSystem", "DCacheFileTarget", "DCacheDirectoryTarget"]


import os

from law.config import Config
from law.target.remote import RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteFileTarget, \
                              RemoteDirectoryTarget


class DCacheFileSystem(RemoteFileSystem):

    default = None

    def __init__(self, base=None, bases=None, config_section=None, **kwargs):
        config = self.create_config(base=base, bases=bases, config_section=config_section)
        if config is None:
            raise Exception("could not create a valid dropbox config")

        # default configs
        kwargs.setdefault("retry", 1)
        kwargs.setdefault("retry_delay", 5)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("validate_copy", False)
        kwargs.setdefault("cache_config", {})

        super(DCacheFileSystem, self).__init__(config["base"], bases=config["bases"],
                                               permissions=False, reset_context=True, **kwargs)

    @staticmethod
    def create_config(base=None, bases=None, config_section=None):
        if base is not None:
            return {
                "base": base,
                "bases": bases
            }

        else:
            if config_section is None:
                config_section = Config.instance().get("target", "default_dcache")

            if not Config.instance().has_section(config_section):
                return None

            return {
                "base": Config.instance().get(config_section, "base"),
                "bases": {opt[5:]: val for opt, val in Config.instance().items(config_section) \
                          if opt.startswith("base_")}
            }


# set a default dcache fs when a default config is available
if DCacheFileSystem.create_config():
    DCacheFileSystem.default = DCacheFileSystem()


class DCacheTarget(RemoteTarget):

    def __init__(self, path, fs=None, exists=None):
        if fs is None:
            fs = DCacheFileSystem.default

        RemoteTarget.__init__(self, path, fs, exists=exists)

    def url(self, *args, **kwargs):
        return self.fs.gfal.url(self.path, *args, **kwargs)


class DCacheFileTarget(DCacheTarget, RemoteFileTarget):
    pass


class DCacheDirectoryTarget(DCacheTarget, RemoteDirectoryTarget):
    pass


DCacheTarget.file_class = DCacheFileTarget
DCacheTarget.directory_class = DCacheDirectoryTarget
