# -*- coding: utf-8 -*-

"""
DCache file system and targets.
"""


__all__ = ["DCacheFileSystem", "DCacheFileTarget", "DCacheDirectoryTarget"]


import os

from law.config import Config
from law.target.remote import RemoteFileSystem, RemoteTarget, RemoteTarget, RemoteFileTarget, \
    RemoteDirectoryTarget


class DCacheFileSystem(RemoteFileSystem):

    default_instance = None

    def __init__(self, config=None, base=None, bases=None, **kwargs):
        # prepare the gfal options
        # resolution order: config, base+bases, default dCache section
        if not config and not base:
            config = Config.instance().get("target", "default_dcache")
        if config and Config.instance().has_section(config):
            base = Config.instance().get_default(config, "base")
            bases = {}
            prefix = "base_"
            for key, value in Config.instance().items(config):
                if key.startswith(prefix):
                    bases[key[len(prefix):]] = value
        if base is None:
            raise Exception("invalid arguments, set either config, base or the "
                "target.default_dcache option in your law config")

        # default configs
        kwargs.setdefault("retry", 1)
        kwargs.setdefault("retry_delay", 5)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("validate_copy", False)
        kwargs.setdefault("cache_config", {})
        kwargs.setdefault("reset_context", True)
        kwargs.setdefault("permissions", False)

        super(DCacheFileSystem, self).__init__(base, bases, **kwargs)


# try to set the default fs instance
try:
    DCacheFileSystem.default_instance = DCacheFileSystem()
except:
    pass


class DCacheTarget(RemoteTarget):

    def __init__(self, path, fs=DCacheFileSystem.default_instance):
        """ __init__(path, fs=DCacheFileSystem.default_instance)
        """
        RemoteTarget.__init__(self, path, fs)


class DCacheFileTarget(DCacheTarget, RemoteFileTarget):

    pass


class DCacheDirectoryTarget(DCacheTarget, RemoteDirectoryTarget):

    pass


DCacheTarget.file_class = DCacheFileTarget
DCacheTarget.directory_class = DCacheDirectoryTarget
