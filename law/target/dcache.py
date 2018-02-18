# -*- coding: utf-8 -*-

"""
DCache file system and targets.
"""


__all__ = ["DCacheFileSystem", "DCacheFileTarget", "DCacheDirectoryTarget"]


import logging

import six

from law.config import Config
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)


logger = logging.getLogger(__name__)


class DCacheFileSystem(RemoteFileSystem):

    default_instance = None

    def __init__(self, config=None, base=None, bases=None, **kwargs):
        # default configs
        kwargs.setdefault("retries", 1)
        kwargs.setdefault("retry_delay", 5)
        kwargs.setdefault("transfer_config", {"checksum_check": False})
        kwargs.setdefault("validate_copy", False)
        kwargs.setdefault("cache_config", {})
        kwargs.setdefault("atomic_contexts", True)
        kwargs.setdefault("permissions", False)

        # prepare the gfal options
        # resolution order: config, base+bases, default dcache section
        cfg = Config.instance()
        if not config and not base:
            config = cfg.get("target", "default_dcache")

        if config and cfg.has_section(config):
            # load the base from the config
            base = cfg.get_default(config, "base")

            # loop through items and load additional configs
            bases = bases or {}
            base_prefix = "base_"
            cache_prefix = "cache_"
            others = ("retries", "retry_delay", "validate_copy", "atomic_contexts", "permissions")
            for key, value in cfg.items(config):
                if key.startswith(base_prefix):
                    bases[key[len(base_prefix):]] = value
                elif key.startswith(cache_prefix):
                    kwargs["cache_config"][key[len(cache_prefix):]] = value
                elif key in others:
                    kwargs[key] = value

        # base is mandatory
        if base is None:
            raise Exception("invalid arguments, set either config, base or the "
                "target.default_dcache option in your law config")

        RemoteFileSystem.__init__(self, base, bases, **kwargs)


# try to set the default fs instance
try:
    DCacheFileSystem.default_instance = DCacheFileSystem()
    logger.debug("created default DCacheFileSystem instance '{}'".format(
        DCacheFileSystem.default_instance))
except:
    logger.debug("could not create default DCacheFileSystem instance")


class DCacheTarget(RemoteTarget):

    def __init__(self, path, fs=DCacheFileSystem.default_instance, **kwargs):
        """ __init__(path, fs=DCacheFileSystem.default_instance, **kwargs)
        """
        if isinstance(fs, six.string_types):
            fs = DCacheFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class DCacheFileTarget(DCacheTarget, RemoteFileTarget):

    pass


class DCacheDirectoryTarget(DCacheTarget, RemoteDirectoryTarget):

    pass


DCacheTarget.file_class = DCacheFileTarget
DCacheTarget.directory_class = DCacheDirectoryTarget
