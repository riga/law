# coding: utf-8

"""
WLCG remote file system and targets.
"""


__all__ = ["WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget"]


import stat
import logging

import six

from law.config import Config
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)


logger = logging.getLogger(__name__)


class WLCGFileSystem(RemoteFileSystem):

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

        # prepare the gfal options, prefer base[+bases] over config
        if not base:
            cfg = Config.instance()
            if not config:
                config = cfg.get("target", "default_wlcg_fs")

            # config might be a section in the law config
            if isinstance(config, six.string_types) and cfg.has_section(config):
                # parse it
                self.parse_config(config, kwargs)

                # set base and bases explicitely
                _base = kwargs.pop("base", None)
                if base is None:
                    base = _base
                _bases = kwargs.pop("bases", None)
                if bases is None:
                    bases = _bases

        # base is required
        if not base:
            raise Exception("invalid arguments, set either config, base or the "
                "target.default_wlcg_fs option in your law config")

        RemoteFileSystem.__init__(self, base, bases, **kwargs)

    def _s_isdir(self, st_mode):
        # some WLCG file protocols do not return standard st_mode values in stat requests,
        # e.g. srm returns file type bits 0o50000 for directories instead of 0o40000,
        # these differences are rather distinct and can be taken into account here,
        # see http://man7.org/linux/man-pages/man7/inode.7.html for info on st_mode values
        return stat.S_ISDIR(st_mode) or stat.S_IFMT(st_mode) == 0o50000


# try to set the default fs instance
try:
    WLCGFileSystem.default_instance = WLCGFileSystem()
    logger.debug("created default WLCGFileSystem instance '{}'".format(
        WLCGFileSystem.default_instance))
except Exception as e:
    logger.debug("could not create default WLCGFileSystem instance: {}".format(e))


class WLCGTarget(RemoteTarget):

    def __init__(self, path, fs=WLCGFileSystem.default_instance, **kwargs):
        """ __init__(path, fs=WLCGFileSystem.default_instance, **kwargs)
        """
        if isinstance(fs, six.string_types):
            fs = WLCGFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class WLCGFileTarget(WLCGTarget, RemoteFileTarget):

    pass


class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):

    pass


WLCGTarget.file_class = WLCGFileTarget
WLCGTarget.directory_class = WLCGDirectoryTarget
