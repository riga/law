# coding: utf-8

"""
WLCG remote file system and targets.
"""

__all__ = ["WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget"]


import six

import law
from law.target.remote import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.logger import get_logger


logger = get_logger(__name__)

law.contrib.load("gfal")


class WLCGFileSystem(RemoteFileSystem):

    file_interface_cls = law.gfal.GFALFileInterface

    def __init__(self, section=None, **kwargs):
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(section, "default_wlcg_fs", "wlcg_fs",
            kwargs)

        # store the config section
        self.config_section = section

        # base path is mandatory
        if not fi_config.get("base"):
            raise Exception("attribute 'base' must not be empty, set it either directly in the {} "
                "constructor, or add the option 'base' to your config section '{}'".format(
                    self.__class__.__name__, self.config_section))

        # enforce some configs
        fs_config["has_permissions"] = False

        # create the file interface
        file_interface = self.file_interface_cls(**fi_config)

        # initialize the file system itself
        super(WLCGFileSystem, self).__init__(file_interface, **fs_config)


# try to set the default fs instance
try:
    WLCGFileSystem.default_instance = WLCGFileSystem()
    logger.debug("created default WLCGFileSystem instance '{}'".format(
        WLCGFileSystem.default_instance))
except Exception as e:
    logger.warning("could not create default WLCGFileSystem instance: {}".format(e))


class WLCGTarget(RemoteTarget):
    """ __init__(path, fs=WLCGFileSystem.default_instance, **kwargs)
    """

    def __init__(self, path, fs=WLCGFileSystem.default_instance, **kwargs):
        if fs is None:
            fs = WLCGFileSystem.default_instance
        elif isinstance(fs, six.string_types):
            fs = WLCGFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class WLCGFileTarget(WLCGTarget, RemoteFileTarget):

    pass


class WLCGDirectoryTarget(WLCGTarget, RemoteDirectoryTarget):

    pass


WLCGTarget.file_class = WLCGFileTarget
WLCGTarget.directory_class = WLCGDirectoryTarget
