# coding: utf-8

"""
HDFS remote file system and targets
"""

__all__ = ["HDFSFileSystem", "HDFSTarget", "HDFSFileTarget", "HDFSDirectoryTarget"]

import six

import law
from law.target.remote import (
    RemoteFileSystem,
    RemoteTarget,
    RemoteFileTarget,
    RemoteDirectoryTarget,
)
from law.logger import get_logger
from law.contrib.hadoop import HDFSFileInterface


logger = get_logger(__name__)


class HDFSFileSystem(RemoteFileSystem):
    file_interface_cls = HDFSFileInterface

    def __init__(self, section=None, **kwargs):
        # read configs from section and combine them with kwargs to get the file system and
        # file interface configs
        section, fs_config, fi_config = self._init_configs(
            section, "default_hdfs_fs", "hdfs_fs", kwargs
        )

        # store the config section
        self.config_section = section
        fs_config.setdefault("name", self.config_section)

        # base path is mandatory
        if not fi_config.get("base"):
            raise Exception(
                "attribute 'base' must not be empty, set it either directly in the {} "
                "constructor, or add the option 'base' to your config section '{}'".format(
                    self.__class__.__name__, self.config_section
                )
            )

        # enforce some configs
        fs_config["has_permissions"] = False

        # create the file interface
        file_interface = self.file_interface_cls(**fi_config)

        # initialize the file system itself
        super(HDFSFileSystem, self).__init__(file_interface, **fs_config)

    # New methods for Hadoop
    # - copyFromLocal
    # - copyToLocal
    def copy_from_local(self, src, dst, **kwargs):
        if self.is_local(dst):
            return self.local_fs.copy(src, dst)
        return self.file_interface.copy_from_local(src, self.abspath(dst), **kwargs)

    def copy_to_local(self, src, dst, **kwargs):
        if self.is_local(src):
            return self.local_fs.copy(src, dst)
        return self.file_interface.copy_to_local(self.abspath(src), dst, **kwargs)


# try to set the default fs instance
try:
    HDFSFileSystem.default_instance = HDFSFileSystem()
    logger.debug(
        "created default HDFSFileSystem instance '{}'".format(
            HDFSFileSystem.default_instance
        )
    )
except Exception as e:
    logger.debug("could not create default HDFSFileSystem instance: {}".format(e))


class HDFSTarget(RemoteTarget):
    """
    __init__(path, fs=HDFSFileSystem.default_instance, **kwargs)
    """

    def __init__(self, path, fs=HDFSFileSystem.default_instance, **kwargs):
        if fs is None:
            fs = HDFSFileSystem.default_instance
        elif isinstance(fs, six.string_types):
            fs = HDFSFileSystem(fs)
        RemoteTarget.__init__(self, path, fs, **kwargs)


class HDFSFileTarget(HDFSTarget, RemoteFileTarget):
    pass


class HDFSDirectoryTarget(HDFSTarget, RemoteDirectoryTarget):
    pass


HDFSTarget.file_class = HDFSFileTarget
HDFSTarget.directory_class = HDFSDirectoryTarget
