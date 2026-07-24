# coding: utf-8
# flake8: noqa

"""
Helpers and targets providing functionality to work with Hadoop
"""

__all__ = ["HDFSFileSystem", "HDFSTarget", "HDFSFileTarget", "HDFSDirectoryTarget"]


# provisioning imports
from law.contrib.hdfs.target import HDFSFileSystem, HDFSTarget, HDFSFileTarget, HDFSDirectoryTarget
