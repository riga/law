"""
HDFS file interface and targets, based on pyarrow.
"""

__all__ = ["HDFSDirectoryTarget", "HDFSFileInterface", "HDFSFileSystem", "HDFSFileTarget", "HDFSTarget"]

# provisioning imports
from law.contrib.hdfs.target import (
    HDFSDirectoryTarget,
    HDFSFileInterface,
    HDFSFileSystem,
    HDFSFileTarget,
    HDFSTarget,
)
