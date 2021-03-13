# coding: utf-8

"""
Base classes and tools for defining and working with remote targets.
"""

__all__ = [
    "RemoteFileSystem", "RemoteTarget", "RemoteFileTarget", "RemoteDirectoryTarget",
    "RemoteFileInterface", "RemoteCache",
]


# provisioning imports
from law.target.remote.base import (
    RemoteFileSystem, RemoteTarget, RemoteFileTarget, RemoteDirectoryTarget,
)
from law.target.remote.interface import RemoteFileInterface
from law.target.remote.cache import RemoteCache
