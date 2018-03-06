# -*- coding: utf-8 -*-
# flake8: noqa

"""
Build large-scale task workflows using luigi, remote job submission, remote targets, and environment
sandboxing.
"""


__author__ = "Marcel Rieger"
__email__ = "python-law@googlegroups.com"
__copyright__ = "Copyright 2018, Marcel Rieger"
__credits__ = ["Marcel Rieger"]
__contact__ = "https://github.com/riga/law"
__license__ = "MIT"
__status__ = "Development"
__version__ = "0.0.20"

__all__ = [
    "Task", "WrapperTask", "SandboxTask",
    "BaseWorkflow", "LocalWorkflow", "workflow_property", "cached_workflow_property",
    "FileSystemTarget", "FileSystemFileTarget", "FileSystemDirectoryTarget",
    "LocalFileSystem, LocalFileTarget, LocalDirectoryTarget",
    "DropboxFileSystem", "DropboxFileTarget", "DropboxDirectoryTarget",
    "DCacheFileSystem", "DCacheFileTarget", "DCacheDirectoryTarget",
    "TargetCollection", "SiblingFileCollection",
    "NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
    "CSVParameter",
    "Config",
]


# use cached software
from law.cli.software import use_software_cache
use_software_cache(reload_deps=True)


# luigi patches
from law.patches import patch_all
patch_all()


# setup logging
import law.logger
law.logger.setup_logging()


# provisioning imports
import law.util
from law.parameter import (
    NO_STR, NO_INT, NO_FLOAT, is_no_param, get_param, TaskInstanceParameter, CSVParameter,
)
from law.target.file import FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget
from law.target.local import LocalFileSystem, LocalFileTarget, LocalDirectoryTarget
from law.target.dropbox import DropboxFileSystem, DropboxFileTarget, DropboxDirectoryTarget
from law.target.dcache import DCacheFileSystem, DCacheFileTarget, DCacheDirectoryTarget
from law.target.collection import TargetCollection, SiblingFileCollection
import law.decorator
from law.task.base import Task, WrapperTask
from law.workflow.base import BaseWorkflow, workflow_property, cached_workflow_property
from law.workflow.local import LocalWorkflow
from law.sandbox.base import SandboxTask
import law.sandbox.docker
import law.sandbox.bash
import law.job.base
import law.workflow.remote
import law.contrib
