# -*- coding: utf-8 -*-
# flake8: noqa


__all__ = [
    "Task", "WrapperTask", "ExternalTask",
    "SandboxTask",
    "BaseWorkflow", "LocalWorkflow", "workflow_property", "cached_workflow_property",
    "FileSystemTarget", "FileSystemFileTarget", "FileSystemDirectoryTarget",
    "LocalFileSystem", "LocalTarget", "LocalFileTarget, LocalDirectoryTarget",
    "TargetCollection", "SiblingFileCollection",
    "NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
    "CSVParameter", "NotifyParameter", "NotifyMailParameter",
    "Config",
    "notify_mail",
]


# package infos
from law.__version__ import (
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)


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
from law.config import Config
from law.notification import notify_mail
from law.parameter import (
    NO_STR, NO_INT, NO_FLOAT, is_no_param, get_param, TaskInstanceParameter, CSVParameter,
    NotifyParameter, NotifyMailParameter,
)
from law.target.file import FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget
from law.target.local import LocalFileSystem, LocalTarget, LocalFileTarget, LocalDirectoryTarget
from law.target.collection import TargetCollection, SiblingFileCollection
import law.decorator
from law.task.base import Task, WrapperTask, ExternalTask
from law.workflow.base import BaseWorkflow, workflow_property, cached_workflow_property
from law.workflow.local import LocalWorkflow
from law.sandbox.base import SandboxTask
import law.sandbox.docker
import law.sandbox.bash
import law.job.base
import law.job.dashboard
import law.workflow.remote
import law.contrib
