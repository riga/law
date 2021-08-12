# coding: utf-8
# flake8: noqa

__all__ = [
    "Task", "WrapperTask", "ExternalTask",
    "SandboxTask",
    "BaseWorkflow", "LocalWorkflow", "workflow_property", "cached_workflow_property",
    "FileSystemTarget", "FileSystemFileTarget", "FileSystemDirectoryTarget",
    "LocalFileSystem", "LocalTarget", "LocalFileTarget, LocalDirectoryTarget",
    "TargetCollection", "FileCollection", "SiblingFileCollection", "NestedSiblingFileCollection",
    "Sandbox", "BashSandbox",
    "NO_STR", "NO_INT", "NO_FLOAT", "is_no_param", "get_param", "TaskInstanceParameter",
    "DurationParameter", "CSVParameter", "MultiCSVParameter", "RangeParameter",
    "MultiRangeParameter", "NotifyParameter", "NotifyMultiParameter", "NotifyMailParameter",
    "Config",
    "run", "no_value",
    "notify_mail",
]


# package infos
from law.__version__ import (
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)


# setup logging
import law.logger
law.logger.setup_logging()


# use cached software
from law.cli.software import use_software_cache
use_software_cache(reload_deps=True)


# luigi patches
import law.patches
law.patches.patch_all()


# provisioning imports
import law.util
from law.util import law_run as run, no_value
from law.config import Config
from law.notification import notify_mail
from law.parameter import (
    NO_STR, NO_INT, NO_FLOAT, is_no_param, get_param, TaskInstanceParameter, DurationParameter,
    CSVParameter, MultiCSVParameter, RangeParameter, MultiRangeParameter, NotifyParameter,
    NotifyMultiParameter, NotifyMailParameter,
)
from law.target.file import (
    FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, localize_file_targets,
)
from law.target.local import LocalFileSystem, LocalTarget, LocalFileTarget, LocalDirectoryTarget
from law.target.collection import (
    TargetCollection, FileCollection, SiblingFileCollection, NestedSiblingFileCollection,
)
import law.decorator
from law.task.base import Task, WrapperTask, ExternalTask
from law.workflow.base import BaseWorkflow, workflow_property, cached_workflow_property
from law.workflow.local import LocalWorkflow
from law.sandbox.base import Sandbox, SandboxTask
from law.sandbox.bash import BashSandbox
import law.job.base
import law.job.dashboard
import law.workflow.remote
import law.contrib
