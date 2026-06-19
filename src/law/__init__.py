from __future__ import annotations

__all__ = [
    "NO_FLOAT",
    "NO_INT",
    "NO_STR",
    "BaseJobFileFactory",
    "BaseJobManager",
    "BaseWorkflow",
    "BashSandbox",
    "BytesParameter",
    "CSVParameter",
    "Config",
    "DurationParameter",
    "ExternalTask",
    "FileCollection",
    "FileSystemDirectoryTarget",
    "FileSystemFileTarget",
    "FileSystemTarget",
    "JobArguments",
    "JobInputFile",
    "LocalDirectoryTarget",
    "LocalFileSystem",
    "LocalFileTarget",
    "LocalTarget",
    "LocalWorkflow",
    "MirroredDirectoryTarget",
    "MirroredFileTarget",
    "MirroredTarget",
    "MultiCSVParameter",
    "MultiRangeParameter",
    "NestedSiblingFileCollection",
    "NotifyCustomParameter",
    "NotifyMailParameter",
    "NotifyMultiParameter",
    "NotifyParameter",
    "OptionalBoolParameter",
    "Parameter",
    "RangeParameter",
    "Register",
    "Sandbox",
    "SandboxTask",
    "SiblingFileCollection",
    "TargetCollection",
    "Task",
    "TaskInstanceParameter",
    "VenvSandbox",
    "WorkflowParameter",
    "WrapperTask",
    "dynamic_workflow_condition",
    "get_param",
    "is_no_param",
    "localize_file_targets",
    "luigi_version",
    "luigi_version_info",
    "no_value",
    "notify_mail",
    "run",
    "workflow_property",
]

import os
import re

import luigi

# package infos
from law.__meta__ import (  # noqa: F401
    __doc__, __author__, __email__, __copyright__, __credits__, __contact__, __license__,
    __status__, __version__,
)

# luigi version infos
luigi_version = luigi.__version__
version_match = re.match(r"^(\d+)\.(\d+)\.(\d+)(.*)$", luigi_version)
if version_match is None:
    raise RuntimeError(f"could not parse luigi version '{luigi_version}'")
luigi_version_info = tuple(
    int(part) if i < 3 else part
    for i, part in enumerate(version_match.groups())
)

# setup logging
import law.logger
law.logger.setup_logging()

# prefer cached software
if os.getenv("LAW_USE_SOFTWARE_CACHE", "1").lower() in ("1", "yes", "true"):
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
    NO_STR, NO_INT, NO_FLOAT, is_no_param, get_param, Parameter, TaskInstanceParameter,
    OptionalBoolParameter, DurationParameter, BytesParameter, CSVParameter, MultiCSVParameter,
    RangeParameter, MultiRangeParameter, NotifyParameter, NotifyMultiParameter, NotifyMailParameter,
    NotifyCustomParameter,
)
from law.target.file import (
    FileSystemTarget, FileSystemFileTarget, FileSystemDirectoryTarget, localize_file_targets,
)
from law.target.local import LocalFileSystem, LocalTarget, LocalFileTarget, LocalDirectoryTarget
from law.target.collection import (
    TargetCollection, FileCollection, SiblingFileCollection, NestedSiblingFileCollection,
)
from law.target.mirrored import MirroredTarget, MirroredFileTarget, MirroredDirectoryTarget
import law.decorator
from law.task.base import Register, Task, WrapperTask, ExternalTask
from law.workflow.base import (
    BaseWorkflow, WorkflowParameter, workflow_property, dynamic_workflow_condition,
)
from law.workflow.local import LocalWorkflow
from law.sandbox.base import Sandbox, SandboxTask
from law.sandbox.bash import BashSandbox
from law.sandbox.venv import VenvSandbox
from law.job.base import BaseJobManager, BaseJobFileFactory, JobInputFile, JobArguments
import law.job.dashboard
import law.workflow.remote
import law.contrib
