# coding: utf-8
# flake8: noqa

"""
Slurm contrib functionality.
"""

__all__ = [
    "get_slurm_version",
    "SlurmJobManager", "SlurmJobFileFactory",
    "SlurmWorkflow",
]


# provisioning imports
from law.contrib.slurm.util import get_slurm_version
from law.contrib.slurm.job import SlurmJobManager, SlurmJobFileFactory
from law.contrib.slurm.workflow import SlurmWorkflow
