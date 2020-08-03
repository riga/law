# coding: utf-8
# flake8: noqa

"""
Slurm contrib functionality.
"""


__all__ = [
    "SlurmJobManager", "SlurmJobFileFactory", "SlurmWorkflow",
]


# provisioning imports
from law.contrib.slurm.job import SlurmJobManager, SlurmJobFileFactory
from law.contrib.slurm.workflow import SlurmWorkflow
