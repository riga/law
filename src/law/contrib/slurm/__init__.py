"""
Slurm contrib functionality.
"""

__all__ = [
    "SlurmJobFileFactory",
    "SlurmJobManager",
    "SlurmWorkflow",
    "get_slurm_version",
]

# provisioning imports
from law.contrib.slurm.job import SlurmJobFileFactory, SlurmJobManager
from law.contrib.slurm.util import get_slurm_version
from law.contrib.slurm.workflow import SlurmWorkflow
