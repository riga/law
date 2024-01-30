# coding: utf-8
# flake8: noqa

"""
gLite contrib functionality.
"""

__all__ = ["GLiteJobManager", "GLiteJobFileFactory", "GLiteWorkflow"]

# dependencies to other contrib modules
import law
law.contrib.load("wlcg")

# provisioning imports
from law.contrib.glite.job import GLiteJobManager, GLiteJobFileFactory
from law.contrib.glite.workflow import GLiteWorkflow
