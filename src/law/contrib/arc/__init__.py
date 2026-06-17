# coding: utf-8
# flake8: noqa

"""
ARC contrib functionality.
"""

__all__ = [
    "get_arcproxy_file", "get_arcproxy_user", "get_arcproxy_lifetime", "get_arcproxy_vo",
    "check_arcproxy_validity", "renew_arcproxy",
    "ARCJobManager", "ARCJobFileFactory", "ARCWorkflow",
    "ensure_arcproxy",
]

# dependencies to other contrib modules
import law
law.contrib.load("wlcg")

# provisioning imports
from law.contrib.arc.util import (
    get_arcproxy_file, get_arcproxy_user, get_arcproxy_lifetime, get_arcproxy_vo,
    check_arcproxy_validity, renew_arcproxy,
)
from law.contrib.arc.job import ARCJobManager, ARCJobFileFactory
from law.contrib.arc.workflow import ARCWorkflow
from law.contrib.arc.decorator import ensure_arcproxy
