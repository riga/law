"""
ARC contrib functionality.
"""

__all__ = [
    "ARCJobFileFactory",
    "ARCJobManager",
    "ARCWorkflow",
    "check_arcproxy_validity",
    "ensure_arcproxy",
    "get_arcproxy_file",
    "get_arcproxy_lifetime",
    "get_arcproxy_user",
    "get_arcproxy_vo",
    "renew_arcproxy",
]

# dependencies to other contrib modules
import law

law.contrib.load("wlcg")

# provisioning imports
from law.contrib.arc.job import ARCJobFileFactory, ARCJobManager  # noqa: I001
from law.contrib.arc.util import (
    check_arcproxy_validity,
    get_arcproxy_file,
    get_arcproxy_lifetime,
    get_arcproxy_user,
    get_arcproxy_vo,
    renew_arcproxy,
)
from law.contrib.arc.workflow import ARCWorkflow
from law.contrib.arc.decorator import ensure_arcproxy
