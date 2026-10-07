"""
Helpers and targets providing functionality to work with the Worldwide LHC Computing Grid.
"""

__all__ = [
    "WLCGDirectoryTarget",
    "WLCGFileSystem",
    "WLCGFileTarget",
    "WLCGTarget",
    "check_vomsproxy_validity",
    "delegate_myproxy",
    "delegate_vomsproxy_glite",
    "ensure_vomsproxy",
    "get_ce_endpoint",
    "get_myproxy_info",
    "get_usercert",
    "get_usercert_subject",
    "get_userkey",
    "get_vomsproxy_file",
    "get_vomsproxy_identity",
    "get_vomsproxy_lifetime",
    "get_vomsproxy_vo",
    "renew_vomsproxy",
]

# dependencies to other contrib modules
import law

law.contrib.load("gfal")

# provisioning imports
from law.contrib.wlcg.target import WLCGDirectoryTarget, WLCGFileSystem, WLCGFileTarget, WLCGTarget   # noqa: I001
from law.contrib.wlcg.util import (
    check_vomsproxy_validity,
    delegate_myproxy,
    delegate_vomsproxy_glite,
    get_ce_endpoint,
    get_myproxy_info,
    get_usercert,
    get_usercert_subject,
    get_userkey,
    get_vomsproxy_file,
    get_vomsproxy_identity,
    get_vomsproxy_lifetime,
    get_vomsproxy_vo,
    renew_vomsproxy,
)
from law.contrib.wlcg.decorator import ensure_vomsproxy
