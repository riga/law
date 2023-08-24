# coding: utf-8
# flake8: noqa

"""
Helpers and targets providing functionality to work with the Worldwide LHC Computing Grid.
"""

__all__ = [
    "get_userkey", "get_usercert", "get_usercert_subject", "get_vomsproxy_file",
    "get_vomsproxy_identity", "get_vomsproxy_lifetime", "get_vomsproxy_vo",
    "check_vomsproxy_validity", "renew_vomsproxy", "delegate_vomsproxy_glite",
    "delegate_myproxy", "get_myproxy_info", "get_ce_endpoint",
    "WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget",
    "ensure_vomsproxy",
]


# provisioning imports
from law.contrib.wlcg.util import (
    get_userkey, get_usercert, get_usercert_subject,
    get_vomsproxy_file, get_vomsproxy_identity, get_vomsproxy_lifetime, get_vomsproxy_vo,
    check_vomsproxy_validity, renew_vomsproxy, delegate_vomsproxy_glite,
    delegate_myproxy, get_myproxy_info,
    get_ce_endpoint,
)
from law.contrib.wlcg.target import WLCGFileSystem, WLCGTarget, WLCGFileTarget, WLCGDirectoryTarget
from law.contrib.wlcg.decorator import ensure_vomsproxy
