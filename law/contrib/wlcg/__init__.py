# coding: utf-8
# flake8: noqa

"""
Helpers and targets providing functionality to work with the Worldwide LHC Computing Grid.
"""


__all__ = [
    "get_voms_proxy_file", "get_voms_proxy_user", "get_voms_proxy_lifetime", "get_voms_proxy_vo",
    "check_voms_proxy_validity", "renew_voms_proxy", "delegate_voms_proxy_glite", "get_ce_endpoint",
    "WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget",
    "ensure_voms_proxy",
]


# provisioning imports
from law.contrib.wlcg.util import (
    get_voms_proxy_file, get_voms_proxy_user, get_voms_proxy_lifetime, get_voms_proxy_vo,
    check_voms_proxy_validity, renew_voms_proxy, delegate_voms_proxy_glite, get_ce_endpoint,
)
from law.contrib.wlcg.target import WLCGFileSystem, WLCGTarget, WLCGFileTarget, WLCGDirectoryTarget
from law.contrib.wlcg.decorator import ensure_voms_proxy
