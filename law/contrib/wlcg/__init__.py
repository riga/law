# coding: utf-8
# flake8: noqa

"""
Helpers and targets providing functionality to work with the Worldwide LHC Computing Grid.
"""


__all__ = [
    "WLCGFileSystem", "WLCGTarget", "WLCGFileTarget", "WLCGDirectoryTarget",
    "get_voms_proxy_file", "get_voms_proxy_user", "get_voms_proxy_lifetime", "get_voms_proxy_vo",
    "check_voms_proxy_validity", "renew_voms_proxy", "delegate_voms_proxy_glite", "get_ce_endpoint",
]


# provisioning imports
from law.contrib.wlcg.target import WLCGFileSystem, WLCGTarget, WLCGFileTarget, WLCGDirectoryTarget
from law.contrib.wlcg.util import (
    get_voms_proxy_file, get_voms_proxy_user, get_voms_proxy_lifetime, get_voms_proxy_vo,
    check_voms_proxy_validity, renew_voms_proxy, delegate_voms_proxy_glite, get_ce_endpoint,
)
