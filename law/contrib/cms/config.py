# coding: utf-8

"""
Function returning the config defaults of the cms package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "crab_job_file_dir": None,
            "crab_job_file_dir_cleanup": False,
            "crab_job_query_timeout": None,
            "crab_sandbox_name": "CMSSW_14_2_1::arch=el9_amd64_gcc12",
            "crab_password_file": None,
        },
        "cmssw_sandbox": {
            "stagein_dir_name": "stagein",
            "stageout_dir_name": "stageout",
            "law_executable": "law",
            "login": False,
        },
        "cmssw_sandbox_env": {},
    }
