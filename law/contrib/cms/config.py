# coding: utf-8

"""
Function returning the config defaults of the cms package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "crab_job_file_dir": None,
            "crab_job_file_dir_cleanup": False,
            "crab_sandbox_name": "CMSSW_10_6_30",
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
