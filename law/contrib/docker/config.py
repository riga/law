# coding: utf-8

"""
Function returning the config defaults of the docker package.
"""


def config_defaults(default_config):
    return {
        "docker_sandbox": {
            "stagein_dir_name": "stagein",
            "stageout_dir_name": "stageout",
            "law_executable": "law",
            "uid": None,
            "gid": None,
            "forward_dir": "/law_forward",
            "python_dir": "py",
            "bin_dir": "bin",
        },
        "docker_sandbox_env": {},
        "docker_sandbox_volumes": {},
    }
