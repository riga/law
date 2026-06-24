# coding: utf-8

"""
Function returning the config defaults of the hdfs package.
"""


def config_defaults(default_config):
    return {
        "logging": {
            "hdfs": "WARNING",
        },
    }
