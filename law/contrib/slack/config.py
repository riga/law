# coding: utf-8

"""
Function returning the config defaults of the slack package.
"""


def config_defaults(default_config):
    return {
        "notifications": {
            "slack_token": None,
            "slack_channel": None,
            "slack_mention_user": None,
        },
    }
