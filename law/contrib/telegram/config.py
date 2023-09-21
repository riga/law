# coding: utf-8

"""
Function returning the config defaults of the telegram package.
"""


def config_defaults(default_config):
    return {
        "notifications": {
            "telegram_token": None,
            "telegram_chat": None,
            "telegram_mention_user": None,
        },
    }
