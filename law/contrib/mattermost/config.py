# coding: utf-8

"""
Function returning the config defaults of the mattermost package.
"""


def config_defaults(default_config):
    return {
        "notifications": {
            "mattermost_hook_url": None,
            "mattermost_header": None,
            "mattermost_channel": None,
            "mattermost_user": None,
            "mattermost_mention_user": None,
            "mattermost_icon_url": "https://media.githubusercontent.com/media/riga/law/refs/heads/master/assets/logo_profile.png",  # noqa
            "mattermost_icon_emoji": None,
            "mattermost_success_emoji": ":tada:",
            "mattermost_failure_emoji": ":rotating_light:",
        },
    }
