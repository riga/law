# coding: utf-8

"""
Function returning the config defaults of the slack package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "notifications": {
            "slack_token": None,
            "slack_channel": None,
            "slack_mention_user": None,
        },
    }
