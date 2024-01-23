# coding: utf-8

"""
Function returning the config defaults of the telegram package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "notifications": {
            "telegram_token": None,
            "telegram_chat": None,
            "telegram_mention_user": None,
        },
    }
