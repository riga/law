# coding: utf-8

"""
Function returning the config defaults of the gfal package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "logging": {
            "gfal2": "WARNING",
        },
    }
