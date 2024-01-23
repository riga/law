# coding: utf-8

"""
Function returning the config defaults of the arc package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "arc_job_file_dir": None,
            "arc_job_file_dir_mkdtemp": None,
            "arc_job_file_dir_cleanup": None,
            "arc_chunk_size_submit": 25,
            "arc_chunk_size_cancel": 25,
            "arc_chunk_size_cleanup": 25,
            "arc_chunk_size_query": 20,
        },
    }
