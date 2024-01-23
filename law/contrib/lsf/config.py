# coding: utf-8

"""
Function returning the config defaults of the lsf package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "lsf_job_file_dir": None,
            "lsf_job_file_dir_mkdtemp": None,
            "lsf_job_file_dir_cleanup": False,
            "lsf_chunk_size_cancel": 25,
            "lsf_chunk_size_query": 25,
        },
    }
