# coding: utf-8

"""
Function returning the config defaults of the glite package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "glite_job_file_dir": None,
            "glite_job_file_dir_mkdtemp": None,
            "glite_job_file_dir_cleanup": None,
            "glite_job_query_timeout": None,
            "glite_chunk_size_cancel": 25,
            "glite_chunk_size_cleanup": 25,
            "glite_chunk_size_query": 25,
        },
    }
