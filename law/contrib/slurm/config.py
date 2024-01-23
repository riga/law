# coding: utf-8

"""
Function returning the config defaults of the slurm package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "slurm_job_file_dir": None,
            "slurm_job_file_dir_mkdtemp": None,
            "slurm_job_file_dir_cleanup": False,
            "slurm_chunk_size_cancel": 25,
            "slurm_chunk_size_query": 25,
        },
    }
