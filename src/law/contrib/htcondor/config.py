"""
Function returning the config defaults of the htcondor package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    return {
        "job": {
            "htcondor_job_grouping_submit": True,
            "htcondor_job_file_dir": None,
            "htcondor_job_file_dir_mkdtemp": None,
            "htcondor_job_file_dir_cleanup": False,
            "htcondor_job_query_timeout": None,
            "htcondor_chunk_size_submit": 25,
            "htcondor_chunk_size_cancel": 25,
            "htcondor_chunk_size_query": 25,
            "htcondor_merge_job_files": True,
            "htcondor_cmd_q": "condor_q",
            "htcondor_cmd_history": "condor_history",
            "htcondor_cmd_submit": "condor_submit",
            "htcondor_cmd_rm": "condor_rm",
        },
    }
