# coding: utf-8

"""
Function returning the config defaults of the htcondor package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "htcondor_job_file_dir": None,
            "htcondor_job_file_dir_mkdtemp": None,
            "htcondor_job_file_dir_cleanup": False,
            "htcondor_chunk_size_submit": 25,
            "htcondor_chunk_size_cancel": 25,
            "htcondor_chunk_size_query": 25,
            "htcondor_merge_job_files": True,
        },
    }
