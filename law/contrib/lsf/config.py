# coding: utf-8

"""
Function returning the config defaults of the lsf package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "lsf_job_file_dir": None,
            "lsf_job_file_dir_mkdtemp": None,
            "lsf_job_file_dir_cleanup": False,
            "lsf_chunk_size_cancel": 25,
            "lsf_chunk_size_query": 25,
        },
    }
