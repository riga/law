# coding: utf-8

"""
Function returning the config defaults of the arc package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "arc_job_file_dir": None,
            "arc_job_file_dir_mkdtemp": None,
            "arc_job_file_dir_cleanup": None,
            "arc_job_query_timeout": None,
            "arc_chunk_size_submit": 25,
            "arc_chunk_size_cancel": 25,
            "arc_chunk_size_cleanup": 25,
            "arc_chunk_size_query": 20,
        },
    }
