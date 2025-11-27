# coding: utf-8

"""
Function returning the config defaults of the slurm package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "slurm_job_file_dir": None,
            "slurm_job_file_dir_mkdtemp": None,
            "slurm_job_file_dir_cleanup": False,
            "slurm_job_query_timeout": None,
            "slurm_chunk_size_cancel": 25,
            "slurm_chunk_size_query": 25,
        },
    }
