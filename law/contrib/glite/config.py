# coding: utf-8

"""
Function returning the config defaults of the glite package.
"""


def config_defaults(default_config):
    return {
        "job": {
            "glite_job_file_dir": None,
            "glite_job_file_dir_mkdtemp": None,
            "glite_job_file_dir_cleanup": None,
            "glite_job_query_timeout": None,
            "glite_chunk_size_cancel": 25,
            "glite_chunk_size_cleanup": 25,
            "glite_chunk_size_query": 25,
            "glite_cmd_submit": "glite-ce-job-submit",
            "glite_cmd_cancel": "glite-ce-job-cancel",
            "glite_cmd_purge": "glite-ce-job-purge",
            "glite_cmd_status": "glite-ce-job-status",
        },
    }
