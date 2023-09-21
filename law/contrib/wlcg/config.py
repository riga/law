# coding: utf-8

"""
Function returning the config defaults of the wlcg package.
"""


def config_defaults(default_config):
    return {
        "target": {
            "default_wlcg_fs": "wlcg_fs",
        },
        "wlcg_fs": {
            # defined by FileSystem
            "has_permissions": False,
            "default_file_perm": None,
            "default_dir_perm": None,
            "create_file_dir": False,  # requires gfal_transfer_create_parent to be True
            # defined by RemoteFileInterface
            "base": None,
            "base_stat": None,
            "base_exists": None,
            "base_chmod": None,
            "base_unlink": None,
            "base_rmdir": None,
            "base_mkdir": None,
            "base_listdir": None,
            "base_filecopy": None,
            "retries": 1,
            "retry_delay": "5s",
            "random_base": True,
            # defined by RemoteFileSystem
            "validate_copy": False,
            "use_cache": False,
            # define by RemoteCache
            "cache_root": None,
            "cache_cleanup": None,
            "cache_max_size": "0MB",
            "cache_mtime_patience": 1.0,
            "cache_file_perm": 0o0660,
            "cache_dir_perm": 0o0770,
            "cache_wait_delay": "5s",
            "cache_max_waits": 120,
            "cache_global_lock": False,
            # defined by GFALFileInterface
            "gfal_atomic_contexts": False,
            "gfal_transfer_timeout": 3600,
            "gfal_transfer_checksum_check": False,
            "gfal_transfer_nbstreams": 1,
            "gfal_transfer_overwrite": True,
            "gfal_transfer_create_parent": True,
            "gfal_transfer_strict_copy": False,
            # defined by WLCGFileSystem
            # no dedicated configs
        },
    }
