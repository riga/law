"""
Function returning the config defaults of the hdfs package.
"""

from __future__ import annotations

from law._types import Any


def config_defaults(default_config: dict) -> dict[str, dict[str, Any]]:
    defaults: dict[str, dict[str, Any]] = {
        "target": {
            "default_hdfs_fs": "hdfs_fs",
        },
        "hdfs_fs": {
            # defined by FileSystem
            "has_permissions": False,
            "default_file_perm": None,
            "default_dir_perm": None,
            "create_file_dir": False,  # parent directories are created by filecopy
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
            # defined by HDFSFileInterface
            "hdfs_user": None,
            "hdfs_kerb_ticket": None,
        },
    }

    defaults["_hdfs_fs_defaults"] = defaults["hdfs_fs"].copy()

    return defaults
