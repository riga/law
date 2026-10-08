"""
Helpers for tests of job file factories and wrapper scripts.
"""

from __future__ import annotations

__all__ = ["write_executable"]

import os
import stat


def write_executable(path: str, content: str) -> str:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
    return path
