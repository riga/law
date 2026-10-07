"""
Helpers for tests of job file factories and wrapper scripts.
"""

from __future__ import annotations

__all__ = ["has_bash4", "write_executable"]

import os
import shutil
import stat
import subprocess


def has_bash4() -> bool:
    bash = shutil.which("bash")
    if not bash:
        return False
    out = subprocess.run([bash, "-c", "echo ${BASH_VERSINFO[0]}"], capture_output=True, text=True, check=False).stdout
    return out.strip().isdigit() and int(out.strip()) >= 4


def write_executable(path: str, content: str) -> str:
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    os.chmod(path, os.stat(path).st_mode | stat.S_IXUSR)
    return path
