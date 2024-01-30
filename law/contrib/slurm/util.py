# coding: utf-8

"""
Slurm utilities.
"""

from __future__ import annotations

__all__ = ["get_slurm_version"]

import re
import subprocess
import threading

from law.util import NoValue, no_value, interruptable_popen


_slurm_version: tuple[int, int, int] | None | NoValue = no_value
_slurm_version_lock = threading.Lock()


def get_slurm_version() -> tuple[int, int, int] | None:
    """
    Returns the version of the Slurm installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations. When the ``sbatch`` executable is not available, *None* is
    returned.
    """
    global _slurm_version

    if _slurm_version == no_value:
        version = None
        with _slurm_version_lock:
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                "sbatch --version",
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            if code == 0:
                first_line = out.strip().split("\n")[0]
                m = re.match(r"^slurm (\d+)\.(\d+)\.(\d+).*$", first_line.strip())
                if m:
                    version = tuple(map(int, m.groups()))

            _slurm_version = version  # type: ignore[assignment]

    return _slurm_version  # type: ignore[return-value]
