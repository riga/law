# coding: utf-8

"""
Slurm utilities.
"""

__all__ = ["get_slurm_version"]


import re
import subprocess
import threading

from law.util import no_value, interruptable_popen


_slurm_version = no_value
_slurm_version_lock = threading.Lock()


def get_slurm_version():
    """
    Returns the version of the Slurm installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations.
    """
    global _slurm_version

    if _slurm_version == no_value:
        version = None
        with _slurm_version_lock:
            code, out, _ = interruptable_popen("sbatch --version", shell=True,
                executable="/bin/bash", stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if code == 0:
                first_line = out.strip().split("\n")[0]
                m = re.match(r"^slurm (\d+)\.(\d+)\.(\d+).*$", first_line.strip())
                if m:
                    version = tuple(map(int, m.groups()))

            _slurm_version = version

    return _slurm_version
