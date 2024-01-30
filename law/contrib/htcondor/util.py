# coding: utf-8

"""
HTCondor utilities.
"""

from __future__ import annotations

__all__ = ["get_htcondor_version"]

import re
import subprocess
import threading

from law.util import NoValue, no_value, interruptable_popen


_htcondor_version: tuple[int, int, int] | None | NoValue = no_value
_htcondor_version_lock = threading.Lock()


def get_htcondor_version() -> tuple[int, int, int] | None:
    """
    Returns the version of the HTCondor installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations. When the ``condor_version`` executable is not available, *None*
    is returned.
    """
    global _htcondor_version

    if _htcondor_version == no_value:
        version = None
        with _htcondor_version_lock:
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                "condor_version",
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )

            if code == 0:
                first_line = out.strip().split("\n")[0]
                m = re.match(r"^\$CondorVersion: (\d+)\.(\d+)\.(\d+) .+$", first_line.strip())
                if m:
                    version = tuple(map(int, m.groups()))

            _htcondor_version = version  # type: ignore[assignment]

    return _htcondor_version  # type: ignore[return-value]
