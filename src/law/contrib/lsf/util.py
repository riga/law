# coding: utf-8

"""
LSF utilities.
"""

from __future__ import annotations

__all__ = ["get_lsf_version"]

import re
import subprocess
import threading

from law.util import NoValue, no_value, interruptable_popen


_lsf_version: tuple[int, int, int] | None | NoValue = no_value
_lsf_version_lock = threading.Lock()


def get_lsf_version() -> tuple[int, int, int] | None:
    """
    Returns the version of the LSF installation in a 3-tuple. The value is cached to accelerate
    repeated function invocations. When the ``bjobs`` executable is not available, *None* is
    returned.
    """
    global _lsf_version

    if _lsf_version == no_value:
        version = None
        with _lsf_version_lock:
            out: str
            code, out, _ = interruptable_popen(  # type: ignore[assignment]
                "bjobs -V",
                shell=True,
                executable="/bin/bash",
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
            )
            if code == 0:
                first_line = out.strip().split("\n")[0].strip()
                m = re.match(r"^Platform LSF (\d+)\.(\d+)\.(\d+).+$", first_line)
                if m:
                    version = tuple(map(int, m.groups()))

            _lsf_version = version  # type: ignore[assignment]

    return _lsf_version  # type: ignore[return-value]
