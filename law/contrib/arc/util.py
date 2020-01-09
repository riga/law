# coding: utf-8

"""
Helpers for working with ARC.
"""


__all__ = [
    "get_arc_proxy_file", "get_arc_proxy_user", "get_arc_proxy_lifetime", "get_arc_proxy_vo",
    "check_arc_proxy_validity", "renew_arc_proxy",
]


import os
import re
import subprocess
import logging

from law.util import interruptable_popen, tmp_file, parse_duration


logger = logging.getLogger(__name__)


def _arc_proxy_info(args=None, silent=False):
    if args is None:
        args = ["--info"]
    cmd = ["arcproxy"] + (args or [])
    code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if not silent and code != 0:
        raise Exception("arcproxy failed: {}".format(err))
    return code, out, err


def get_arc_proxy_file():
    """
    Returns the path to the arc proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]
    else:
        # resolution order as in http://www.nordugrid.org/documents/arc-ce-sysadm-guide.pdf
        tmp = "/tmp"
        for v in ["TMPDIR", "TMP", "TEMP"]:
            if os.getenv(v):
                tmp = os.environ[v]
                break
        return os.path.join(tmp, "x509up_u{}".format(os.getuid()))


def get_arc_proxy_user():
    """
    Returns the owner of the arc proxy.
    """
    out = _arc_proxy_info(["--infoitem=identity"])[1].strip()
    try:
        return re.match(r".*\/CN\=([^\/]+).*", out.strip()).group(1)
    except:
        raise Exception("no valid identity found in arc proxy: {}".format(out))


def get_arc_proxy_lifetime():
    """
    Returns the remaining lifetime of the arc proxy in seconds.
    """
    out = _arc_proxy_info(["--infoitem=validityLeft"])[1].strip()
    try:
        return int(out)
    except:
        raise Exception("no valid lifetime found in arc proxy: {}".format(out))


def get_arc_proxy_vo():
    """
    Returns the virtual organization name of the arc proxy.
    """
    return _arc_proxy_info(["--infoitem=vomsVO"])[1].strip()


def check_arc_proxy_validity(log=False):
    """
    Returns *True* when a valid arc proxy exists, *False* otherwise. When *log* is *True*, a
    warning will be logged.
    """
    valid = _arc_proxy_info(silent=True)[0] == 0
    if log and not valid:
        logger.warning("no valid arc proxy found")
    return valid


def renew_arc_proxy(password="", lifetime="8 days"):
    """
    Renews the arc proxy using a password *password* and a default *lifetime* of 8 days, which is
    internally parsed by :py:func:`law.util.parse_duration` where the default input unit is hours.
    To ensure that the *password* it is not visible in any process listing, it is written to a
    temporary file first and piped into the ``arcproxy`` command using the ``expect`` executable
    which must be installed on the system. This is required as ``arcproxy`` does not read input
    from stdin but rather from raw input.
    """
    # convert the lifetime to seconds
    lifetime_seconds = int(max(parse_duration(lifetime, input_unit="h", unit="s"), 60))

    with tmp_file() as (_, tmp):
        with open(tmp, "w") as f:
            f.write(password)

        cmd = r"""expect -c '
            set timeout -1; \
            spawn arcproxy --constraint=validityPeriod={}; \
            expect "Enter pass phrase for private key:"; \
            send [exec cat {}]\r; \
            lassign [wait] pid spawnid flag code; \
            expect eof; \
            exit $code'""".format(lifetime_seconds, tmp)
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if code != 0:
            raise Exception("arcproxy failed: {}".format(out))
