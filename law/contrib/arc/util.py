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

from law.util import interruptable_popen, tmp_file, parse_duration, quote_cmd


logger = logging.getLogger(__name__)


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


def _arc_proxy_info(args=None, proxy_file=None, silent=False):
    if args is None:
        args = ["--info"]
    cmd = ["arcproxy"] + (args or [])

    # when proxy_file is None, get the default
    # when empty string, don't add a --proxy argument
    if proxy_file is None:
        proxy_file = get_arc_proxy_file()
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(proxy_file))
        cmd.extend(["--proxy", proxy_file])

    code, out, err = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # arcproxy does not use proper exit codes but writes into stderr in case of an error
    if err:
        code = 1

    if not silent and code != 0:
        raise Exception("arcproxy failed: {}".format(err))

    return code, out, err


def get_arc_proxy_user(proxy_file=None):
    """
    Returns the owner of the arc proxy. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_arc_proxy_file`. Otherwise, when it evaluates to *False*, ``arcproxy`` is queried
    without a custom proxy file.
    """
    out = _arc_proxy_info(args=["--infoitem=identity"], proxy_file=proxy_file)[1].strip()
    try:
        return re.match(r".*\/CN\=([^\/]+).*", out.strip()).group(1)
    except:
        raise Exception("no valid identity found in arc proxy: {}".format(out))


def get_arc_proxy_lifetime(proxy_file=None):
    """
    Returns the remaining lifetime of the arc proxy in seconds. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_arc_proxy_file`. Otherwise, when it evaluates to
    *False*, ``arcproxy`` is queried without a custom proxy file.
    """
    out = _arc_proxy_info(args=["--infoitem=validityLeft"], proxy_file=proxy_file)[1].strip()
    try:
        return int(out)
    except:
        raise Exception("no valid lifetime found in arc proxy: {}".format(out))


def get_arc_proxy_vo(proxy_file=None):
    """
    Returns the virtual organization name of the arc proxy. When *proxy_file* is *None*, it defaults
    to the result of :py:func:`get_arc_proxy_file`. Otherwise, when it evaluates to *False*,
    ``arcproxy`` is queried without a custom proxy file.
    """
    return _arc_proxy_info(args=["--infoitem=vomsVO"], proxy_file=proxy_file)[1].strip()


def check_arc_proxy_validity(log=False, proxy_file=None):
    """
    Returns *True* when a valid arc proxy exists, *False* otherwise. When *log* is *True*, a
    warning will be logged. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_arc_proxy_file`. Otherwise, when it evaluates to *False*, ``arcproxy`` is queried
    without a custom proxy file.
    """
    code, out, err = _arc_proxy_info(proxy_file=proxy_file, silent=True)

    if code == 0:
        valid = get_arc_proxy_lifetime(proxy_file=proxy_file) > 0
    elif err.strip().lower().startswith("error: cannot find file at"):
        valid = False
    else:
        raise Exception("arcproxy failed: {}".format(err))

    if log and not valid:
        logger.warning("no valid arc proxy found")

    return valid


def renew_arc_proxy(password="", lifetime="8 days", proxy_file=None):
    """
    Renews the arc proxy using a password *password* and a default *lifetime* of 8 days, which is
    internally parsed by :py:func:`law.util.parse_duration` where the default input unit is hours.
    To ensure that the *password* it is not visible in any process listing, it is written to a
    temporary file first and piped into the ``arcproxy`` command. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_arc_proxy_file`. Otherwise, when it evaluates to
    *False*, ``arcproxy`` is invoked without a custom proxy file.
    """
    # convert the lifetime to seconds
    lifetime_seconds = int(parse_duration(lifetime, input_unit="h", unit="s"))

    if proxy_file is None:
        proxy_file = get_arc_proxy_file()

    args = "--constraint=validityPeriod={}".format(lifetime_seconds)
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(proxy_file))
        args += " --proxy={}".format(proxy_file)

    with tmp_file() as (_, tmp):
        with open(tmp, "w") as f:
            f.write(password)

        cmd = "arcproxy --passwordsource=key=file:{} {}".format(tmp, args)
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if code != 0:
            raise Exception("arcproxy failed: {}".format(out))
