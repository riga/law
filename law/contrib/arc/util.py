# coding: utf-8

"""
Helpers for working with ARC.
"""

from __future__ import annotations

__all__ = [
    "get_arcproxy_file", "get_arcproxy_user", "get_arcproxy_lifetime", "get_arcproxy_vo",
    "check_arcproxy_validity", "renew_arcproxy",
]

import os
import re
import pathlib
import subprocess

from law.util import interruptable_popen, tmp_file, parse_duration, quote_cmd, custom_context
from law.logger import get_logger


logger = get_logger(__name__)


def get_arcproxy_file() -> str:
    """
    Returns the path to the arc proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]

    # resolution order as in http://www.nordugrid.org/documents/arc-ce-sysadm-guide.pdf
    tmp = "/tmp"
    for v in ["TMPDIR", "TMP", "TEMP"]:
        if os.getenv(v):
            tmp = os.environ[v]
            break

    return os.path.join(tmp, f"x509up_u{os.getuid()}")


def _arcproxy_info(
    args: list[str] | None = None,
    proxy_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> tuple[int, str, str]:
    if args is None:
        args = ["--info"]
    cmd = ["arcproxy"] + (args or [])

    # when proxy_file is None, get the default
    # when empty string, don't add a --proxy argument
    if proxy_file is None:
        proxy_file = get_arcproxy_file()
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(str(proxy_file)))
        cmd.extend(["--proxy", proxy_file])

    out: str
    err: str
    code, out, err = interruptable_popen(  # type: ignore[assignment]
        quote_cmd(cmd),
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # arcproxy does not use proper exit codes but writes into stderr in case of an error
    if err:
        code = 1

    if not silent and code != 0:
        raise Exception(f"arcproxy failed: {err}")

    return code, out, err


def get_arcproxy_user(proxy_file: str | pathlib.Path | None = None) -> str:
    """
    Returns the owner of the arc proxy. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_arcproxy_file`. Otherwise, when it evaluates to *False*, ``arcproxy`` is queried
    without a custom proxy file.
    """
    out = _arcproxy_info(args=["--infoitem=identity"], proxy_file=proxy_file)[1].strip()
    try:
        return re.match(r".*\/CN\=([^\/]+).*", out.strip()).group(1)  # type: ignore[union-attr]
    except:
        raise Exception(f"no valid identity found in arc proxy: {out}")


def get_arcproxy_lifetime(proxy_file: str | pathlib.Path | None = None) -> int:
    """
    Returns the remaining lifetime of the arc proxy in seconds. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_arcproxy_file`. Otherwise, when it evaluates to
    *False*, ``arcproxy`` is queried without a custom proxy file.
    """
    out = _arcproxy_info(args=["--infoitem=validityLeft"], proxy_file=proxy_file)[1].strip()
    try:
        return int(out)
    except:
        raise Exception(f"no valid lifetime found in arc proxy: {out}")


def get_arcproxy_vo(proxy_file: str | pathlib.Path | None = None) -> str:
    """
    Returns the virtual organization name of the arc proxy. When *proxy_file* is *None*, it defaults
    to the result of :py:func:`get_arcproxy_file`. Otherwise, when it evaluates to *False*,
    ``arcproxy`` is queried without a custom proxy file.
    """
    return _arcproxy_info(args=["--infoitem=vomsVO"], proxy_file=proxy_file)[1].strip()


def check_arcproxy_validity(log=False, proxy_file: str | pathlib.Path | None = None) -> bool:
    """
    Returns *True* when a valid arc proxy exists, *False* otherwise. When *log* is *True*, a
    warning will be logged. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_arcproxy_file`. Otherwise, when it evaluates to *False*, ``arcproxy`` is queried
    without a custom proxy file.
    """
    code, out, err = _arcproxy_info(proxy_file=proxy_file, silent=True)

    if code == 0:
        valid = get_arcproxy_lifetime(proxy_file=proxy_file) > 0
    elif err.strip().lower().startswith("error: cannot find file at"):
        valid = False
    else:
        raise Exception(f"arcproxy failed: {err}")

    if log and not valid:
        logger.warning("no valid arc proxy found")

    return valid


def renew_arcproxy(
    password: str | pathlib.Path = "",
    lifetime="8 days",
    proxy_file: str | pathlib.Path | None = None,
) -> None:
    """
    Renews the arc proxy using a password *password* and a default *lifetime* of 8 days, which is
    internally parsed by :py:func:`law.util.parse_duration` where the default input unit is hours.
    To ensure that the *password*, in case it is not passed as a file, is not visible in any process
    listing, it is written to a temporary file first and piped into the ``arcproxy`` command.

    When *proxy_file* is *None*, it defaults to the result of :py:func:`get_arcproxy_file`.
    Otherwise, when it evaluates to *False*, ``arcproxy`` is invoked without a custom proxy file.
    """
    # convert the lifetime to seconds
    lifetime_seconds = int(parse_duration(lifetime, input_unit="h", unit="s"))

    if proxy_file is None:
        proxy_file = get_arcproxy_file()

    args = f"--constraint=validityPeriod={lifetime_seconds}"
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(str(proxy_file)))
        args += f" --proxy={proxy_file}"

    password = str(password)
    password_file_given = os.path.exists(password)
    password_context = custom_context((None, password)) if password_file_given else tmp_file
    with password_context() as (_, password_file):  # type: ignore[operator]
        if not password_file_given:
            with open(password_file, "w") as f:
                f.write(password)

        cmd = f"arcproxy --passwordsource=key=file:{password_file} {args}"
        code, out, _ = interruptable_popen(
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        if code != 0:
            raise Exception(f"arcproxy failed: {out}")
