# coding: utf-8

"""
Helpers for working with the WLCG.
"""

from __future__ import annotations

__all__ = [
    "get_userkey", "get_usercert", "get_usercert_subject",
    "get_vomsproxy_file", "get_vomsproxy_identity", "get_vomsproxy_lifetime", "get_vomsproxy_vo",
    "check_vomsproxy_validity", "renew_vomsproxy", "delegate_vomsproxy_glite",
    "delegate_myproxy", "get_myproxy_info",
    "get_ce_endpoint",
]

import os
import re
import io
import subprocess
import uuid
import json
import hashlib
import pathlib
import getpass
import functools

from law.util import (
    interruptable_popen, create_hash, human_duration, parse_duration, quote_cmd,
)
from law.logger import get_logger


logger = get_logger(__name__)


def get_userkey() -> str:
    """
    Returns the expanded path to the globus user key, reading from "$X509_USER_KEY" and defaulting
    to "$HOME/.globus/userkey.pem".
    """
    path = os.getenv("X509_USER_KEY", "$HOME/.globus/userkey.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_usercert() -> str:
    """
    Returns the expanded path to the globus user certificate, reading from "$X509_USER_CERT" and
    defaulting to "$HOME/.globus/usercert.pem".
    """
    path = os.getenv("X509_USER_CERT", "$HOME/.globus/usercert.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_usercert_subject(usercert: str | pathlib.Path | None = None) -> str:
    """
    Returns the user "subject" string of the certificate at *usercert*, which defaults to the
    return value of :py:func:`get_usercert`.
    """
    # get the user certificate file
    if usercert is None:
        usercert = get_usercert()
    usercert = str(usercert)
    if not os.path.exists(usercert):
        raise Exception(f"usercert does not exist at '{usercert}'")

    # extract the subject via openssl
    cmd = ["openssl", "x509", "-in", usercert, "-noout", "-subject"]
    code, out, err = interruptable_popen(
        quote_cmd(cmd),
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if code != 0:
        raise Exception(f"subject extraction from usercert failed: {err}")

    return re.sub(r"^subject\s*=\s*", "", str(out).strip())


def get_vomsproxy_file() -> str:
    """
    Returns the path to the voms proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]

    return f"/tmp/x509up_u{os.getuid()}"


def _vomsproxy_info(
    args: list[str] | None = None,
    proxy_file: str | pathlib.Path | None = None,
    silent: bool = False,
):
    cmd = ["voms-proxy-info"] + (args or [])

    # when proxy_file is None, get the default
    # when empty string, don't add a --file argument
    if proxy_file is None:
        proxy_file = get_vomsproxy_file()
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(str(proxy_file)))
        cmd.extend(["--file", proxy_file])

    code, out, err = interruptable_popen(
        quote_cmd(cmd),
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    if not silent and code != 0:
        raise Exception(f"voms-proxy-info failed: {err}")

    return code, out, err


def get_vomsproxy_identity(
    proxy_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> str | None:
    """
    Returns the identity information of the voms proxy. When *proxy_file* is *None*, it defaults to
    the result of :py:func:`get_vomsproxy_file`. Otherwise, when it evaluates to *False*,
    ``voms-proxy-info`` is queried without a custom proxy file.
    """
    code, out, _ = _vomsproxy_info(args=["--identity"], proxy_file=proxy_file, silent=silent)
    return out.strip() if code == 0 else None


def get_vomsproxy_lifetime(
    proxy_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> int | None:
    """
    Returns the remaining lifetime of the voms proxy in seconds. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_vomsproxy_file`. Otherwise, when it evaluates to
    *False*, ``voms-proxy-info`` is queried without a custom proxy file.
    """
    code, out, _ = _vomsproxy_info(args=["--timeleft"], proxy_file=proxy_file, silent=silent)

    if code != 0:
        return None

    try:
        return int(out)
    except:
        if silent:
            return None
        raise Exception(f"no valid lifetime found in voms proxy: {out}")


def get_vomsproxy_vo(
    proxy_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> str | None:
    """
    Returns the virtual organization name of the voms proxy. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_vomsproxy_file`. Otherwise, when it evaluates to
    *False*, ``voms-proxy-info`` is queried without a custom proxy file.
    """
    code, out, _ = _vomsproxy_info(args=["--vo"], proxy_file=proxy_file, silent=silent)
    return out.strip() if code == 0 else None


def check_vomsproxy_validity(
    return_rfc: bool = False,
    proxy_file: str | pathlib.Path | None = None,
) -> bool | tuple[bool, bool]:
    """
    Returns *True* if a valid voms proxy exists (positive lifetime), and *False* otherwise. When
    *return_rfc* is *True*, The return value will be a 2-tuple, containing also whether the proxy
    is RFC3820 compliant.

    When *proxy_file* is *None*, it defaults to the result of :py:func:`get_vomsproxy_file`.
    Otherwise, when it evaluates to *False*, ``voms-proxy-info`` is ueried without a custom proxy
    file.
    """
    if proxy_file is None:
        proxy_file = get_vomsproxy_file()

    code, _, err = _vomsproxy_info(args=["--exists"], proxy_file=proxy_file, silent=True)

    rfc = False
    if code == 0:
        valid = get_vomsproxy_lifetime(proxy_file=proxy_file) > 0  # type: ignore[operator]

        if valid and return_rfc:
            out = _vomsproxy_info(args=["--type"], proxy_file=proxy_file, silent=True)[1]
            rfc = out.strip().lower().startswith("rfc3820 compliant")

    elif err.strip().lower().startswith("proxy not found"):
        valid = False

    else:
        raise Exception(f"voms-proxy-info failed: {err}")

    return (valid, rfc) if return_rfc else valid


def renew_vomsproxy(
    proxy_file: str | pathlib.Path | None = None,
    vo: str | None = None,
    rfc: bool = True,
    lifetime: str | float = "8 days",
    password: str | None = None,
    password_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> str | None:
    """
    Renews a voms proxy at *proxy_file* using an optional virtual organization name *vo*, and a
    default *lifetime* of 8 days, which is internally parsed by :py:func:`law.util.parse_duration`
    where the default input unit is hours. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_vomsproxy_file`.

    When *rfc* is *True*, the created proxy will be RFC compliant.

    By default, this function will prompt for input to securely receive the password. However, if
    *password* or *password_file* is given, the password is extracted from that variable or file
    instead.
    """
    # parse and format the lifetime
    lifetime_seconds = max(parse_duration(lifetime, input_unit="h", unit="s"), 60.0)
    lifetime = human_duration(seconds=lifetime_seconds, colon_format="h")
    # cut the seconds part
    normalized = ":".join((2 - lifetime.count(":")) * ["00"] + [""]) + lifetime
    lifetime = ":".join(normalized.rsplit(":", 3)[-3:-1])

    # when proxy_file is None, get the default
    if proxy_file is None:
        proxy_file = get_vomsproxy_file()
    proxy_file = str(proxy_file)

    # build the command
    cmd = [
        "voms-proxy-init",
        "--valid", lifetime,
        "--out", proxy_file,
    ]
    if vo:
        cmd.extend(["-voms", vo])
    if rfc:
        cmd.append("--rfc")

    # run it, depending on whether a password file is given
    silent_pipe = subprocess.PIPE if silent else None
    if password_file:
        password_file = os.path.expandvars(os.path.expanduser(str(password_file)))
        cmd_str = f"cat \"{password_file}\" | {quote_cmd(cmd)}"
        code = interruptable_popen(
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=silent_pipe,
            stderr=silent_pipe,
        )[0]
    else:
        stdin_callback = (lambda: password) if password else functools.partial(getpass.getpass, "")
        cmd_str = quote_cmd(cmd)
        code = interruptable_popen(
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=silent_pipe,
            stderr=silent_pipe,
            stdin=subprocess.PIPE,
            stdin_callback=stdin_callback,  # type: ignore[arg-type]
            stdin_delay=0.2,
        )[0]

    if code == 0:
        return proxy_file

    if silent:
        return None

    raise Exception(f"voms-proxy-init failed with code {code}")


def delegate_vomsproxy_glite(
    endpoint: str,
    proxy_file: str | pathlib.Path | None = None,
    stdout: int | io.TextIO | None = None,
    stderr: int | io.TextIO | None = None,
    cache: bool | str | pathlib.Path = True,
):
    """
    Delegates the voms proxy via gLite to an *endpoint*, e.g.
    ``grid-ce.physik.rwth-aachen.de:8443``. When *proxy_file* is *None*, it defaults to the result
    of :py:func:`get_vomsproxy_file`. *stdout* and *stderr* are passed to the *Popen* constructor
    for executing the ``glite-ce-delegate-proxy`` command. When *cache* is *True*, a json file is
    created alongside the proxy file, which stores the delegation ids per endpoint. The next time
    the exact same proxy should be delegated to the same endpoint, the cached delegation id is
    returned.
    """
    # get the proxy file
    if not proxy_file:
        proxy_file = get_vomsproxy_file()
    proxy_file = os.path.expandvars(os.path.expanduser(str(proxy_file)))
    if not os.path.exists(proxy_file):
        raise Exception(f"proxy file '{proxy_file}' does not exist")

    if cache:
        cache_file = f"{proxy_file}_delegation_cache.json" if isinstance(cache, bool) else str(cache)

        def remove_cache() -> None:
            try:
                if os.path.exists(cache_file):
                    os.remove(cache_file)
            except OSError:
                pass

        # create the hash of the proxy file content
        with open(proxy_file, "r") as f:
            proxy_hash = create_hash(f.read())

        # already delegated?
        cache_data = {}
        if os.path.exists(cache_file):
            with open(cache_file, "r") as f:
                try:
                    cache_data = json.load(f)
                except:
                    remove_cache()

        # is the hash up-to-date?
        if cache_data.get("hash") != proxy_hash:
            remove_cache()
            cache_data = {}

        # proxy already delegated to that endpoint?
        elif endpoint in cache_data.get("ids", []):
            return str(cache_data["ids"][endpoint])

    # do the actual delegation
    delegation_id = uuid.uuid4().hex
    cmd = ["glite-ce-delegate-proxy", "-e", endpoint, delegation_id]
    code = interruptable_popen(cmd, stdout=stdout, stderr=stderr)[0]
    if code != 0:
        raise Exception(f"glite proxy delegation to endpoint {endpoint} failed")

    if cache:
        # write the id back to the delegation file
        cache_data["hash"] = proxy_hash
        cache_data.setdefault("ids", {})[endpoint] = delegation_id
        with open(cache_file, "w") as f:
            json.dump(cache_data, f, indent=4)
        os.chmod(cache_file, 0o0600)

    return delegation_id


def delegate_myproxy(
    endpoint: str = "myproxy.cern.ch",
    userkey: str | pathlib.Path | None = None,
    usercert: str | pathlib.Path | None = None,
    username: str | None = None,
    encode_username: bool = True,
    cred_lifetime: int = 720,
    proxy_lifetime: int = 168,
    retrievers: str | None = None,
    rfc: bool = True,
    vo: str | None = None,
    create_local: bool = False,
    password: str | None = None,
    password_file: str | pathlib.Path | None = None,
    silent: bool = False,
) -> str | None:
    """
    Delegates an X509 proxy to a myproxy server *endpoint*.

    *userkey* and *usercert* default to the return values of :py:func:`get_userkey` and
    :py:func:`get_usercert`, respectively. When *username* is *None*, the identity field of an
    existing voms proxy is used, or the subject string of the certificate otherwise. If
    *encode_username* is set, *username* is the sha1 encoded.

    The credential and proxy lifetimes can be defined in hours by *cred_lifetime* and
    *proxy_lifetime*. When *retrievers* is given, it is passed as both ``--renewable_by`` and
    ``--retrievable_by_cert`` to the underlying ``myproxy-init`` command.

    When *rfc* is *True*, the delegated proxy will be RFC compliant. To pass VOMS attributes to the
    ``myproxy-init`` command, *vo* can be defined. When *create_local* is *True*, the delegation
    also creates a local proxy file (usually at $X509_USER_PROXY).

    If no *password* is given, the user is prompted for the password of the user certificate.
    However, if a *password_file* is present, the password is extracted from this file.

    The user name is returned upon success. Otherwise an exception is raised unless *silent* is
    *True* in which case *None* is returned.
    """
    # prepare arguments
    if not userkey:
        userkey = get_userkey()
    userkey = str(userkey)
    if not usercert:
        usercert = get_usercert()
    usercert = str(usercert)
    if not username:
        username = get_vomsproxy_identity(silent=True) or get_usercert_subject()
    if encode_username:
        username = hashlib.sha1(username.encode("utf-8")).hexdigest()

    # build the command
    cmd = [
        "myproxy-init",
        "-s", endpoint,
        "-y", userkey,
        "-C", usercert,
        "-l", username,
        "-t", str(proxy_lifetime),
        "-c", str(cred_lifetime),
    ]
    if retrievers:
        cmd.extend([
            "-x", "-R", retrievers,
            "-x", "-Z", retrievers,
        ])
    if vo:
        cmd.extend(["-m", vo])
    if create_local:
        cmd.append("-L")
    rfc_export = "GT_PROXY_MODE=rfc " if rfc else ""

    # run it, depending on whether a password file is given
    silent_pipe = subprocess.PIPE if silent else None
    if password_file:
        password_file = os.path.expandvars(os.path.expanduser(str(password_file)))
        cmd_str = f"{rfc_export}cat \"{password_file}\" | {quote_cmd(cmd)} -S"
        code = interruptable_popen(
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=silent_pipe,
            stderr=silent_pipe,
        )[0]
    else:
        stdin_callback = (lambda: password) if password else functools.partial(getpass.getpass, "")
        cmd_str = f"{rfc_export}{quote_cmd(cmd)}"
        code = interruptable_popen(
            cmd_str,
            shell=True,
            executable="/bin/bash",
            stdout=silent_pipe,
            stderr=silent_pipe,
            stdin=subprocess.PIPE,
            stdin_callback=stdin_callback,  # type: ignore[arg-type]
            stdin_delay=0.2,
        )[0]

    if code == 0:
        return username

    if silent:
        return None

    raise Exception(f"myproxy-init failed with code {code}")


def get_myproxy_info(
    endpoint: str = "myproxy.cern.ch",
    username: str | None = None,
    encode_username: bool = True,
    silent: bool = False,
) -> dict[str, str | int] | None:
    """
    Returns information about a previous myproxy delegation to a server *endpoint*. When *username*
    is *None*, the subject string of the certificate is used instead, and sha1 encoded if
    *encode_username* is *True*.

    The returned dictionary contains the fields ``username``, ``subject`` and ``timeleft``.

    An exception is raised if the underlying ``myproxy-info`` command fails, unless *silent* is
    *True* in which case *None* is returned.
    """
    # prepare arguments
    if not username:
        username = get_vomsproxy_identity(silent=True) or get_usercert_subject()
    if encode_username:
        username = hashlib.sha1(username.encode("utf-8")).hexdigest()

    # build and run the command
    cmd = ["myproxy-info", "-s", endpoint, "-l", username]
    code, out, _ = interruptable_popen(
        quote_cmd(cmd),
        shell=True,
        executable="/bin/bash",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if silent else None,
    )
    if code != 0:
        if silent:
            return None
        raise Exception(f"myproxy-info failed with code {code}")

    # parse the output
    info: dict[str, str | int] = {}
    for line in str(out).strip().replace("\r\n", "\n").split("\n"):
        line = line.strip()
        if line.startswith("username: "):
            info["username"] = line[len("username: "):]
        elif line.startswith("owner: "):
            info["subject"] = line[len("owner: "):]
        elif line.startswith("timeleft: "):
            m = re.match(r"^timeleft:\s+(\d+):(\d+):(\d+).*$", line)
            if m:
                times = list(map(int, m.groups()))
                info["timeleft"] = times[0] * 3600 + times[1] * 60 + times[2]

    return info


def get_ce_endpoint(ce: str) -> str:
    """
    Extracts the endpoint from a computing element *ce* and returns it. Example:

    .. code-block:: python

        get_ce_endpoint("grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms")
        # -> "grid-ce.physik.rwth-aachen.de:8443"
    """
    return ce.split("/", 1)[0]
