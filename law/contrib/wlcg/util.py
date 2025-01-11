# coding: utf-8

"""
Helpers for working with the WLCG.
"""

__all__ = [
    "get_userkey", "get_usercert", "get_usercert_subject",
    "get_vomsproxy_file", "get_vomsproxy_identity", "get_vomsproxy_lifetime", "get_vomsproxy_vo",
    "check_vomsproxy_validity", "renew_vomsproxy", "delegate_vomsproxy_glite",
    "delegate_myproxy", "get_myproxy_info",
    "get_ce_endpoint",
]


import os
import re
import subprocess
import uuid
import json
import hashlib
import getpass
import functools

import six

from law.util import (
    interruptable_popen, create_hash, human_duration, parse_duration, quote_cmd,
)
from law.logger import get_logger


logger = get_logger(__name__)


def get_userkey():
    """
    Returns the expanded path to the globus user key, reading from "$X509_USER_KEY" and defaulting
    to "$HOME/.globus/userkey.pem".
    """
    path = os.getenv("X509_USER_KEY", "$HOME/.globus/userkey.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_usercert():
    """
    Returns the expanded path to the globus user certificate, reading from "$X509_USER_CERT" and
    defaulting to "$HOME/.globus/usercert.pem".
    """
    path = os.getenv("X509_USER_CERT", "$HOME/.globus/usercert.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_usercert_subject(usercert=None):
    """
    Returns the user "subject" string of the certificate at *usercert*, which defaults to the
    return value of :py:func:`get_usercert`.
    """
    # get the user certificate file
    if usercert is None:
        usercert = get_usercert()
    usercert = str(usercert)
    if not os.path.exists(usercert):
        raise Exception("usercert does not exist at '{}'".format(usercert))

    # extract the subject via openssl
    cmd = ["openssl", "x509", "-in", usercert, "-noout", "-subject"]
    code, out, err = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if code != 0:
        raise Exception("subject extraction from usercert failed: {}".format(err))

    return re.sub(r"^subject\s*=\s*", "", out.strip())


def get_vomsproxy_file():
    """
    Returns the path to the voms proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]

    return "/tmp/x509up_u{}".format(os.getuid())


def _vomsproxy_info(args=None, proxy_file=None, silent=False):
    cmd = ["voms-proxy-info"] + (args or [])

    # when proxy_file is None, get the default
    # when empty string, don't add a --file argument
    if proxy_file is None:
        proxy_file = get_vomsproxy_file()
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(str(proxy_file)))
        cmd.extend(["--file", proxy_file])

    code, out, err = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if not silent and code != 0:
        raise Exception("voms-proxy-info failed: {}".format(err))

    return code, out, err


def get_vomsproxy_identity(proxy_file=None, silent=False):
    """
    Returns the identity information of the voms proxy. When *proxy_file* is *None*, it defaults to
    the result of :py:func:`get_vomsproxy_file`. Otherwise, when it evaluates to *False*,
    ``voms-proxy-info`` is queried without a custom proxy file.
    """
    code, out, _ = _vomsproxy_info(args=["--identity"], proxy_file=proxy_file, silent=silent)
    return out.strip() if code == 0 else None


def get_vomsproxy_lifetime(proxy_file=None, silent=False):
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
        raise Exception("no valid lifetime found in voms proxy: {}".format(out))


def get_vomsproxy_vo(proxy_file=None, silent=False):
    """
    Returns the virtual organization name of the voms proxy. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_vomsproxy_file`. Otherwise, when it evaluates to
    *False*, ``voms-proxy-info`` is queried without a custom proxy file.
    """
    code, out, _ = _vomsproxy_info(args=["--vo"], proxy_file=proxy_file, silent=silent)
    return out.strip() if code == 0 else None


def check_vomsproxy_validity(return_rfc=False, proxy_file=None):
    """
    Returns *True* if a valid voms proxy exists (positive lifetime), and *False* otherwise. When
    *return_rfc* is *True*, The return value will be a 2-tuple, containing also whether the proxy
    is RFC3820 compliant.

    When *proxy_file* is *None*, it defaults to the result of :py:func:`get_vomsproxy_file`.
    Otherwise, when it evaluates to *False*, ``voms-proxy-info`` is ueried without a custom proxy
    file.
    """
    code, _, err = _vomsproxy_info(args=["--exists"], proxy_file=proxy_file, silent=True)

    rfc = False
    if code == 0:
        valid = get_vomsproxy_lifetime(proxy_file=proxy_file) > 0

        if valid and return_rfc:
            out = _vomsproxy_info(args=["--type"], proxy_file=proxy_file, silent=True)[1]
            rfc = out.strip().lower().startswith("rfc3820 compliant")

    elif err.strip().lower().startswith("proxy not found"):
        valid = False

    else:
        raise Exception("voms-proxy-info failed: {}".format(err))

    return (valid, rfc) if return_rfc else valid


def renew_vomsproxy(
    proxy_file=None,
    vo=None,
    rfc=True,
    lifetime="8 days",
    password=None,
    password_file=None,
    silent=False,
):
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
        cmd = "cat \"{}\" | {}".format(password_file, quote_cmd(cmd))
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe)[0]
    else:
        cmd = quote_cmd(cmd)
        stdin_callback = (lambda: password) if password else functools.partial(getpass.getpass, "")
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe, stdin=subprocess.PIPE, stdin_callback=stdin_callback,
            stdin_delay=0.2)[0]

    if code == 0:
        return proxy_file

    if silent:
        return None

    raise Exception("voms-proxy-init failed with code {}".format(code))


def delegate_vomsproxy_glite(endpoint, proxy_file=None, stdout=None, stderr=None, cache=True):
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
        raise Exception("proxy file '{}' does not exist".format(proxy_file))

    if cache:
        if isinstance(cache, six.string_types):
            cache_file = cache
        else:
            cache_file = proxy_file + "_delegation_cache.json"

        def remove_cache():
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
        raise Exception("glite proxy delegation to endpoint {} failed".format(endpoint))

    if cache:
        # write the id back to the delegation file
        cache_data["hash"] = proxy_hash
        cache_data.setdefault("ids", {})[endpoint] = delegation_id
        with open(cache_file, "w") as f:
            json.dump(cache_data, f, indent=4)
        os.chmod(cache_file, 0o0600)

    return delegation_id


def delegate_myproxy(
    endpoint="myproxy.cern.ch",
    userkey=None,
    usercert=None,
    username=None,
    proxy_file=None,
    encode_username=True,
    cred_lifetime=720,
    proxy_lifetime=168,
    retrievers=None,
    rfc=True,
    vo=None,
    create_local=False,
    password=None,
    password_file=None,
    silent=False,
):
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
        username = (
            get_vomsproxy_identity(proxy_file=proxy_file, silent=True) or
            get_usercert_subject()
        )
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
        cmd = "{}cat \"{}\" | {} -S".format(rfc_export, password_file, quote_cmd(cmd))
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe)[0]
    else:
        cmd = "{}{}".format(rfc_export, quote_cmd(cmd))
        stdin_callback = (lambda: password) if password else functools.partial(getpass.getpass, "")
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe, stdin=subprocess.PIPE, stdin_callback=stdin_callback,
            stdin_delay=0.2)[0]

    if code == 0:
        return username

    if silent:
        return None

    raise Exception("myproxy-init failed with code {}".format(code))


def get_myproxy_info(endpoint="myproxy.cern.ch", username=None, encode_username=True,
        proxy_file=None, silent=False):
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
        username = (
            get_vomsproxy_identity(proxy_file=proxy_file, silent=True) or
            get_usercert_subject()
        )
    if encode_username:
        username = hashlib.sha1(username.encode("utf-8")).hexdigest()

    # build and run the command
    cmd = ["myproxy-info", "-s", endpoint, "-l", username]
    code, out, _ = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE if silent else None)
    if code != 0:
        if silent:
            return
        raise Exception("myproxy-info failed with code {}".format(code))

    # parse the output
    info = {}
    for line in out.strip().replace("\r\n", "\n").split("\n"):
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


def get_ce_endpoint(ce):
    """
    Extracts the endpoint from a computing element *ce* and returns it. Example:

    .. code-block:: python

        get_ce_endpoint("grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms")
        # -> "grid-ce.physik.rwth-aachen.de:8443"
    """
    return ce.split("/", 1)[0]
