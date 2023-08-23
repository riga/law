# coding: utf-8

"""
Helpers for working with the WLCG.
"""

__all__ = [
    "get_user_key", "get_user_cert", "get_user_cert_subject",
    "get_voms_proxy_file", "get_voms_proxy_user", "get_voms_proxy_lifetime", "get_voms_proxy_vo",
    "check_voms_proxy_validity", "renew_voms_proxy", "delegate_voms_proxy_glite",
    "delegate_my_proxy", "get_my_proxy_info",
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
    interruptable_popen, tmp_file, create_hash, human_duration, parse_duration, quote_cmd,
)
from law.logger import get_logger


logger = get_logger(__name__)


def get_user_key():
    """
    Returns the expanded path to the globus user key, reading from "$X509_USER_KEY" and defaulting
    to "$HOME/.globus/userkey.pem".
    """
    path = os.getenv("X509_USER_KEY", "$HOME/.globus/userkey.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_user_cert():
    """
    Returns the expanded path to the globus user certificate, reading from "$X509_USER_CERT" and
    defaulting to "$HOME/.globus/usercert.pem".
    """
    path = os.getenv("X509_USER_CERT", "$HOME/.globus/usercert.pem")
    return os.path.expandvars(os.path.expanduser(path))


def get_user_cert_subject(user_cert=None):
    """
    Returns the user "subject" string of the certificate at *user_cert*, which defaults to the
    return value of :py:func:`get_user_cert`.
    """
    # get the user certificate file
    if user_cert is None:
        user_cert = get_user_cert()
    if not os.path.exists(user_cert):
        raise Exception("usercert does not exist at '{}'".format(user_cert))

    # extract the subject via openssl
    cmd = ["openssl", "x509", "-in", user_cert, "-noout", "-subject"]
    code, out, err = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if code != 0:
        raise Exception("subject extraction from usercert failed: {}".format(err))

    return re.sub(r"^subject\s*=\s*", "", out.strip())


def get_voms_proxy_file():
    """
    Returns the path to the voms proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]

    return "/tmp/x509up_u{}".format(os.getuid())


def _voms_proxy_info(args=None, proxy_file=None, silent=False):
    cmd = ["voms-proxy-info"] + (args or [])

    # when proxy_file is None, get the default
    # when empty string, don't add a --file argument
    if proxy_file is None:
        proxy_file = get_voms_proxy_file()
    if proxy_file:
        proxy_file = os.path.expandvars(os.path.expanduser(proxy_file))
        cmd.extend(["--file", proxy_file])

    code, out, err = interruptable_popen(quote_cmd(cmd), shell=True, executable="/bin/bash",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if not silent and code != 0:
        raise Exception("voms-proxy-info failed: {}".format(err))

    return code, out, err


def get_voms_proxy_user(proxy_file=None):
    """
    Returns the owner of the voms proxy. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_voms_proxy_file`. Otherwise, when it evaluates to *False*, ``voms-proxy-info`` is
    queried without a custom proxy file.
    """
    out = _voms_proxy_info(args=["--identity"], proxy_file=proxy_file)[1].strip()

    cns = re.findall(r"/CN=[a-z]+", out.replace(" ", ""))
    if not cns:
        raise Exception("no valid identity found in voms proxy: {}".format(out))

    # extract actual names
    names = [cn[4:] for cn in cns]

    # return the shortest name
    return sorted(names, key=len)[0]


def get_voms_proxy_lifetime(proxy_file=None):
    """
    Returns the remaining lifetime of the voms proxy in seconds. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_voms_proxy_file`. Otherwise, when it evaluates to
    *False*, ``voms-proxy-info`` is queried without a custom proxy file.
    """
    out = _voms_proxy_info(args=["--timeleft"], proxy_file=proxy_file)[1].strip()
    try:
        return int(out)
    except:
        raise Exception("no valid lifetime found in voms proxy: {}".format(out))


def get_voms_proxy_vo(proxy_file=None):
    """
    Returns the virtual organization name of the voms proxy. When *proxy_file* is *None*, it
    defaults to the result of :py:func:`get_voms_proxy_file`. Otherwise, when it evaluates to
    *False*, ``voms-proxy-info`` is queried without a custom proxy file.
    """
    return _voms_proxy_info(args=["--vo"], proxy_file=proxy_file)[1].strip()


def check_voms_proxy_validity(return_rfc=False, proxy_file=None):
    """
    Returns *True* if a valid voms proxy exists (positive lifetime), and *False* otherwise. When
    *return_rfc* is *True*, The return value will be a 2-tuple, containing also whether the proxy
    is RFC3820 compliant.

     When *proxy_file* is *None*, it defaults to the result of :py:func:`get_voms_proxy_file`.
     Otherwise, when it evaluates to *False*, ``voms-proxy-info`` is ueried without a custom proxy
     file.
    """
    code, _, err = _voms_proxy_info(args=["--exists"], proxy_file=proxy_file, silent=True)

    rfc = False
    if code == 0:
        valid = get_voms_proxy_lifetime(proxy_file=proxy_file) > 0

        if valid and return_rfc:
            out = _voms_proxy_info(args=["--type"], proxy_file=proxy_file, silent=True)[1]
            rfc = out.strip().lower().startswith("rfc3820 compliant")

    elif err.strip().lower().startswith("proxy not found"):
        valid = False

    else:
        raise Exception("voms-proxy-info failed: {}".format(err))

    return (valid, rfc) if return_rfc else valid


def renew_voms_proxy(password="", vo=None, lifetime="8 days", proxy_file=None):
    """
    Renews the voms proxy using a password *password*, an optional virtual organization name *vo*,
    and a default *lifetime* of 8 days, which is internally parsed by
    :py:func:`law.util.parse_duration` where the default input unit is hours. To ensure that the
    *password* is not visible in any process listing, it is written to a temporary file first and
    piped into the ``voms-proxy-init`` command. When *proxy_file* is *None*, it defaults to the
    result of :py:func:`get_voms_proxy_file`.
    """
    # parse and format the lifetime
    lifetime_seconds = max(parse_duration(lifetime, input_unit="h", unit="s"), 60.0)
    lifetime = human_duration(seconds=lifetime_seconds, colon_format="h")
    # cut the seconds part
    normalized = ":".join((2 - lifetime.count(":")) * ["00"] + [""]) + lifetime
    lifetime = ":".join(normalized.rsplit(":", 3)[-3:-1])

    # when proxy_file is None, get the default
    # when empty string, don't add a --out argument
    if proxy_file is None:
        proxy_file = get_voms_proxy_file()

    with tmp_file() as (_, tmp):
        with open(tmp, "w") as f:
            f.write(password)

        cmd = "cat '{}' | voms-proxy-init --valid '{}'".format(tmp, lifetime)
        if vo:
            cmd += " -voms '{}'".format(vo)
        if proxy_file:
            proxy_file = os.path.expandvars(os.path.expanduser(proxy_file))
            cmd += " --out '{}'".format(proxy_file)

        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        if code != 0:
            raise Exception("voms-proxy-init failed: {}".format(out))


def delegate_voms_proxy_glite(endpoint, proxy_file=None, stdout=None, stderr=None, cache=True):
    """
    Delegates the voms proxy via gLite to an *endpoint*, e.g.
    ``grid-ce.physik.rwth-aachen.de:8443``. When *proxy_file* is *None*, it defaults to the result
    of :py:func:`get_voms_proxy_file`. *stdout* and *stderr* are passed to the *Popen* constructor
    for executing the ``glite-ce-delegate-proxy`` command. When *cache* is *True*, a json file is
    created alongside the proxy file, which stores the delegation ids per endpoint. The next time
    the exact same proxy should be delegated to the same endpoint, the cached delegation id is
    returned.
    """
    # get the proxy file
    if not proxy_file:
        proxy_file = get_voms_proxy_file()
    proxy_file = os.path.expandvars(os.path.expanduser(proxy_file))
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


def delegate_my_proxy(
    endpoint="myproxy.cern.ch",
    user_key=None,
    user_cert=None,
    user_name=None,
    cred_lifetime=720,
    proxy_lifetime=168,
    retrievers=None,
    rfc=True,
    voms=None,
    create_local=True,
    password=None,
    password_file=None,
    silent=False,
):
    """
    Delegates an X509 proxy to a myproxy server *endpoint*.

    *user_key* and *user_cert* default to the return values of :py:func:`get_user_key` and
    :py:func:`get_user_cert`, respectively. When *user_name* is *None*, the sha1 encoded certificate
    subject string is used instead.

    The credential and proxy lifetimes can be defined in hours by *cred_lifetime* and
    *proxy_lifetime*. When *retrievers* is given, it is passed as both ``--renewable_by`` and
    ``--retrievable_by_cert`` to the underlying ``myproxy-init`` command.

    When *rfc* is *True*, the delegated proxy will be RFC compliant. To pass VOMS attributes to the
    ``myproxy-init`` command, *voms* can be defined. When *create_local* is *True*, the delegatio
    also creates a local proxy file (usually at $X509_USER_PROXY).

    If no *password* is given, the user is prompted for the password of the user certificate.
    However, if a *password_file* is present, the password is extracted from this file.

    The username is returned upon success. Otherwise an exception is raised unless *silent* is
    *True* in which case *None* is returned.
    """
    # prepare arguments
    if not user_key:
        user_key = get_user_key()
    if not user_cert:
        user_cert = get_user_cert()
    if not user_name:
        subject = get_user_cert_subject()
        user_name = hashlib.sha1(subject.encode("utf-8")).hexdigest()

    # build the command
    cmd = [
        "myproxy-init",
        "-s", endpoint,
        "-y", user_key,
        "-C", user_cert,
        "-l", user_name,
        "-t", str(proxy_lifetime),
        "-c", str(cred_lifetime),
    ]
    if retrievers:
        cmd.extend([
            "-x", "-R", retrievers,
            "-x", "-Z", retrievers,
        ])
    if voms:
        cmd.extend(["-m", voms])
    if create_local:
        cmd.append("-L")
    rfc_export = "GT_PROXY_MODE=rfc " if rfc else ""

    # run it, depending on whether a password file is given
    silent_pipe = subprocess.PIPE if silent else None
    if password_file:
        password_file = os.path.expandvars(os.path.expanduser(password_file))
        cmd = "{}cat \"{}\" | {} -S".format(rfc_export, password_file, quote_cmd(cmd))
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe)[0]
        if code != 0:
            if silent:
                return
            raise Exception("myproxy-init failed with code {}".format(code))
    else:
        cmd = "{}{}".format(rfc_export, quote_cmd(cmd))
        stdin_callback = (lambda: password) if password else functools.partial(getpass.getpass, "")
        code = interruptable_popen(cmd, shell=True, executable="/bin/bash", stdout=silent_pipe,
            stderr=silent_pipe, stdin=subprocess.PIPE, stdin_callback=stdin_callback,
            stdin_delay=0.2)[0]
        if code != 0:
            if silent:
                return
            raise Exception("myproxy-init failed with code {}".format(code))

    return user_name


def get_my_proxy_info(endpoint="myproxy.cern.ch", user_name=None, silent=False):
    """
    Returns information about a previous myproxy delegation to a server *endpoint*. When *user_name*
    is *None*, the sha1 encoded certificate subject string is used instead.

    The returned dictionary contains the fields ``user_name``, ``subject`` and ``time_left``.

    An exception is raised if the underlying ``myproxy-info`` command fails, unless *silent* is
    *True* in which case *None* is returned.
    """
    # prepare arguments
    if not user_name:
        subject = get_user_cert_subject()
        user_name = hashlib.sha1(subject.encode("utf-8")).hexdigest()

    # build and run the command
    cmd = ["myproxy-info", "-s", endpoint, "-l", user_name]
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
            info["user_name"] = line[len("username: "):]
        elif line.startswith("owner: "):
            info["subject"] = line[len("owner: "):]
        elif line.startswith("timeleft: "):
            m = re.match(r"^timeleft:\s+(\d+):(\d+):(\d+).*$", line)
            if m:
                times = list(map(int, m.groups()))
                info["time_left"] = times[0] * 3600 + times[1] * 60 + times[2]

    return info


def get_ce_endpoint(ce):
    """
    Extracts the endpoint from a computing element *ce* and returns it. Example:

    .. code-block:: python

        get_ce_endpoint("grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms")
        # -> "grid-ce.physik.rwth-aachen.de:8443"
    """
    return ce.split("/", 1)[0]
