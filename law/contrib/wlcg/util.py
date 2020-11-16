# coding: utf-8

"""
Helpers for working with the WLCG.
"""


__all__ = [
    "get_voms_proxy_file", "get_voms_proxy_user", "get_voms_proxy_lifetime", "get_voms_proxy_vo",
    "check_voms_proxy_validity", "renew_voms_proxy", "delegate_voms_proxy_glite", "get_ce_endpoint",
]


import os
import re
import subprocess
import uuid
import json
import logging

import six

from law.util import (
    interruptable_popen, tmp_file, create_hash, human_duration, parse_duration, quote_cmd,
)


logger = logging.getLogger(__name__)


def get_voms_proxy_file():
    """
    Returns the path to the voms proxy file.
    """
    if "X509_USER_PROXY" in os.environ:
        return os.environ["X509_USER_PROXY"]
    else:
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
    try:
        return re.match(r".*\/CN\=([^\/]+).*", out.strip()).group(1)
    except:
        raise Exception("no valid identity found in voms proxy: {}".format(out))


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


def check_voms_proxy_validity(log=False, proxy_file=None):
    """
    Returns *True* when a valid voms proxy exists, *False* otherwise. When *log* is *True*, a
    warning will be logged. When *proxy_file* is *None*, it defaults to the result of
    :py:func:`get_voms_proxy_file`. Otherwise, when it evaluates to *False*, ``voms-proxy-info`` is
    queried without a custom proxy file.
    """
    code, out, err = _voms_proxy_info(args=["--exists"], proxy_file=proxy_file, silent=True)

    if code == 0:
        valid = get_voms_proxy_lifetime(proxy_file=proxy_file) > 0
    elif err.strip().lower().startswith("proxy not found"):
        valid = False
    else:
        raise Exception("voms-proxy-info failed: {}".format(err))

    if log and not valid:
        logger.warning("no valid voms proxy found")

    return valid


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
    lifetime_seconds = max(parse_duration(lifetime, input_unit="h", unit="s"), 60.)
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


def get_ce_endpoint(ce):
    """
    Extracts the endpoint from a computing element *ce* and returns it. Example:

    .. code-block:: python

        get_ce_endpoint("grid-ce.physik.rwth-aachen.de:8443/cream-pbs-cms")
        # -> "grid-ce.physik.rwth-aachen.de:8443"
    """
    return ce.split("/", 1)[0]
