# -*- coding: utf-8 -*-

"""
Helpers for working with the Worldwide LHC Computing Grid.
"""


__all__ = ["get_voms_proxy_user", "get_voms_proxy_lifetime", "get_voms_proxy_vo",
           "renew_voms_proxy", "delegate_voms_proxy_glite", "get_ce_endpoint"]


import os
import time
import re
import subprocess
import uuid
import json

import six

from law.util import interruptable_popen, tmp_file, create_hash


def _voms_proxy_info(args=None):
    cmd = ["voms-proxy-info"] + (args or [])
    code, out, err = interruptable_popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if code != 0:
        raise Exception("voms-proxy-info failed: {}".format(err))
    return code, out, err


def get_voms_proxy_user():
    out = _voms_proxy_info(["--identity"])[1].strip()
    try:
        return re.match(".*\/CN\=([^\/]+).*", out.strip()).group(1)
    except:
        raise Exception("no valid identity found in voms proxy: {}".format(out))


def get_voms_proxy_lifetime():
    out = _voms_proxy_info(["--timeleft"])[1].strip()
    try:
        return int(out)
    except:
        raise Exception("no valid lifetime found in voms proxy: {}".format(out))


def get_voms_proxy_vo():
    return _voms_proxy_info(["--vo"])[1].strip()


def renew_voms_proxy(vo, passwd, valid="196:00"):
    with tmp_file() as (_, tmp):
        with open(tmp, "w") as f:
            f.write(passwd)

        cmd = "cat '{}' | voms-proxy-init -voms '{}' --valid '{}'".format(tmp, vo, valid)
        code, out, _ = interruptable_popen(cmd, shell=True, executable="/bin/bash",
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        if code != 0:
            raise Exception("proxy renewal failed: {}".format(out))


def delegate_voms_proxy_glite(endpoint, stdout=None, stderr=None, cache=True):
    # get the proxy file
    proxy_file = os.environ.get("X509_USER_PROXY", "/tmp/x509up_u%i" % os.getuid())
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

    return delegation_id


def get_ce_endpoint(ce):
    return ce.split("/", 1)[0]
