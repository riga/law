# coding: utf-8

"""
CMS-related utilities.
"""

__all__ = ["Site", "lfn_to_pfn", "renew_vomsproxy", "delegate_myproxy", "RucioReporter", "rucio_report_access"]


import os
import re
import sys
import time
import copy
import base64
import functools
import atexit
import urllib.parse
import threading

import six

import law
from law.logger import get_logger

law.contrib.load("wlcg")


logger = get_logger(__name__)

# obtained via _get_crab_receivers below
_default_crab_receivers = [
    "/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod)-tw(01|02).cern.ch|/DC=ch/DC=cern/OU=computers/CN=crab-dev-tw(01|02|03|04).cern.ch|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmscrab/CN=(817881|373708)/CN=Robot: cms crab|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=crabint1/CN=373708/CN=Robot: CMS CRAB Integration 1",  # noqa
]


class Site(object):
    """
    Helper class that provides site-related data, mostly via simple properties. When *name* is
    *None*, the name of the site is used that the instance of this class is instantiated on.
    Example:

    .. code-block:: python

        site = Site()  # executed on T2_DE_RWTH
        print(site.name)        # "T2_DE_RWTH"
        print(site.country)     # "DE"
        print(site.redirector)  # "xrootd-cms.infn.it"

        site = Site("T1_US_FNAL")
        print(site.name)        # "T1_US_FNAL"
        print(site.country)     # "US"
        print(site.redirector)  # "cmsxrootd.fnal.gov"

    .. py:classattribute:: redirectors

        type: dict

        A mapping of country codes to redirectors.

    .. py:attribute:: name

        type: string

        The name of the site, e.g. ``T2_DE_RWTH``. This is either the name provided in the
        constructor or it is determined for the current site by reading environment variables.
    """

    name_cre = re.compile(r"^(T\d)_([A-Z]{2,2})_(.+)$")

    redirectors = {
        "global": "cms-xrd-global.cern.ch",
        "eu": "xrootd-cms.infn.it",
        "us": "cmsxrootd.fnal.gov",
    }

    @classmethod
    def validate(cls, name):
        return bool(cls.name_cre.match(name))

    @classmethod
    def get_name_from_env(cls):
        """
        Tries to extract the local site name from the environment. Returns the name on succcess and
        *None* otherwise.
        """
        # check local site config
        siteconf_path = "/cvmfs/cms.cern.ch/SITECONF/local"
        if os.path.exists(siteconf_path):
            name = os.path.basename(os.path.realpath(siteconf_path))
            if cls.validate(name):
                return name

        # fallbacks from env variables
        for v in ["GLIDEIN_CMSSite"]:
            name = os.getenv(v)
            if name and cls.validate(name):
                return name

        return None

    def __init__(self, name=None):
        super(Site, self).__init__()

        # site name cache
        self.name = name or self.get_name_from_env()

        # validate the name when set
        if self.name:
            self.validate(self.name)

    @property
    def info(self):
        """
        Tier, country and locality information in a 3-tuple, e.g. ``("T2", "DE", "RWTH")``.
        """
        return self.name and self.name_cre.match(self.name).groups()

    @property
    def tier(self):
        """
        The tier of the site, e.g. ``T2``.
        """
        return self.name and self.name_cre.match(self.name).group(1)

    @property
    def country(self):
        """
        The country of the site, e.g. ``DE``.
        """
        return self.name and self.name_cre.match(self.name).group(2)

    @property
    def locality(self):
        """
        The locality of the site, e.g. ``RWTH``.
        """
        return self.name and self.name_cre.match(self.name).group(3)

    @property
    def redirector(self):
        """
        The XRD redirector that should be used on this site. For more information on XRD, see
        `this link <https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookXrootdService>`_.
        """
        return self.redirectors.get(self.country.lower(), self.redirectors["global"])


def lfn_to_pfn(lfn, redirector="global"):
    """
    Converts a logical file name *lfn* to a physical file name *pfn* using a *redirector*. Valid
    values for *redirector* are defined by :py:attr:`Site.redirectors`.
    """
    if redirector not in Site.redirectors:
        raise ValueError("unknown redirector: {}".format(redirector))

    return "root://{}/{}".format(Site.redirectors[redirector], lfn)


def _default_vo():
    return os.getenv("LAW_CMS_VO", "cms")


def renew_vomsproxy(**kwargs):
    """
    Renews a VOMS proxy in the exact same way that :py:func:`law.wlcg.renew_vomsproxy` does, but
    with the *vo* argument default to the environment variable LAW_CMS_VO or ``"cms"`` when empty.
    """
    if "vo" not in kwargs:
        kwargs["vo"] = _default_vo()
    return law.wlcg.renew_vomsproxy(**kwargs)


def delegate_myproxy(**kwargs):
    """
    Delegates a X509 proxy to a myproxy server in the exact same way that
    :py:func:`law.wlcg.delegate_myproxy` does, but with the *vo* argument default to the environment
    variable LAW_CMS_VO or ``"cms"`` when empty.
    """
    if "vo" not in kwargs:
        kwargs["vo"] = _default_vo()
    return law.wlcg.delegate_myproxy(**kwargs)


def _get_crab_receivers():
    from CRABClient.ClientUtilities import initLoggers, server_info
    from CRABClient.Commands.createmyproxy import createmyproxy

    cmd = createmyproxy(logger=initLoggers()[1])
    alldns = server_info(crabserver=cmd.crabserver, subresource="delegatedn")
    print(alldns.get("services"))


class RucioReporter(threading.Thread):

    default_client_args = None
    default_server_url = "aHR0cDovL2Ntcy1ydWNpby10cmFjZS5jZXJuLmNo"
    default_max_rate = 3

    __instance = None

    @classmethod
    def instance(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = cls(*args, **kwargs)
            cls.__instance.start()
        return cls.__instance

    @classmethod
    def stop_instance(cls):
        if cls.__instance is not None:
            cls.__instance.stop()
            cls.__instance = None

    def __init__(self, client_args=None, server_url=None, max_rate=None, **kwargs):
        super().__init__(daemon=True, **kwargs)

        # store attributes
        self._server_url = server_url or self.default_server_url
        self._max_rate = max_rate if max_rate is not None else self.default_max_rate
        self._queue = six.moves.queue.Queue()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # setup the client in a fail-safe way
        self._client = None
        _client_args = copy.deepcopy(self.default_client_args or {})
        _client_args.update(copy.deepcopy(client_args or {}))
        try:
            import rucio.client
        except ImportError as e:
            logger.warning("rucio file access reporting disabled: {}".format(e))
        else:
            try:
                self._client = rucio.client.Client(**_client_args)
            except rucio.common.exception.ConfigNotFound as e:
                logger.warning("rucio file access reporting disabled: {}".format(e))
        if self._client is None:
            self.stop()

    def stop(self):
        self._stop_event.set()

    def run(self):
        while True:
            # handling stopping
            self._stop_event.wait(1)
            if self._stop_event.is_set():
                break

            # work on queue
            while True:
                with self._lock:
                    # get work or wait
                    if self._queue.empty():
                        break
                    event, data = self._queue.get()

                # dispatch to handler, gather list of callbacks to invoke
                if event == "report_access":
                    callbacks = self._report_access_callbacks(**data)
                else:
                    raise ValueError("unknown event in {}: {}".format(self.__class__.__name__, event))

                # invoke callbacks for this event, enforcing max_rate calls per second
                for i, callback in enumerate(callbacks):
                    if self._stop_event.is_set():
                        break
                    if i > 0 and self._max_rate > 0:
                        time.sleep(1.0 / self._max_rate)
                    callback()

    def _report_access_callbacks(self, lfn, rse=None, local_rse=None, silent=True):
        # identify rse's when not set
        if rse is None:
            rses = []
            replicas = self._client.list_replicas([{"scope": "cms", "name": lfn}])
            for replica in replicas:
                for pfn_data in replica["pfns"].values():
                    # skip tape replicas
                    if pfn_data.get("type", "").lower() == "tape":
                        continue
                    # skip unavailable replicas
                    _rse = pfn_data["rse"]
                    if replica["states"].get(_rse, "").lower() != "available":
                        continue
                    # store it
                    if _rse not in rses:
                        rses.append(_rse)
        else:
            rses = law.util.make_unique(law.util.make_list(rse))

        # identify the local rse when not set
        if local_rse is None:
            local_rse = Site.get_name_from_env()

        # build callbacks
        return [
            functools.partial(self._report_access, lfn=lfn, rse=rse, local_rse=local_rse, silent=silent)
            for rse in rses
        ]

    def _report_access(self, lfn, rse, local_rse, silent=True):
        # https://github.com/dmwm/CMSRucio/blob/master/UserDMTools/trace_example/TraceSendExample.py
        import requests

        url = self._server_url if "://" in self._server_url else base64.b64decode(self._server_url).decode("utf-8")
        endpoint = urllib.parse.urljoin(url, "traces")

        data = {
            "eventType": "touch",
            "clientState": "DONE",
            "account": os.getenv("RUCIO_ACCOUNT"),
            "localSite": local_rse or rse,
            "remoteSite": rse,
            "scope": "cms",
            "filename": lfn,
        }
        try:
            res = requests.post(endpoint, json=data)
            res.raise_for_status()
        except requests.HTTPError as e:
            msg = "rucio file access reporting failed for lfn '{}' and rse '{}': {}".format(lfn, rse, e)
            if silent:
                logger.debug(msg)
            else:
                logger.error(msg)
                six.reraise(*sys.exc_info())
        else:
            logger.debug("rucio file access reported for lfn '{}' and rse '{}' at {}".format(lfn, rse, endpoint))

    def report_access(self, lfn, rse=None, local_rse=None, silent=True):
        if self._stop_event.is_set():
            logger.info(
                "skipping rucio file access reporting for lfn '{}' and rse '{}': reporter is stopped".format(lfn, rse),
            )
            return

        event = "report_access"
        data = {"lfn": lfn, "rse": rse, "local_rse": local_rse, "silent": silent}
        self._queue.put((event, data))


atexit.register(RucioReporter.stop_instance)


def rucio_report_access(lfn, rse=None, local_rse=None, silent=True):
    return RucioReporter.instance().report_access(lfn=lfn, rse=rse, local_rse=local_rse, silent=silent)
