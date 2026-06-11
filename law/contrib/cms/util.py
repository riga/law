# coding: utf-8

"""
CMS-related utilities.
"""

from __future__ import annotations

__all__ = ["Site", "lfn_to_pfn", "renew_vomsproxy", "delegate_myproxy", "RucioReporter", "rucio_report_access"]

import os
import time
import copy
import base64
import functools
import atexit
import queue
import urllib.parse
import threading

import law
from law.logger import get_logger
from law._types import Any, Sequence, Callable

law.contrib.load("wlcg")


logger = get_logger(__name__)

# obtained via _get_crab_receivers below
_default_crab_receivers = [
    "/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod)-tw(01|02).cern.ch|/DC=ch/DC=cern/OU=computers/CN=crab-dev-tw(01|02|03|04).cern.ch|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=cmscrab/CN=(817881|373708)/CN=Robot: cms crab|/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=crabint1/CN=373708/CN=Robot: CMS CRAB Integration 1",  # noqa
]


def _default_vo() -> str:
    return os.getenv("LAW_CMS_VO", "cms")


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

    redirectors = {
        "global": "cms-xrd-global.cern.ch",
        "eu": "xrootd-cms.infn.it",
        "us": "cmsxrootd.fnal.gov",
    }

    def __init__(self, name: str | None = None) -> None:
        super().__init__()

        # site name cache
        self.name: str | None = self.get_name_from_env() if name is None else name

    @classmethod
    def get_name_from_env(cls) -> str | None:
        """
        Tries to extract the site name from environment variables. Returns the name on succcess and
        *None* otherwise.
        """
        # TODO: add fallbacks
        for v in ["GLIDEIN_CMSSite"]:
            if v in os.environ:
                return os.getenv(v)
        return None

    @property
    def info(self) -> tuple[str, str, str] | tuple[None, None, None]:
        """
        Tier, country and locality information in a 3-tuple, e.g. ``("T2", "DE", "RWTH")``.
        """
        if self.name is not None:
            info = self.name.split("_", 2)
            if len(info) != 3:
                raise ValueError(f"invalid site name: {self.name}")
            return tuple(info)  # type: ignore[return-value]

        return (None, None, None)

    @property
    def tier(self) -> str | None:
        """
        The tier of the site, e.g. ``T2``.
        """
        info = self.info
        return None if self.info is None else info[0]

    @property
    def country(self) -> str | None:
        """
        The country of the site, e.g. ``DE``.
        """
        info = self.info
        return None if self.info is None else info[1]

    @property
    def locality(self):
        """
        The locality of the site, e.g. ``RWTH``.
        """
        info = self.info
        return None if self.info is None else info[2]

    @property
    def redirector(self) -> str:
        """
        The XRD redirector that should be used on this site. For more information on XRD, see
        `this link <https://twiki.cern.ch/twiki/bin/view/CMSPublic/WorkBookXrootdService>`_.
        """
        country = self.country
        if country in self.redirectors:
            return self.redirectors[country]
        return self.redirectors["global"]


def lfn_to_pfn(lfn: str, redirector: str = "global") -> str:
    """
    Converts a logical file name *lfn* to a physical file name *pfn* using a *redirector*. Valid
    values for *redirector* are defined by :py:attr:`Site.redirectors`.
    """
    if redirector not in Site.redirectors:
        raise ValueError(f"unknown redirector: {redirector}")

    return f"root://{Site.redirectors[redirector]}/{lfn}"


def renew_vomsproxy(**kwargs) -> str | None:
    """
    Renews a VOMS proxy in the exact same way that :py:func:`law.wlcg.renew_vomsproxy` does, but
    with the *vo* argument default to the environment variable LAW_CMS_VO or ``"cms"`` when empty.
    """
    if "vo" not in kwargs:
        kwargs["vo"] = _default_vo()
    return law.wlcg.renew_vomsproxy(**kwargs)  # type: ignore[attr-defined]


def delegate_myproxy(**kwargs) -> str | None:
    """
    Delegates a X509 proxy to a myproxy server in the exact same way that
    :py:func:`law.wlcg.delegate_myproxy` does, but with the *vo* argument default to the environment
    variable LAW_CMS_VO or ``"cms"`` when empty.
    """
    if "vo" not in kwargs:
        kwargs["vo"] = _default_vo()
    return law.wlcg.delegate_myproxy(**kwargs)  # type: ignore[attr-defined]


def _get_crab_receivers() -> None:
    from CRABClient.ClientUtilities import initLoggers, server_info  # type: ignore[import-not-found] # noqa
    from CRABClient.Commands.createmyproxy import createmyproxy  # type: ignore[import-not-found]

    cmd = createmyproxy(logger=initLoggers()[1])
    alldns = server_info(crabserver=cmd.crabserver, subresource="delegatedn")
    print(alldns.get("services"))


class RucioReporter(threading.Thread):

    default_client_args: dict[str, Any] | None = None
    default_server_url: str = "aHR0cDovL2Ntcy1ydWNpby10cmFjZS5jZXJuLmNo"
    default_max_rate: int | float = 3

    __instance: RucioReporter | None = None

    @classmethod
    def instance(cls, *args, **kwargs) -> RucioReporter:
        if cls.__instance is None:
            cls.__instance = cls(*args, **kwargs)
            cls.__instance.start()
        return cls.__instance

    @classmethod
    def stop_instance(cls) -> None:
        if cls.__instance is not None:
            cls.__instance.stop()
            cls.__instance = None

    def __init__(
        self,
        client_args: dict[str, Any] | None = None,
        server_url: str | None = None,
        max_rate: int | float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(daemon=True, **kwargs)

        # store attributes
        self._server_url = server_url or self.default_server_url
        self._max_rate = max_rate if max_rate is not None else self.default_max_rate
        self._queue: queue.Queue = queue.Queue()
        self._stop_event = threading.Event()
        self._lock = threading.Lock()

        # setup the client in a fail-safe way
        self._client = None
        _client_args = copy.deepcopy(self.default_client_args or {})
        _client_args.update(copy.deepcopy(client_args or {}))
        try:
            import rucio.client  # type: ignore[import-not-found, import-untyped]
        except ImportError as e:
            logger.warning(f"rucio file access reporting disabled: {e}")
        else:
            try:
                self._client = rucio.client.Client(**_client_args)
            except rucio.common.exception.ConfigNotFound as e:
                logger.warning(f"rucio file access reporting disabled: {e}")
        if self._client is None:
            self.stop()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
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
                    raise ValueError(f"unknown event in {self.__class__.__name__}: {event}")

                # invoke callbacks for this event, enforcing max_rate calls per second
                for i, callback in enumerate(callbacks):
                    if self._stop_event.is_set():
                        break
                    if i > 0 and self._max_rate > 0:
                        time.sleep(1.0 / self._max_rate)
                    callback()

    def _report_access_callbacks(
        self,
        lfn: str,
        rse: str | Sequence[str] | None = None,
        silent: bool = True,
    ) -> list[Callable[[], None]]:
        # identify rse's when not set
        if rse is None:
            rses = []
            replicas = self._client.list_replicas([{"scope": "cms", "name": lfn}])  # type: ignore[union-attr]
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
            rses = law.util.make_unique(law.util.make_list(rse))  # type: ignore[assignment]

        # build callbacks
        return [functools.partial(self._report_access, lfn=lfn, rse=rse, silent=silent) for rse in rses]

    def _report_access(self, *, lfn: str, rse: str, silent: bool = True) -> None:
        # https://github.com/dmwm/CMSRucio/blob/master/UserDMTools/trace_example/TraceSendExample.py
        import requests  # type: ignore[import-not-found, import-untyped]

        url = self._server_url if "://" in self._server_url else base64.b64decode(self._server_url).decode("utf-8")
        endpoint = urllib.parse.urljoin(url, "traces")

        data = {
            "eventType": "touch",
            "clientState": "DONE",
            "account": os.getenv("RUCIO_ACCOUNT"),
            "localSite": rse,
            "remoteSite": rse,
            "scope": "cms",
            "filename": lfn,
        }
        try:
            res = requests.post(endpoint, json=data)
            res.raise_for_status()
        except requests.HTTPError as e:
            msg = f"rucio file access reporting failed for lfn '{lfn}' and rse '{rse}': {e}"
            if silent:
                logger.debug(msg)
            else:
                logger.error(msg)
                raise
        else:
            logger.debug(f"rucio file access reported for lfn '{lfn}' and rse '{rse}' at {endpoint}")

    def report_access(self, lfn: str, rse: str | Sequence[str] | None = None, silent: bool = True) -> None:
        if self._stop_event.is_set():
            logger.info(f"skipping rucio file access reporting for lfn '{lfn}' and rse '{rse}': reporter is stopped")
            return

        event = "report_access"
        data = {"lfn": lfn, "rse": rse, "silent": silent}
        self._queue.put((event, data))


atexit.register(RucioReporter.stop_instance)


def rucio_report_access(lfn: str, rse: str | Sequence[str] | None = None, silent: bool = True) -> None:
    RucioReporter.instance().report_access(lfn=lfn, rse=rse, silent=silent)
