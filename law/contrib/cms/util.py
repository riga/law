# coding: utf-8

"""
CMS-related utilities.
"""

from __future__ import annotations

__all__ = ["Site", "lfn_to_pfn", "renew_vomsproxy", "delegate_myproxy"]

import os

import law


law.contrib.load("wlcg")


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
    with the *vo* attribute set to ``"cms"`` by default.
    """
    kwargs.setdefault("vo", "cms")
    return law.wlcg.renew_vomsproxy(**kwargs)  # type: ignore[attr-defined]


def delegate_myproxy(**kwargs) -> str | None:
    """
    Delegates a X509 proxy to a myproxy server in the exact same way that
    :py:func:`law.wlcg.delegate_myproxy` does, but with the *retrievers* argument set to a value
    that is usually expected for crab submissions and the vo set to "cms".
    """
    kwargs.setdefault(
        "retrievers",
        "/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod|dev)-tw(01|02|03).cern.ch|/DC=ch/DC=cern/OU=computers/CN=stefanov(m|m2).cern.ch|/DC=ch/DC=cern/OU=computers/CN=dciangot-tw.cern.ch",  # noqa
    )
    kwargs.setdefault("vo", "cms")
    return law.wlcg.delegate_myproxy(**kwargs)  # type: ignore[attr-defined]
