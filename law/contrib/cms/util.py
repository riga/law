# coding: utf-8

"""
CMS-related utilities.
"""

__all__ = ["Site", "lfn_to_pfn", "delegate_my_proxy"]


import os

import law

law.contrib.load("wlcg")


class Site(object):
    """
    Helper class that provides site-related data, mostly via simple properties. When *name* is
    *None*, the name of the site is used that the instance of this class is instantiated on.
    Example:

    .. code-block:: python

        site = Site() # executed on T2_DE_RWTH
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

    def __init__(self, name=None):
        super(Site, self).__init__()

        # site name cache
        self.name = name or self.get_name_from_env()

    @classmethod
    def get_name_from_env(cls):
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
    def info(self):
        """
        Tier, country and locality information in a 3-tuple, e.g. ``("T2", "DE", "RWTH")``.
        """
        return self.name and self.name.split("", 2)

    @property
    def tier(self):
        """
        The tier of the site, e.g. ``T2``.
        """
        return self.name and self.info[0]

    @property
    def country(self):
        """
        The country of the site, e.g. ``DE``.
        """
        return self.name and self.info[1]

    @property
    def locality(self):
        """
        The locality of the site, e.g. ``RWTH``.
        """
        return self.name and self.info[2]

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


def delegate_my_proxy(*args, **kwargs):
    """
    Delegates a X509 proxy to a myproxy server in the exact same way that
    :py:func:`law.wlcg.delegate_my_proxy` does, but with the *retrievers* argument set to a value
    that is usually expected for crab submissions.
    """
    kwargs.setdefault(
        "retrievers",
        "/DC=ch/DC=cern/OU=computers/CN=crab-(preprod|prod|dev)-tw(01|02|03).cern.ch|/DC=ch/DC=cern/OU=computers/CN=stefanov(m|m2).cern.ch|/DC=ch/DC=cern/OU=computers/CN=dciangot-tw.cern.ch",  # noqa
    )
    return law.wlcg.delegate_my_proxy(*args, **kwargs)
