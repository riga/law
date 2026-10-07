"""
CMS-related contrib package. https://home.cern/about/experiments/cms
"""

__all__ = [
    "BundleCMSSW",
    "CMSJobDashboard",
    "CMSSWSandbox",
    "CrabJobFileFactory",
    "CrabJobManager",
    "CrabWorkflow",
    "RucioReporter",
    "Site",
    "delegate_myproxy",
    "lfn_to_pfn",
    "renew_vomsproxy",
    "rucio_report_access",
]

# dependencies to other contrib modules
import law

law.contrib.load("wlcg")

# provisioning imports
from law.contrib.cms.job import CMSJobDashboard, CrabJobFileFactory, CrabJobManager
from law.contrib.cms.sandbox import CMSSWSandbox
from law.contrib.cms.tasks import BundleCMSSW
from law.contrib.cms.util import RucioReporter, Site, delegate_myproxy, lfn_to_pfn, renew_vomsproxy, rucio_report_access
from law.contrib.cms.workflow import CrabWorkflow
