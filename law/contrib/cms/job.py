# -*- coding: utf-8 -*-

"""
CMS-related job helpers.
"""


__all__ = ["CMSJobDashboard"]


import time
import socket

import six

from law.job.base import BaseJobManager, BaseJobDashboard


class CMSJobDashboard(BaseJobDashboard):
    """
    This CMS job dashboard interface requires ``apmon`` to be installed on your system.
    See http://monalisa.caltech.edu/monalisa__Documentation__ApMon_User_Guide__apmon_ug_py.html and
    https://twiki.cern.ch/twiki/bin/view/ArdaGrid/CMSJobMonitoringCollector.
    """

    PENDING = "pending"
    RUNNING = "running"
    CANCELLED = "cancelled"
    POSTPROC = "postproc"
    SUCCESS = "success"
    FAILED = "failed"

    default_apmon_config = {
        "cms-jobmon.cern.ch:8884": {
            "sys_monitoring": 0,
            "general_info": 0,
            "job_monitoring": 0,
        },
    }

    tracking_url = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#" + \
        "table=Jobs&p=1&activemenu=2&tid={dashboard_task_id}"

    persistent_attributes = ["task_id", "cms_user", "voms_user", "init_timestamp"]

    def __init__(self, task, cms_user, voms_user, apmon_config=None, log_level="INFO", max_rate=20,
            task_type="analysis", site=None, executable="law", application=None,
            application_version=None, submission_tool="law", submission_type="direct",
            submission_ui=None):
        # setup the apmon interface
        import apmon
        apmon_config = apmon_config or self.default_apmon_config
        log_level = getattr(apmon.Logger, log_level.upper())
        self.apmon = apmon.ApMon(apmon_config, log_level)

        # hotfix of a bug occurring in apmon for too large pids
        for key, value in self.apmon.senderRef.items():
            value["INSTANCE_ID"] = value["INSTANCE_ID"] & 0x7fffffff

        super(CMSJobDashboard, self).__init__(max_rate=max_rate)

        # mandatory (persistent) attributes
        self.task_id = task
        self.cms_user = cms_user
        self.voms_user = voms_user
        self.init_timestamp = self.create_timestamp()

        # optional attributes
        self.task_type = task_type
        self.site = site
        self.executable = executable
        self.application = application or task.task_family
        self.application_version = application_version or self.task_id.rsplit("_", 1)[1]
        self.submission_tool = submission_tool
        self.submission_type = submission_type
        self.submission_ui = submission_ui or socket.gethostname()

    @classmethod
    def create_timestamp(cls):
        return time.strftime("%y%m%d_%H%M%S")

    @classmethod
    def create_dashboard_task_id(cls, task_id, cms_user, timestamp=None):
        if not timestamp:
            timestamp = cls.create_timestamp()
        return "{}:{}_{}".format(timestamp, cms_user, task_id)

    @classmethod
    def create_dashboard_job_id(cls, job_num, job_id, attempt=0):
        return "{}_{}_{}".format(job_num, job_id, attempt)

    @classmethod
    def params_from_status(cls, dashboard_status, fail_code=1):
        if dashboard_status == cls.PENDING:
            return {"StatusValue": "pending", "SyncCE": None}
        elif dashboard_status == cls.RUNNING:
            return {"StatusValue": "running"}
        elif dashboard_status == cls.CANCELLED:
            return {"StatusValue": "cancelled", "SyncCE": None}
        elif dashboard_status == cls.POSTPROC:
            return {"StatusValue": "running", "JobExitCode": 0}
        elif dashboard_status == cls.SUCCESS:
            return {"StatusValue": "success", "JobExitCode": 0}
        elif dashboard_status == cls.FAILED:
            return {"StatusValue": "failed", "JobExitCode": fail_code}
        else:
            raise ValueError("invalid dashboard status '{}'".format(dashboard_status))

    @property
    def max_rate(self):
        return self.apmon.maxMsgRate

    @max_rate.setter
    def max_rate(self, max_rate):
        self.apmon.maxMsgRate = max_rate

    def map_status(self, job_status, event):
        if event == "action.submit":
            return self.PENDING
        elif event == "action.cancel":
            return self.CANCELLED
        else:
            # event must start with "status." and end with the valid job status
            if not event.startswith("status.") or event.split(".", 1)[-1] != job_status:
                raise ValueError("event '{}' does not match job status '{}'".format(
                    event, job_status))
            elif job_status not in BaseJobManager.status_names:
                raise ValueError("invalid job status '{}'".format(job_status))
            elif job_status == BaseJobManager.PENDING:
                return self.PENDING
            elif job_status == BaseJobManager.RUNNING:
                return self.RUNNING
            elif job_status == BaseJobManager.FINISHED:
                return self.SUCCESS
            elif job_status == BaseJobManager.RETRY:
                return self.FAILED
            elif job_status == BaseJobManager.FAILED:
                return self.FAILED

    def create_tracking_url(self):
        dashboard_task_id = self.create_dashboard_task_id(self.task_id, self.cms_user,
            self.init_timestamp)
        return self.tracking_url.format(dashboard_task_id=dashboard_task_id)

    @BaseJobDashboard.cache_by_status
    def publish(self, job_num, job_data, event, attempt=0, custom_params=None, **kwargs):
        # we need the voms user, which must start with "/CN="
        voms_user = self.voms_user
        if not voms_user:
            return
        if not voms_user.startswith("/CN="):
            voms_user = "/CN=" + voms_user

        # map to job status to a valid dashboard status
        dashboard_status = self.map_status(job_data.get("status"), event)
        if not dashboard_status:
            return

        # build the dashboard task id
        dashboard_task_id = self.create_dashboard_task_id(self.task_id, self.cms_user,
            self.init_timestamp)

        # build the id of the particular job
        dashboard_job_id = self.create_dashboard_job_id(job_num, job_data["job_id"],
            attempt=attempt)

        # build the parameters to send
        params = {
            "TaskId": dashboard_task_id,
            "JobId": dashboard_job_id,
            "GridJobId": job_data["job_id"],
            "CMSUser": self.cms_user,
            "GridName": voms_user,
            "JSToolUI": kwargs.get("submission_ui", self.submission_ui),
        }

        # add optional params
        params.update({
            "TaskType": kwargs.get("task_type", self.task_type),
            "SyncCE": kwargs.get("site", self.site),
            "Executable": kwargs.get("executable", self.executable),
            "Application": kwargs.get("application", self.application),
            "ApplicationVersion": kwargs.get("application_version", self.application_version),
            "JSTool": kwargs.get("submission_tool", self.submission_tool),
            "SubmissionType": kwargs.get("submission_type", self.submission_type),
        })

        # add status params
        params.update(self.params_from_status(dashboard_status, fail_code=job_data.get("code", 1)))

        # add custom params
        if custom_params:
            params.update(custom_params)

        # finally filter None's and convert everything to strings
        params = {key: str(value) for key, value in six.iteritems(params) if value is not None}

        # send
        with self.rate_guard():
            self.apmon.sendParameters(dashboard_task_id, dashboard_job_id, params)
