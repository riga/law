# coding: utf-8

"""
CMS-related job helpers.
"""


__all__ = ["CMSJobDashboard"]


import time
import socket
import threading

import six

import law
from law.job.base import BaseJobManager
from law.job.dashboard import BaseJobDashboard


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

    tracking_url = "http://dashb-cms-job.cern.ch/dashboard/templates/task-analysis/#" + \
        "table=Jobs&p=1&activemenu=2&refresh=60&tid={dashboard_task_id}"

    persistent_attributes = ["task_id", "cms_user", "voms_user", "init_timestamp"]

    def __init__(self, task, cms_user, voms_user, apmon_config=None, log_level="WARNING",
            max_rate=20, task_type="analysis", site=None, executable="law", application=None,
            application_version=None, submission_tool="law", submission_type="direct",
            submission_ui=None, init_timestamp=None):
        super(CMSJobDashboard, self).__init__(max_rate=max_rate)

        # setup the apmon thread
        try:
            self.apmon = Apmon(apmon_config, self.max_rate, log_level)
        except ImportError as e:
            e.message += " (required for {})".format(self.__class__.__name__)
            e.args = (e.message,) + e.args[1:]
            raise e

        # get the task family for use as default application name
        task_family = task.get_task_family() if isinstance(task, law.Task) else task

        # mandatory (persistent) attributes
        self.task_id = task.task_id if isinstance(task, law.Task) else task
        self.cms_user = cms_user
        self.voms_user = voms_user
        self.init_timestamp = init_timestamp or self.create_timestamp()

        # optional attributes
        self.task_type = task_type
        self.site = site
        self.executable = executable
        self.application = application or task_family
        self.application_version = application_version or self.task_id.rsplit("_", 1)[1]
        self.submission_tool = submission_tool
        self.submission_type = submission_type
        self.submission_ui = submission_ui or socket.gethostname()

        # start the apmon thread
        self.apmon.daemon = True
        self.apmon.start()

    def __del__(self):
        if getattr(self, "apmon", None) and self.apmon.is_alive():
            self.apmon.stop()
            self.apmon.join()

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

    @classmethod
    def map_status(cls, job_status, event):
        # when starting with "status.", event must end with the job status
        if event.startswith("status.") and event.split(".", 1)[-1] != job_status:
            raise ValueError("event '{}' does not match job status '{}'".format(event, job_status))

        status = lambda attr: "status.{}".format(getattr(BaseJobManager, attr))

        return {
            "action.submit": cls.PENDING,
            "action.cancel": cls.CANCELLED,
            "custom.running": cls.RUNNING,
            "custom.postproc": cls.POSTPROC,
            "custom.failed": cls.FAILED,
            status("FINISHED"): cls.SUCCESS,
        }.get(event)

    def remote_hook_file(self):
        return law.util.rel_path(__file__, "scripts", "cmsdashb_hooks.sh")

    def remote_hook_data(self, job_num, attempt):
        data = [
            "task_id='{}'".format(self.task_id),
            "cms_user='{}'".format(self.cms_user),
            "voms_user='{}'".format(self.voms_user),
            "init_timestamp='{}'".format(self.init_timestamp),
            "job_num={}".format(job_num),
            "attempt={}".format(attempt),
        ]
        if self.site:
            data.append("site='{}'".format(self.site))
        return data

    def create_tracking_url(self):
        dashboard_task_id = self.create_dashboard_task_id(self.task_id, self.cms_user,
            self.init_timestamp)
        return self.tracking_url.format(dashboard_task_id=dashboard_task_id)

    def create_message(self, job_data, event, job_num, attempt=0, custom_params=None, **kwargs):
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

        return (dashboard_task_id, dashboard_job_id, params)

    @BaseJobDashboard.cache_by_status
    def publish(self, *args, **kwargs):
        message = self.create_message(*args, **kwargs)
        if message:
            self.apmon.send(*message)


apmon_lock = threading.Lock()


class Apmon(threading.Thread):

    default_config = {
        "cms-jobmon.cern.ch:8884": {
            "sys_monitoring": 0,
            "general_info": 0,
            "job_monitoring": 0,
        },
    }

    def __init__(self, config=None, max_rate=20, log_level="INFO"):
        super(Apmon, self).__init__()

        import apmon
        log_level = getattr(apmon.Logger, log_level.upper())
        self._apmon = apmon.ApMon(config or self.default_config, log_level)
        self._apmon.maxMsgRate = int(max_rate * 1.5)

        # hotfix of a bug occurring in apmon for too large pids
        for key, value in self._apmon.senderRef.items():
            value["INSTANCE_ID"] = value["INSTANCE_ID"] & 0x7fffffff

        self._max_rate = max_rate
        self._queue = six.moves.queue.Queue()
        self._stop_event = threading.Event()

    def send(self, *args, **kwargs):
        self._queue.put((args, kwargs))

    def _send(self, *args, **kwargs):
        self._apmon.sendParameters(*args, **kwargs)

    def stop(self):
        self._stop_event.set()

    def run(self):
        while True:
            # handling stopping
            self._stop_event.wait(0.5)
            if self._stop_event.is_set():
                break

            if self._queue.empty():
                continue

            with apmon_lock:
                while not self._queue.empty():
                    args, kwargs = self._queue.get()
                    self._send(*args, **kwargs)
                    time.sleep(1. / self._max_rate)
