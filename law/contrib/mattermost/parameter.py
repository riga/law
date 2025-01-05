# coding: utf-8

"""
Mattermost-related parameters.
"""

__all__ = ["NotifyMattermostParameter"]

from collections import OrderedDict

from law.config import Config
from law.parameter import NotifyParameter
from law.contrib.mattermost.notification import notify_mattermost


class NotifyMattermostParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifyMattermostParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = (
                "when true, and the task's run method is decorated with law.decorator.notify, "
                "a Mattermost notification is sent once the task finishes"
            )

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
            "colored": False,
        }

    @classmethod
    def notify(cls, success, title, content, **kwargs):
        content = OrderedDict(content)

        # overwrite title
        cfg = Config.instance()
        header = cfg.get_expanded("notifications", "mattermost_header", default=None)
        task_block = "```\n{}\n```".format(content["Task"])
        title = "{}\n{}".format(header, task_block) if header else task_block
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = "\n```\n{}\n```".format(content["Traceback"])

        # prepend the status text to the message content
        cfg = Config.instance()
        status_text = "success" if success else "failure"
        status_emoji = cfg.get_expanded("notifications", "mattermost_{}_emoji".format(status_text),
            default=None)
        if status_emoji:
            status_text += " " + status_emoji
        content["Status"] = status_text
        content.move_to_end("Status", last=False)

        # highlight last message
        if "Last message" in content:
            content["Last message"] = "`{}`".format(content["Last message"])

        # highlight keys
        content = content.__class__(("**{}**".format(k), v) for k, v in content.items())

        # send the notification
        return notify_mattermost(title, content, **kwargs)
