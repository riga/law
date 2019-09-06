# coding: utf-8

"""
Slack-related parameters.
"""


__all__ = ["NotifySlackParameter"]


import collections

from law.parameter import NotifyParameter
from law.contrib.slack.notification import notify_slack


class NotifySlackParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifySlackParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, a Slack notification is sent once the task finishes"

    @staticmethod
    def notify(success, title, content, **kwargs):
        # overwrite title with slack markdown markup
        title = "*Notification from* `{}`".format(content["Task"])
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = "```{}```".format(content["Traceback"])

        # prepend the status text to the message content
        # emojis are "party popper" and "exclamation mark"
        parts = list(content.items())
        status_text = "success \xF0\x9F\x8E\x89" if success else "failure \xE2\x9D\x97"
        parts.insert(0, ("Status", status_text))
        content = collections.OrderedDict(parts)

        # attachment color depends on success
        color = "#4bb543" if success else "#ff0033"

        # send the notification
        return notify_slack(title, content, attachment_color=color, **kwargs)

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
        }
