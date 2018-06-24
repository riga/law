# -*- coding: utf-8 -*-

"""
Slack-related parameters.
"""


import collections

from law.parameter import NotifyParameter
from law.contrib.slack.notification import notify_slack


class NotifySlackParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifySlackParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, a slack notification is sent once the task finishes"

    @staticmethod
    def notify(success, title, content, token=None, channel=None, **kwargs):
        # test import
        import slackclient  # noqa: F401

        # title with slack markup
        slack_title = "Notification from *{}*".format(content["Task"])
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = "```{}```".format(content["Traceback"])

        # prepend the status text to the message content
        parts = list(content.items())
        status_text = "success :tada:" if success else "failure :exclamation:"
        parts.insert(0, ("Status", status_text))
        content = collections.OrderedDict(parts)

        # attachment color and fallback
        color = "#4bb543" if success else "#ff0033"
        fallback = "*{}*\n\n".format(title)

        # send the notification
        return notify_slack(slack_title, content, attachment_color=color,
            attachment_fallback=fallback, token=token, channel=channel, **kwargs)

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
        }
