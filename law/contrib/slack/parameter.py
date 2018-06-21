# -*- coding: utf-8 -*-

"""
Slack notification.
"""


import os

from law.config import Config
from law.parameter import NotifyParameter


class NotifySlackParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifySlackParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, a slack notification is sent once the task finishes"

    @staticmethod
    def notify(success, title, parts, token=None, channel=None, **kwargs):
        import slackclient

        cfg = Config.instance()

        if not token:
            token = cfg.get("notifications", "slack_token")
        if not channel:
            channel = cfg.get("notifications", "slack_channel")

        if token and channel:
            # minimal markup
            text = "*{}*\n\n".format(title)
            for key, value in parts:
                text += "_{}_: {}\n".format(key, value)

            # token might be a file
            if os.path.isfile(token):
                with open(token, "r") as f:
                    token = f.read().strip()

            sc = slackclient.SlackClient(token)
            sc.api_call(
                "chat.postMessage",
                channel=channel,
                text=text,
            )

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
        }
