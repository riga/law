# -*- coding: utf-8 -*-

"""
Slack notification.
"""


from law.config import Config
from law.parameter import NotifyParameter


class NotifySlackParameter(NotifyParameter):

    @staticmethod
    def notify(title, parts, token=None, channel=None, **kwargs):
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
