# -*- coding: utf-8 -*-

"""
Slack notification.
"""


import logging
import os

from law.config import Config
from law.parameter import NotifyParameter


logger = logging.getLogger(__name__)


class NotifySlackParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifySlackParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, a slack notification is sent once the task finishes"

    @staticmethod
    def notify(success, title, parts, token=None, channel=None, **kwargs):
        import slackclient
        import json

        cfg = Config.instance()

        if not token:
            token = cfg.get_expanded("notifications", "slack_token")
        if not channel:
            channel = cfg.get_expanded("notifications", "slack_channel")

        if token and channel:
            parts = dict(parts)

            if "Task" in parts:
                text = "*Notification from: {}!*".format(parts["Task"])
                del parts["Task"]
            else:
                text = "# New Notification!"

            attachment = {
                "color": "#4BB543" if success else "#FF0033",
                "title": title,
                "fields": [],
            }

            fallback = "*{}*\n\n".format(title)

            for key, value in parts.items():
                fallback += "_{}_: {}\n".format(key, value)
                attachment["fields"].append({
                    "title": key,
                    "value": "```{}```".format(value) if key == "Traceback" else value,
                    "short": len(value) <= 40,
                })

            attachment["fallback"] = fallback

            # token might be a file
            if os.path.isfile(token):
                with open(token, "r") as f:
                    token = f.read().strip()

            sc = slackclient.SlackClient(token)
            res = sc.api_call(
                "chat.postMessage",
                channel=channel,
                text=text,
                attachments=json.dumps([attachment]),
                as_user=True,
                parse="full",
            )

            if not res["ok"]:
                logger.warning("Unsuccessful Slack API call: %s".format(res))

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
        }
