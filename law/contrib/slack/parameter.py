# -*- coding: utf-8 -*-

"""
Slack notification.
"""


import os
import threading
import logging

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

        cfg = Config.instance()

        if not token:
            token = cfg.get_expanded("notifications", "slack_token")
        if not channel:
            channel = cfg.get_expanded("notifications", "slack_channel")

        if token and channel:
            request = {
                "channel": channel,
                "text": "*Notification from: {}!*".format(parts["Task"]),
                "attachments": {
                    "color": "#4BB543" if success else "#FF0033",
                    "title": title,
                    "fields": [],
                    "fallback": "*{}*\n\n".format(title),
                },
                "as_user": True,
                "parse": "full",
            }

            for key, value in parts.items():
                if key == "Task":
                    continue
                request["attachments"]["fallback"] += "_{}_: {}\n".format(key, value)
                request["attachments"]["fields"].append({
                    "title": key,
                    "value": "```{}```".format(value) if key == "Traceback" else value,
                    "short": len(value) <= 40,
                })

            def notify_thread(token, request):
                import json
                import traceback

                try:
                    # token might be a file
                    if os.path.isfile(token):
                        with open(token, "r") as f:
                            token = f.read().strip()

                    request["attachments"] = json.dumps([request["attachments"]])

                    sc = slackclient.SlackClient(token)
                    res = sc.api_call("chat.postMessage", **request)

                    if not res["ok"]:
                        logger.warning("unsuccessful Slack API call: {}".format(res))
                except Exception as e:
                    t = traceback.format_exc()
                    logger.warning("could not send Slack notification: {}\n{}".format(e, t))

            thread = threading.Thread(target=notify_thread, args=(token, request))
            thread.start()

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
        }
