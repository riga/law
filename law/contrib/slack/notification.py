# -*- coding: utf-8 -*-

"""
Slack notifications.
"""


import threading
import logging

from law.config import Config


logger = logging.getLogger(__name__)


def notify_slack(title, content, attachment_color="#4bb543", attachment_fallback=None,
        short_threshold=40, token=None, channel=None, **kwargs):
    # test import
    import slackclient  # noqa: F401

    cfg = Config.instance()

    if not token:
        token = cfg.get_expanded("notifications", "slack_token")
    if not channel:
        channel = cfg.get_expanded("notifications", "slack_channel")

    if not token or not channel:
        logger.warning("cannot send slack notification, token ({}) or channel ({}) empty".format(
            token, channel))
        return False

    request = {
        "channel": channel,
        "text": title,
        "attachments": {
            "color": attachment_color,
            "fields": [],
        },
        "as_user": True,
        "parse": "full",
    }

    if attachment_fallback:
        request["attachments"]["fallback"] = attachment_fallback

    for key, value in content.items():
        request["attachments"]["fields"].append({
            "title": key,
            "value": value,
            "short": len(value) <= short_threshold,
        })
        request["attachments"]["fallback"] += "_{}_: {}\n\n".format(key, value)

    thread = threading.Thread(target=_notify_slack, args=(token, request))
    thread.start()

    return True


def _notify_slack(token, request):
    import os
    import json
    import traceback

    import six
    import slackclient

    try:
        # token might be a file
        if os.path.isfile(token):
            with open(token, "r") as f:
                token = f.read().strip()

        if not isinstance(request["attachments"], six.string_types):
            request["attachments"] = json.dumps([request["attachments"]])

        sc = slackclient.SlackClient(token)
        res = sc.api_call("chat.postMessage", **request)

        if not res["ok"]:
            logger.warning("unsuccessful Slack API call: {}".format(res))
    except Exception as e:
        t = traceback.format_exc()
        logger.warning("could not send Slack notification: {}\n{}".format(e, t))
