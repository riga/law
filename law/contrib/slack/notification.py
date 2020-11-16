# coding: utf-8

"""
Slack notifications.
"""


__all__ = ["notify_slack"]


import threading
import logging

import six

from law.config import Config


logger = logging.getLogger(__name__)


def notify_slack(title, content, attachment_color="#4bb543", short_threshold=40, token=None,
        channel=None, mention_user=None, **kwargs):
    """
    Sends a slack notification and returns *True* on success. The communication with the slack API
    might have some delays and is therefore handled by a thread. The format of the notification
    depends on *content*. If it is a string, a simple text notification is sent. Otherwise, it
    should be a dictionary whose fields are used to build a message attachment with two-column
    formatting.
    """
    # test import
    import_slack()

    cfg = Config.instance()

    # get default token and channel
    if not token:
        token = cfg.get_expanded("notifications", "slack_token")
    if not channel:
        channel = cfg.get_expanded("notifications", "slack_channel")

    if not token or not channel:
        logger.warning("cannot send Slack notification, token ({}) or channel ({}) empty".format(
            token, channel))
        return False

    # append the user to mention to the title
    # unless explicitly set to empty string
    mention_text = ""
    if mention_user is None:
        mention_user = cfg.get_expanded("notifications", "slack_mention_user")
    if mention_user:
        mention_text = " (@{})".format(mention_user)

    # request data for the API call
    request = {
        "channel": channel,
        "as_user": True,
        "parse": "full",
    }

    # standard or attachment content?
    if isinstance(content, six.string_types):
        request["text"] = "{}{}\n\n{}".format(title, mention_text, content)
    else:
        # content is a dict, send its data as an attachment
        request["text"] = "{} {}".format(title, mention_text)
        request["attachments"] = at = {
            "color": attachment_color,
            "fields": [],
            "fallback": "{}{}\n\n".format(title, mention_text),
        }

        # fill the attachment fields and extend the fallback
        for key, value in content.items():
            at["fields"].append({
                "title": key,
                "value": value,
                "short": len(value) <= short_threshold,
            })
            at["fallback"] += "_{}_: {}\n".format(key, value)

    # extend by arbitrary kwargs
    request.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_slack, args=(token, request))
    thread.start()

    return True


def _notify_slack(token, request):
    import os
    import json
    import traceback

    slack, vslack = import_slack()

    try:
        # token might be a file
        token_file = os.path.expanduser(os.path.expandvars(token))
        if os.path.isfile(token_file):
            with open(token_file, "r") as f:
                token = f.read().strip()

        if "attachments" in request and not isinstance(request["attachments"], six.string_types):
            request["attachments"] = json.dumps([request["attachments"]])

        if vslack == 1:
            sc = slack.SlackClient(token)
            res = sc.api_call("chat.postMessage", **request)
        else:  # 2
            wc = slack.WebClient(token)
            res = wc.chat_postMessage(**request)

        if not res["ok"]:
            logger.warning("unsuccessful Slack API call: {}".format(res))
    except Exception as e:
        t = traceback.format_exc()
        logger.warning("could not send Slack notification: {}\n{}".format(e, t))


def import_slack():
    try:
        # slackclient 1.x
        import slackclient  # noqa: F401
        return slackclient, 1
    except ImportError:
        try:
            # slackclient 2.x
            import slack  # noqa: F401
            return slack, 2
        except ImportError as e:
            e.msg = "neither module 'slackclient' nor 'slack' found, " \
                "run 'pip install slackclient' to install them"
            raise
