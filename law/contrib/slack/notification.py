# coding: utf-8

"""
Slack notifications.
"""

from __future__ import annotations

__all__ = ["notify_slack"]

import os
import json
import pathlib
import threading
import traceback

from law.config import Config
from law.util import escape_markdown
from law.logger import get_logger
from law._types import Any, ModuleType

logger = get_logger(__name__)


def notify_slack(
    title: str,
    content: str | dict[str, str],
    attachment_color: str = "#4bb543",
    short_threshold: int = 40,
    token: str | pathlib.Path | None = None,
    channel: str | None = None,
    mention_user: str | None = None,
    **kwargs,
) -> bool:
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
        logger.warning(
            f"cannot send Slack notification, token ({token}) or channel ({channel}) empty",
        )
        return False

    # append the user to mention to the title
    # unless explicitly set to empty string
    mention_text = ""
    if mention_user is None:
        mention_user = cfg.get_expanded("notifications", "slack_mention_user")
    if mention_user:
        mention_text = f" (@{escape_markdown(mention_user)})"

    # request data for the API call
    request = {
        "channel": channel,
        "as_user": True,
        "parse": "full",
    }

    # standard or attachment content?
    if isinstance(content, str):
        request["text"] = f"{title}{mention_text}\n\n{content}"
    else:
        # content is a dict, send its data as an attachment
        request["text"] = f"{title} {mention_text}"
        request["attachments"] = at = {
            "color": attachment_color,
            "fields": [],
            "fallback": f"{title}{mention_text}\n\n",
        }

        # fill the attachment fields and extend the fallback
        for key, value in content.items():
            at["fields"].append({  # type: ignore[attr-defined]
                "title": key,
                "value": value,
                "short": len(value) <= short_threshold,
            })
            at["fallback"] += f"_{key}_: {value}\n"  # type: ignore[operator]

    # extend by arbitrary kwargs
    request.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_slack, args=(token, request))
    thread.start()

    return True


def _notify_slack(token: str | pathlib.Path, request: dict[str, Any]) -> None:
    slack, vslack = import_slack()

    try:
        # token might be a file
        token_file = os.path.expanduser(os.path.expandvars(token))
        if os.path.isfile(token_file):
            with open(token_file, "r") as f:
                token = f.read().strip()

        if "attachments" in request and not isinstance(request["attachments"], str):
            request["attachments"] = json.dumps([request["attachments"]])

        if vslack == 1:
            sc = slack.SlackClient(token)
            res = sc.api_call("chat.postMessage", **request)
        else:  # 2
            wc = slack.WebClient(token)
            res = wc.chat_postMessage(**request)

        if not res["ok"]:
            logger.warning(f"unsuccessful Slack API call: {res}")
    except Exception as e:
        t = traceback.format_exc()
        logger.warning(f"could not send Slack notification: {e}\n{t}")


def import_slack() -> tuple[ModuleType, int]:
    try:
        # slackclient 1.x
        import slackclient  # type: ignore[import-untyped, import-not-found] # noqa
        return slackclient, 1
    except ImportError:
        try:
            # slackclient 2.x
            import slack  # type: ignore[import-untyped, import-not-found] # noqa
            return slack, 2
        except ImportError as e:
            e.args = (
                "neither module 'slackclient' nor 'slack' found, run 'pip install slackclient' to "
                "install them",
            )
            raise e
