# coding: utf-8

"""
Mattermost notifications.
"""

__all__ = ["notify_mattermost"]

import threading

from law.config import Config
from law.util import escape_markdown
from law.logger import get_logger


logger = get_logger(__name__)


def notify_mattermost(
    title,
    content,
    hook_url=None,
    channel=None,
    user=None,
    mention_user=None,
    icon_url=None,
    icon_emoji=None,
    **kwargs,
):
    """
    Sends a mattermost notification and returns *True* on success. The communication with the
    mattermost API might have some delays and is therefore handled by a thread. The format of the
    notification depends on *content*. If it is a string, a simple text notification is sent.
    Otherwise, it should be a dictionary whose fields are formatted as key-value pairs.
    """
    cfg = Config.instance()

    # get default settings
    if not hook_url:
        hook_url = cfg.get_expanded("notifications", "mattermost_hook_url", default=None)
    if not channel:
        channel = cfg.get_expanded("notifications", "mattermost_channel", default=None)
    if not user:
        user = cfg.get_expanded("notifications", "mattermost_user", default=None)
    if not mention_user:
        mention_user = cfg.get_expanded("notifications", "mattermost_mention_user", default=None)
    if not icon_url:
        icon_url = cfg.get_expanded("notifications", "mattermost_icon_url", default=None)
    if not icon_emoji:
        icon_emoji = cfg.get_expanded("notifications", "mattermost_icon_emoji", default=None)

    if not hook_url:
        logger.warning("cannot send Mattermost notification, hook_url ({}) empty".format(hook_url))
        return False

    # append the user to mention to the title
    # unless explicitly set to empty string
    mention_text = ""
    if mention_user:
        mention_text = " (@{})".format(escape_markdown(mention_user.lstrip("@")))

    # request data for the API call
    request_data = {}
    if channel:
        request_data["channel"] = channel
    if user:
        request_data["username"] = user
    if icon_url:
        request_data["icon_url"] = icon_url
    if icon_emoji:
        request_data["icon_emoji"] = icon_emoji

    # standard or attachment content?
    request_data["text"] = "{}{}\n\n".format(title, mention_text)
    if isinstance(content, str):
        request_data["text"] += content
    else:
        for k, v in content.items():
            request_data["text"] += "{}: {}\n".format(k, v)

    # extend by arbitrary kwargs
    request_data.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_mattermost, args=(hook_url, request_data))
    thread.start()

    return True


def _notify_mattermost(hook_url, request_data):
    import traceback
    import requests

    try:
        res = requests.post(hook_url, json=request_data)
        if not res.ok:
            logger.warning("unsuccessful Mattermost API call: {}".format(res.text))
    except Exception as e:
        t = traceback.format_exc()
        logger.warning("could not send Mattermost notification: {}\n{}".format(e, t))
