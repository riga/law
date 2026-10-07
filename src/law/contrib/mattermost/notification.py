"""
Mattermost notifications.
"""

from __future__ import annotations

__all__ = ["notify_mattermost"]

import threading
import traceback

from law._types import Any
from law.config import Config
from law.logger import get_logger
from law.util import escape_markdown

logger = get_logger(__name__)


def notify_mattermost(
    title: str,
    content: str | dict[str, Any],
    hook_url: str | None = None,
    channel: str | None = None,
    user: str | None = None,
    mention_user: str | None = None,
    icon_url: str | None = None,
    icon_emoji: str | None = None,
    **kwargs,
) -> bool:
    """
    Sends a mattermost notification. The communication with the mattermost API might have some delays and is therefore
    handled by a thread.

    :param title: The title.
    :param content: The content. If it is a string, a simple text notification is sent. Otherwise, it should be a
        dictionary whose fields are formatted as key-value pairs.
    :param hook_url: The webhook url, defaulting to the ``mattermost_hook_url`` option in the ``[notifications]``
        section.
    :param channel: The channel, defaulting to the ``mattermost_channel`` option in the ``[notifications]`` section.
    :param user: The user name to post as, defaulting to the ``mattermost_user`` option in the ``[notifications]``
        section.
    :param mention_user: The user to mention, defaulting to the ``mattermost_mention_user`` option in the
        ``[notifications]`` section.
    :param icon_url: The url of the icon, defaulting to the ``mattermost_icon_url`` option in the ``[notifications]``
        section.
    :param icon_emoji: The icon emoji, defaulting to the ``mattermost_icon_emoji`` option in the ``[notifications]``
        section.
    :param kwargs: Additional fields that are added to the request data.
    :return: Whether the notification was sent, i.e., whether *hook_url* is set.
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
        logger.warning(f"cannot send Mattermost notification, hook_url ({hook_url}) empty")
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
    request_data["text"] = f"{title}{mention_text}\n\n"
    if isinstance(content, str):
        request_data["text"] += content
    else:
        for k, v in content.items():
            request_data["text"] += f"{k}: {v}\n"

    # extend by arbitrary kwargs
    request_data.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_mattermost, args=(hook_url, request_data))
    thread.start()

    return True


def _notify_mattermost(hook_url: str, request_data: dict[str, Any]) -> None:
    import requests

    try:
        res = requests.post(hook_url, json=request_data)
        if not res.ok:
            logger.warning(f"unsuccessful Mattermost API call: {res.text}")
    except Exception as e:
        t = traceback.format_exc()
        logger.warning(f"could not send Mattermost notification: {e}\n{t}")
