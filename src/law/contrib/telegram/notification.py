# coding: utf-8

"""
Telegram notifications.
"""

from __future__ import annotations

__all__ = ["notify_telegram"]

import os
import threading
import traceback
import pathlib

from law.config import Config
from law.util import escape_markdown
from law.logger import get_logger
from law._types import Any


logger = get_logger(__name__)


def notify_telegram(
    title: str,
    content: str | dict[str, Any],
    token: str | pathlib.Path | None = None,
    chat: str | None = None,
    mention_user: str | None = None,
    **kwargs,
) -> bool:
    """
    Sends a telegram notification and returns *True* on success. The communication with the telegram
    API might have some delays and is therefore handled by a thread.
    """
    # test import
    import telegram  # type: ignore[import-untyped, import-not-found] # noqa: F401

    cfg = Config.instance()

    # get default token and chat
    if not token:
        token = cfg.get_expanded("notifications", "telegram_token")
    if not chat:
        chat = cfg.get_expanded("notifications", "telegram_chat")

    if not token or not chat:
        logger.warning(f"cannot send telegram notification, token ({token}) or chat ({chat}) empty")
        return False

    # append the user to mention to the title
    # unless explicitly set to empty string
    mention_text = ""
    if mention_user is None:
        mention_user = cfg.get_expanded("notifications", "telegram_mention_user")
    if mention_user:
        mention_text = f" (@{escape_markdown(mention_user)})"

    # request data for the API call
    request = {"parse_mode": "MarkdownV2"}

    # standard or attachment content?
    if isinstance(content, dict):
        # content is a dict, add some formatting
        request["text"] = f"{title}{mention_text}\n\n"

        for key, value in content.items():
            request["text"] += f"_{key}_: {value}\n"
    else:
        request["text"] = f"{title}{mention_text}\n\n{content}"

    # extend by arbitrary kwargs
    request.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_telegram, args=(token, chat, request))
    thread.start()

    return True


def _notify_telegram(token: str | pathlib.Path, chat: str, request: dict[str, Any]) -> bool:
    import telegram  # type: ignore[import-untyped, import-not-found] # noqa: F401

    try:
        # token might be a file
        token_file = os.path.expanduser(os.path.expandvars(token))
        if os.path.isfile(token_file):
            with open(token_file, "r") as f:
                token = f.read().strip()

        bot = telegram.Bot(token=token)
        return bot.send_message(chat, **request)

    except Exception as e:
        t = traceback.format_exc()
        logger.warning(f"could not send telegram notification: {e}\n{t}")
        return False
