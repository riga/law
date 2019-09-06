# coding: utf-8

"""
Telegram notifications.
"""


__all__ = ["notify_telegram"]


import threading
import logging

import six

from law.config import Config


logger = logging.getLogger(__name__)


def notify_telegram(title, content, token=None, chat=None, mention_user=None, **kwargs):
    """
    Sends a telegram notification and returns *True* on success. The communication with the telegram
    API might have some delays and is therefore handled by a thread.
    """
    # test import
    import telegram  # noqa: F401

    cfg = Config.instance()

    # get default token and chat
    if not token:
        token = cfg.get_expanded("notifications", "telegram_token")
    if not chat:
        chat = cfg.get_expanded("notifications", "telegram_chat")

    if not token or not chat:
        logger.warning("cannot send Telegram notification, token ({}) or chat ({}) empty".format(
            token, chat))
        return False

    # append the user to mention to the title
    # unless explicitly set to empty string
    mention_text = ""
    if mention_user is None:
        mention_user = cfg.get_expanded("notifications", "telegram_mention_user")
    if mention_user:
        mention_text = " (@{})".format(mention_user)

    # request data for the API call
    request = {
        "parse_mode": "Markdown",
    }

    # standard or attachment content?
    if isinstance(content, six.string_types):
        request["text"] = "{}{}\n\n{}".format(title, mention_text, content)
    else:
        # content is a dict, add some formatting
        request["text"] = "{}{}\n\n".format(title, mention_text)

        for key, value in content.items():
            request["text"] += "_{}_: {}\n".format(key, value)

    # extend by arbitrary kwargs
    request.update(kwargs)

    # threaded, non-blocking API communication
    thread = threading.Thread(target=_notify_telegram, args=(token, chat, request))
    thread.start()

    return True


def _notify_telegram(token, chat, request):
    import os
    import traceback

    import telegram

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
        logger.warning("could not send Telegram notification: {}\n{}".format(e, t))
