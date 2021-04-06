# coding: utf-8

"""
Telegram-related parameters.
"""

__all__ = ["NotifyTelegramParameter"]


import collections

import six

from law.parameter import NotifyParameter
from law.util import escape_markdown
from law.contrib.telegram.notification import notify_telegram


class NotifyTelegramParameter(NotifyParameter):

    def __init__(self, *args, **kwargs):
        super(NotifyTelegramParameter, self).__init__(*args, **kwargs)

        if not self.description:
            self.description = "when true, and the task's run method is decorated with " \
                "law.decorator.notify, a Telegram notification is sent once the task finishes"

    @staticmethod
    def notify(success, title, content, **kwargs):
        # escape the full content
        content = content.__class__(
            (k, escape_markdown(v) if isinstance(v, six.string_types) else v)
            for k, v in content.items()
        )

        # overwrite title with telegram markdown markup
        title = "*Notification from* `{}`".format(content["Task"])
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = "```{}```".format(content["Traceback"])

        # prepend the status text to the message content
        # emojis are "party popper" and "exclamation mark"
        parts = list(content.items())
        status_text = "success \xF0\x9F\x8E\x89" if success else "failure \xE2\x9D\x97"
        parts.insert(0, ("Status", status_text))
        content = collections.OrderedDict(parts)

        # send the notification
        return notify_telegram(title, content, **kwargs)

    def get_transport(self):
        return {
            "func": self.notify,
            "raw": True,
            "colored": False,
        }
