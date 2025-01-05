# coding: utf-8

"""
Telegram-related parameters.
"""

from __future__ import annotations

__all__ = ["NotifyTelegramParameter"]

from law.parameter import NotifyParameter
from law.util import escape_markdown
from law.contrib.telegram.notification import notify_telegram
from law._types import Any


class NotifyTelegramParameter(NotifyParameter):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.description: str | None
        if not self.description:
            self.description = (
                "when true, and the task's run method is decorated with "
                "law.decorator.notify, a Telegram notification is sent once the task finishes"
            )

    def get_transport(self) -> dict[str, Any]:
        return {
            "func": self.notify,
            "raw": True,
            "colored": False,
        }

    @staticmethod
    def notify(success: bool, title: str, content: dict[str, str], **kwargs) -> bool:
        # escape the full content
        content = content.__class__(
            (k, (escape_markdown(v) if isinstance(v, str) else v))
            for k, v in content.items()
        )

        # overwrite title with telegram markdown markup
        title = f"*Notification from* `{content['Task']}`"
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = f"```{content['Traceback']}```"

        # prepend the status text to the message content
        # emojis are "party popper" and "exclamation mark"
        parts = list(content.items())
        status_text = "success \xF0\x9F\x8E\x89" if success else "failure \xE2\x9D\x97"
        parts.insert(0, ("Status", status_text))
        content = dict(parts)

        # send the notification
        return notify_telegram(title, content, **kwargs)
