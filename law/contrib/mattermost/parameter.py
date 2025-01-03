# coding: utf-8

"""
Mattermost related parameters.
"""

from __future__ import annotations

__all__ = ["NotifyMattermostParameter"]

from collections import OrderedDict

from law.config import Config
from law.parameter import NotifyParameter
from law.contrib.mattermost.notification import notify_mattermost
from law._types import Any


class NotifyMattermostParameter(NotifyParameter):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.description: str
        if not self.description:
            self.description = (
                "when true, and the task's run method is decorated with law.decorator.notify, "
                "a Mattermost notification is sent once the task finishes"
            )

    @classmethod
    def notify(cls, success: bool, title: str, content: dict[str, Any], **kwargs) -> bool:
        content = OrderedDict(content)

        # overwrite title
        cfg = Config.instance()
        header = cfg.get_expanded("notifications", "mattermost_header", default=None)
        task_block = f"```\n{content['Task']}\n```"
        title = f"{header}\n{task_block}" if header else task_block
        del content["Task"]

        # markup for traceback
        if "Traceback" in content:
            content["Traceback"] = f"\n```\n{content['Traceback']}\n```"

        # prepend the status text to the message content
        cfg = Config.instance()
        status_text = "success" if success else "failure"
        status_emoji = cfg.get_expanded(
            "notifications",
            f"mattermost_{status_text}_emoji",
            default=None,
        )
        if status_emoji:
            status_text += " " + status_emoji
        content["Status"] = status_text
        content.move_to_end("Status", last=False)

        # highlight last message
        if "Last message" in content:
            content["Last message"] = f"`{content['Last Message']}`"

        # highlight keys
        content = content.__class__((f"**{k}**", v) for k, v in content.items())

        # send the notification
        return notify_mattermost(title, content, **kwargs)

    def get_transport(self) -> dict[str, Any]:
        return {
            "func": self.notify,
            "raw": True,
            "colored": False,
        }
