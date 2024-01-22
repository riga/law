# coding: utf-8

"""
Notification functions.
"""

from __future__ import annotations

__all__ = ["notify_mail"]

from law.config import Config
from law.util import send_mail, uncolored
from law.logger import get_logger


logger = get_logger(__name__)


def notify_mail(
    title: str,
    message: str,
    recipient: str | None = None,
    sender: str | None = None,
    smtp_host: str | None = None,
    smtp_port: int | None = None,
    **kwargs,
) -> bool:
    """
    Sends a notification mail with a *title* and a string *message*. *recipient*, *sender*,
    *smtp_host* and *smtp_port* default to the configuration values in the [notifications] section.
    When *recipient* or *sender* are not set, a warning is issued and *False* is returned.
    Otherwise, the result of :py:meth:`util.send_mail` is returned.
    """
    cfg = Config.instance()

    if not recipient:
        recipient = cfg.get_expanded("notifications", "mail_recipient")
    if not sender:
        sender = cfg.get_expanded("notifications", "mail_sender")
    if not smtp_host:
        smtp_host = cfg.get_expanded("notifications", "mail_smtp_host")
    if not smtp_port:
        smtp_port = cfg.get_expanded("notifications", "mail_smtp_port")

    if not recipient or not sender:
        logger.warning(
            f"cannot send mail notification, recipient ({recipient}) or sender ({sender}) empty",
        )
        return False

    mail_kwargs: dict[str, str | int] = {}
    if smtp_host:
        mail_kwargs["smtp_host"] = smtp_host
    if smtp_port:
        mail_kwargs["smtp_port"] = smtp_port

    return send_mail(recipient, sender, title, uncolored(message), **mail_kwargs)
