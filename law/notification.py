# coding: utf-8

"""
Notification functions.
"""


__all__ = ["notify_mail"]


import logging

from law.config import Config
from law.util import send_mail


logger = logging.getLogger(__name__)


def notify_mail(title, message, recipient=None, sender=None, smtp_host=None, smtp_port=None,
        **kwargs):
    """
    Mail notification method taking a *title* and a string *message*. *recipient*, *sender*,
    *smtp_host* and *smtp_port* default to the configuration values in the [notifications] section.
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
        logger.warning("cannot send mail notification, recipient ({}) or sender ({}) empty".format(
            recipient, sender))
        return False

    mail_kwargs = {}
    if smtp_host:
        mail_kwargs["smtp_host"] = smtp_host
    if smtp_port:
        mail_kwargs["smtp_port"] = smtp_port

    return send_mail(recipient, sender, title, message, **mail_kwargs)
