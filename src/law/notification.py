# coding: utf-8

"""
Notification functions.
"""

from __future__ import annotations

__all__ = ["notify_mail", "notify_custom"]

import importlib

from law.config import Config
from law.util import send_mail, uncolored
from law.logger import get_logger
from law._types import Any, Callable


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

    # get config recipient and sender values from config
    if not recipient:
        recipient = cfg.get_expanded("notifications", "mail_recipient")
    if not sender:
        sender = cfg.get_expanded("notifications", "mail_sender")
    if not recipient or not sender:
        logger.warning(
            f"cannot send mail notification, recipient ({recipient}) or sender ({sender}) empty",
        )
        return False

    # get host and port
    if not smtp_port:
        smtp_port = cfg.get_expanded("notifications", "mail_smtp_port")
    if not smtp_host:
        smtp_host = cfg.get_expanded("notifications", "mail_smtp_host")
    # infer host from sender when not set
    if not smtp_host:
        smtp_host = sender.split("@", 1)[1]

    mail_kwargs: dict[str, str | int] = {}
    if smtp_host:
        mail_kwargs["smtp_host"] = smtp_host
    if smtp_port:
        mail_kwargs["smtp_port"] = smtp_port

    return send_mail(
        recipient=recipient,
        sender=sender,
        subject=title,
        content=uncolored(message),
        **mail_kwargs,  # type: ignore[arg-type]
    )


def notify_custom(
    title: str,
    content: dict[str, Any],
    notify_func: Callable[[str, dict[str, Any]], Any] | str | None = None,
    **kwargs,
) -> bool:
    """
    Sends a notification with *title* and *content* using a custom *notify_func*. When *notify_func*
    is empty, the configuration value "custom_func" in the [notifications] section is used. When it
    is a string (which it will be when obtained from the config), it should have the format
    ``"module.id.func"``. The function is then imported and called with the *title* and *message*.
    *True* is returned when the notification was sent successfully, *False* otherwise.
    """
    # prepare the notify function
    if not notify_func:
        cfg = Config.instance()
        notify_func = cfg.get_expanded("notifications", "custom_func", default=None)
    if not notify_func:
        logger.warning("cannot send custom notification, notify_func empty")
        return False
    if isinstance(notify_func, str):
        try:
            module_id, func_name = notify_func.rsplit(".", 1)
        except ValueError:
            logger.warning(
                f"cannot send custom notification, notify_func '{notify_func}' has invalid format",
            )
            return False
        try:
            notify_module = importlib.import_module(module_id)
        except ImportError:
            logger.warning(f"cannot send custom notification, module '{module_id}' not found")
            return False
        notify_func = getattr(notify_module, func_name, None)
        if not notify_func:
            logger.warning(
                f"cannot send custom notification, notify_func '{notify_func}' not found",
            )
            return False
    if not callable(notify_func):
        logger.warning(f"cannot send custom notification, notify_func '{notify_func}' not callable")
        return False

    # invoke it
    try:
        notify_func(title, content, **kwargs)
    except TypeError:
        logger.warning(
            f"cannot send custom notification, notify_func '{notify_func}' has invalid signature",
        )
        return False

    return True
