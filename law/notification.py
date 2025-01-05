# coding: utf-8

"""
Notification functions.
"""

__all__ = ["notify_mail", "notify_custom"]


import importlib

from law.config import Config
from law.util import send_mail, uncolored
from law.logger import get_logger


logger = get_logger(__name__)


def notify_mail(title, message, recipient=None, sender=None, smtp_host=None, smtp_port=None,
        **kwargs):
    """
    Sends a notification mail with a *title* and a string *message*. *recipient*, *sender*,
    *smtp_host* and *smtp_port* default to the configuration values in the [notifications] section.
    When *recipient* or *sender* are not set, a warning is issued and *False* is returned.
    Otherwise, the result of :py:meth:`util.send_mail` is returned.
    """
    cfg = Config.instance()

    # get config recipient and sender values from config
    if not recipient:
        recipient = cfg.get_expanded("notifications", "mail_recipient", default=None)
    if not sender:
        sender = cfg.get_expanded("notifications", "mail_sender", default=None)
    if not recipient or not sender:
        logger.warning("cannot send mail notification, recipient ({}) or sender ({}) empty".format(
            recipient, sender))
        return False

    if not smtp_port:
        smtp_port = cfg.get_expanded("notifications", "mail_smtp_port", default=None)
    if not smtp_host:
        smtp_host = cfg.get_expanded("notifications", "mail_smtp_host", default=None)
    # infer host from sender when not set
    if not smtp_host and "@" in sender:
        smtp_host = sender.split("@", 1)[1]

    mail_kwargs = {}
    if smtp_host:
        mail_kwargs["smtp_host"] = smtp_host
    if smtp_port:
        mail_kwargs["smtp_port"] = smtp_port

    return send_mail(recipient, sender, title, uncolored(message), **mail_kwargs)


def notify_custom(title, content, notify_func=None, **kwargs):
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
            logger.warning("cannot send custom notification, notify_func '{}' has invalid "
                "format".format(notify_func))
            return False
        try:
            notify_module = importlib.import_module(module_id)
        except ImportError:
            logger.warning("cannot send custom notification, module '{}' not found".format(
                module_id))
            return False
        notify_func = getattr(notify_module, func_name, None)
        if not notify_func:
            logger.warning("cannot send custom notification, notify_func '{}' not found".format(
                notify_func))
            return False
    if not callable(notify_func):
        logger.warning("cannot send custom notification, notify_func '{}' not callable".format(
            notify_func))
        return False

    # invoke it
    try:
        notify_func(title, content, **kwargs)
    except TypeError:
        logger.warning("cannot send custom notification, notify_func '{}' has invalid "
            "signature".format(notify_func))
        return False

    return True
