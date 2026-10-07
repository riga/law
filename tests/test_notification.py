from __future__ import annotations

__all__ = ["TestNotification"]

from unittest import mock

import pytest

import law
from law.notification import notify_custom, notify_mail
from law.parameter import NotifyMailParameter

# custom notification function that is imported via its module path in tests below
_custom_notifications: list[tuple] = []


def custom_notify_func(title, content, **kwargs):
    _custom_notifications.append((title, content, kwargs))


class TestNotification:

    @pytest.fixture(autouse=True)
    def reset_notifications(self) -> None:
        _custom_notifications.clear()

    def test_notify_mail_missing_addresses(self) -> None:
        with mock.patch("law.notification.send_mail") as send_mail:
            assert notify_mail("title", "message") is False
            assert notify_mail("title", "message", recipient="a@b.c") is False
            assert notify_mail("title", "message", sender="a@b.c") is False
            send_mail.assert_not_called()

    def test_notify_mail(self) -> None:
        with mock.patch("law.notification.send_mail", return_value=True) as send_mail:
            message = law.util.colored("message", "red", force=True)
            assert notify_mail("title", message, recipient="to@x.y", sender="from@host.tld")
        send_mail.assert_called_once()
        kwargs = send_mail.call_args.kwargs
        assert kwargs["recipient"] == "to@x.y"
        assert kwargs["sender"] == "from@host.tld"
        assert kwargs["subject"] == "title"
        # colors are removed
        assert kwargs["content"] == "message"
        # the smtp host is inferred from the sender
        assert kwargs["smtp_host"] == "host.tld"

        with mock.patch("law.notification.send_mail", return_value=True) as send_mail:
            notify_mail("t", "m", recipient="to@x.y", sender="from@host.tld", smtp_host="smtp.x", smtp_port=587)
        assert send_mail.call_args.kwargs["smtp_host"] == "smtp.x"
        assert send_mail.call_args.kwargs["smtp_port"] == 587

    def test_notify_mail_sender_without_host(self) -> None:
        with mock.patch("law.notification.send_mail", return_value=True):
            notify_mail("title", "message", recipient="to@x.y", sender="nobody")

    def test_notify_mail_parameter(self) -> None:
        title, message = NotifyMailParameter.format_message(False, "title", {"Task": "t", "Traceback": "tb"})
        assert title == "title"
        lines = message.split("\n")
        assert lines[0] == "**Status**: failure"
        assert lines[1] == "**Task**: t"
        assert "```" in message

    def test_notify_custom_callable(self) -> None:
        assert notify_custom("title", {"a": 1}, notify_func=custom_notify_func, extra=True)
        assert _custom_notifications == [("title", {"a": 1}, {"extra": True})]

    def test_notify_custom_string(self) -> None:
        assert notify_custom("title", {}, notify_func=f"{__name__}.custom_notify_func")
        assert len(_custom_notifications) == 1

    def test_notify_custom_failures(self) -> None:
        # no function configured
        assert notify_custom("title", {}) is False
        # invalid format
        assert notify_custom("title", {}, notify_func="no_module_path") is False
        # module not found
        assert notify_custom("title", {}, notify_func="not_existing_module_42.func") is False
        # function not found
        assert notify_custom("title", {}, notify_func=f"{__name__}.not_existing") is False
        # not callable
        assert notify_custom("title", {}, notify_func=f"{__name__}.__all__") is False

        # invalid signature
        def func():
            pass

        assert notify_custom("title", {}, notify_func=func) is False  # type: ignore[arg-type]

    def test_notify_custom_missing_func_message(self) -> None:
        with mock.patch("law.notification.logger") as logger:
            notify_custom("title", {}, notify_func=f"{__name__}.not_existing")
        assert f"{__name__}.not_existing" in logger.warning.call_args.args[0]
