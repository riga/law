# coding: utf-8
# flake8: noqa

"""
Slack contrib functionality.
"""


__all__ = ["notify_slack", "NotifySlackParameter"]


# provisioning imports
from law.contrib.slack.notification import notify_slack
from law.contrib.slack.parameter import NotifySlackParameter
