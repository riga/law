# coding: utf-8
# flake8: noqa

"""
Mattermost contrib functionality.
"""

__all__ = ["notify_mattermost", "NotifyMattermostParameter"]

# provisioning imports
from law.contrib.mattermost.notification import notify_mattermost
from law.contrib.mattermost.parameter import NotifyMattermostParameter
