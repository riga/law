"""
Custom exception classes.

All exceptions inherit from :py:class:`LawError`, so that errors raised by law can be caught in one place. Where
appropriate, they additionally inherit from a matching built-in exception type, so that code catching the built-in type
keeps working.

Exceptions defined here are part of the core API or are raised by more than one package (e.g. :py:class:`JobError` by
remote workflows and several contrib job managers). Exceptions that are specific to a single package, in particular to a
contrib package, should be defined in that package instead, inheriting from :py:class:`LawError` or one of its
subclasses. This module must not import other law modules (except for ``law._types``) so that it can be imported
anywhere without causing import cycles.
"""

from __future__ import annotations

__all__ = [
    "ConfigError",
    "FormatterNotFoundError",
    "JobError",
    "JobsFailedError",
    "LawError",
    "SandboxError",
]

from law._types import Iterable


class LawError(Exception):
    """
    Base class of all custom exceptions raised by law.

    Exceptions that are specific to a single package (e.g. a contrib package) should be defined in that package and
    inherit from this class (or one of its subclasses), whereas exceptions shared by multiple packages belong to this
    module.
    """


class ConfigError(LawError):
    """
    Raised when a required configuration (e.g. a section or option of the law config) is missing or invalid.
    """


class FormatterNotFoundError(LawError, LookupError):
    """
    Raised when no target formatter could be found, either by name or by the file path to load or dump.
    """


class SandboxError(LawError, RuntimeError):
    """
    Raised when setting up or running a sandbox fails. When the failure is caused by a process that exited with a
    non-zero code, it is stored in :py:attr:`exit_code`.

    .. py:attribute:: exit_code

        type: int, None

        The exit code of the failed process, or *None* if not applicable.
    """

    def __init__(self, msg: str = "", *, exit_code: int | None = None) -> None:
        super().__init__(msg)

        self.exit_code = exit_code


class JobError(LawError, RuntimeError):
    """
    Raised when managing jobs on a remote batch system fails, e.g. during submission, cancellation or status queries, or
    when polling the status of jobs fails. When the failure is caused by a command that exited with a non-zero code, it
    is stored in :py:attr:`exit_code`.

    .. py:attribute:: exit_code

        type: int, None

        The exit code of the failed command, or *None* if not applicable.
    """

    def __init__(self, msg: str = "", *, exit_code: int | None = None) -> None:
        super().__init__(msg)

        self.exit_code = exit_code


class JobsFailedError(JobError):
    """
    Raised by remote workflows when too many jobs failed, i.e., when the tolerance is exceeded or the acceptance can no
    longer be reached. The numbers of the failed jobs are stored in :py:attr:`job_nums`.

    .. py:attribute:: job_nums

        type: list

        The sorted numbers of the failed jobs.
    """

    def __init__(self, msg: str = "", *, job_nums: Iterable[int] | None = None) -> None:
        super().__init__(msg)

        self.job_nums = sorted(job_nums or [])
