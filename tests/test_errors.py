from __future__ import annotations

import pickle

import pytest

import law
from law.errors import (
    ConfigError,
    FormatterNotFoundError,
    JobError,
    JobsFailedError,
    LawError,
    SandboxError,
)
from law.target.remote.interface import RetryException


class TestErrors:

    def test_hierarchy(self) -> None:
        for cls in [ConfigError, FormatterNotFoundError, SandboxError, JobError, JobsFailedError, RetryException]:
            assert issubclass(cls, LawError)
        # built-in base types for compatibility
        assert issubclass(FormatterNotFoundError, LookupError)
        assert issubclass(SandboxError, RuntimeError)
        assert issubclass(JobError, RuntimeError)
        assert issubclass(JobsFailedError, JobError)

    def test_exports(self) -> None:
        names = ["LawError", "ConfigError", "FormatterNotFoundError", "SandboxError", "JobError", "JobsFailedError"]
        for name in names:
            assert name in law.__all__
            assert getattr(law, name) is getattr(law.errors, name)

    def test_attributes(self) -> None:
        assert SandboxError("msg").exit_code is None
        assert SandboxError("msg", exit_code=2).exit_code == 2
        assert JobError("msg", exit_code=1).exit_code == 1
        assert JobsFailedError("msg").job_nums == []
        assert JobsFailedError("msg", job_nums={3, 1, 2}).job_nums == [1, 2, 3]
        assert str(JobsFailedError("tolerance exceeded", job_nums=[1])) == "tolerance exceeded"

    def test_pickle(self) -> None:
        err = pickle.loads(pickle.dumps(SandboxError("boom", exit_code=3)))
        assert isinstance(err, SandboxError)
        assert err.args == ("boom",)
        assert err.exit_code == 3
        err = pickle.loads(pickle.dumps(JobsFailedError("failed", job_nums=[2, 1])))
        assert err.job_nums == [1, 2]

    def test_catch_as_law_error(self) -> None:
        with pytest.raises(LawError):
            law.target.formatter.get_formatter("law_test_not_existing")
