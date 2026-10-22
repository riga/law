from __future__ import annotations

__all__ = ["TestGFALErrorReasons", "TestGFALParseConfig"]

import pytest

from law._types import Any, Iterator
from law.config import Config
from law.contrib.gfal import target as gfal_target
from law.contrib.gfal.target import (
    GFALError_chmod,
    GFALError_filecopy,
    GFALError_listdir,
    GFALError_mkdir,
    GFALError_rmdir,
    GFALError_unlink,
    GFALFileInterface,
    GFALOperationError,
)
from law.target.remote.interface import RetryException

# example uris per scheme
URIS = {
    "file": "file:///tmp/law_test/file.txt",
    "root": "root://eosuser.cern.ch//eos/user/l/law/file.txt",
    "gsiftp": "gsiftp://gridftp.example.org:2811/pnfs/law/file.txt",
    "srm": "srm://srm.example.org:8443/srm/managerv2?SFN=/pnfs/law/file.txt",
    "dav": "dav://webdav.example.org:2880/pnfs/law/file.txt",
    "davs": "davs://webdav.example.org:2880/pnfs/law/file.txt",
    "dropbox": "dropbox://dropbox.com/law/file.txt",
    "s3": "s3://bucket.example.org/law/file.txt",
}


class GError(Exception):
    """
    Stand-in for gfal2.GError, which is not required to be installed for these tests.
    """


def make_exc(msg: str) -> Any:
    return (GError, GError(msg), None)


class TestGFALErrorReasons:

    @pytest.fixture
    def warnings(self, monkeypatch: pytest.MonkeyPatch) -> list[str]:
        # record warnings of the gfal logger
        msgs: list[str] = []
        monkeypatch.setattr(gfal_target.logger, "warning", msgs.append)
        return msgs

    @pytest.mark.parametrize(("cls", "scheme", "msg", "reason"), [
        # chmod
        (GFALError_chmod, "root", "[ERROR] Server responded with an error: [3011] No such file or directory",
            GFALError_chmod.NOT_FOUND),
        (GFALError_chmod, "gsiftp", "globus_ftp_client: No such file or directory", GFALError_chmod.NOT_FOUND),
        (GFALError_chmod, "srm", "srm-ifce err: No such file or directory", GFALError_chmod.NOT_FOUND),
        (GFALError_chmod, "srm", "srm-ifce err: Operation not supported", GFALError_chmod.NOT_SUPPORTED),
        (GFALError_chmod, "dav", "Protocol not supported", GFALError_chmod.NOT_SUPPORTED),
        (GFALError_chmod, "davs", "Protocol not supported", GFALError_chmod.NOT_SUPPORTED),
        (GFALError_chmod, "dropbox", "Protocol not supported", GFALError_chmod.NOT_SUPPORTED),
        (GFALError_chmod, "root", "Permission denied", GFALError_chmod.UNKNOWN),
        (GFALError_chmod, "gsiftp", "Operation not supported", GFALError_chmod.UNKNOWN),
        # unlink
        (GFALError_unlink, "root", "[3011] No such file or directory", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "root", "[3016] Is a directory", GFALError_unlink.IS_DIRECTORY),
        (GFALError_unlink, "gsiftp", "No such file or directory", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "gsiftp", "Not a file", GFALError_unlink.IS_DIRECTORY),
        (GFALError_unlink, "srm", "No such file", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "srm", "Not a file", GFALError_unlink.IS_DIRECTORY),
        (GFALError_unlink, "dav", "File not found", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "davs", "File not found", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "dav", "Is a directory", GFALError_unlink.IS_DIRECTORY),
        (GFALError_unlink, "davs", "Is a directory", GFALError_unlink.IS_DIRECTORY),
        (GFALError_unlink, "dropbox", "path_lookup/not_found/", GFALError_unlink.NOT_FOUND),
        (GFALError_unlink, "root", "Permission denied", GFALError_unlink.UNKNOWN),
        # rmdir
        (GFALError_rmdir, "root", "[3011] No such file or directory", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "root", "[3017] Not a directory", GFALError_rmdir.IS_FILE),
        (GFALError_rmdir, "root", "[3005] No such device", GFALError_rmdir.NOT_EMPTY),
        (GFALError_rmdir, "gsiftp", "No such file or directory", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "gsiftp", "Not a directory", GFALError_rmdir.IS_FILE),
        (GFALError_rmdir, "gsiftp", "Directory is not empty", GFALError_rmdir.NOT_EMPTY),
        (GFALError_rmdir, "srm", "No such file or directory", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "srm", "This file is not a directory", GFALError_rmdir.IS_FILE),
        (GFALError_rmdir, "srm", "Directory not empty", GFALError_rmdir.NOT_EMPTY),
        (GFALError_rmdir, "dav", "File not found", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "davs", "File not found", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "dropbox", "path_lookup/not_found/", GFALError_rmdir.NOT_FOUND),
        (GFALError_rmdir, "root", "Permission denied", GFALError_rmdir.UNKNOWN),
        # mkdir
        (GFALError_mkdir, "root", "[3018] File exists", GFALError_mkdir.EXISTS),
        (GFALError_mkdir, "gsiftp", "Directory already exists", GFALError_mkdir.EXISTS),
        (GFALError_mkdir, "srm", "Directory already exist", GFALError_mkdir.EXISTS),
        (GFALError_mkdir, "dropbox", "The directory already exists", GFALError_mkdir.EXISTS),
        (GFALError_mkdir, "root", "Permission denied", GFALError_mkdir.UNKNOWN),
        # listdir
        (GFALError_listdir, "root", "[ERROR] Invalid response (unknown error 303)", GFALError_listdir.EMPTY),
        (GFALError_listdir, "root", "[ERROR] Invalid response (unknown error 303)  \n", GFALError_listdir.EMPTY),
        (GFALError_listdir, "dav", "File not found", GFALError_listdir.EMPTY),
        (GFALError_listdir, "davs", "File not found", GFALError_listdir.EMPTY),
        (GFALError_listdir, "root", "Invalid response (unknown error 303) and more", GFALError_listdir.UNKNOWN),
        (GFALError_listdir, "root", "Permission denied", GFALError_listdir.UNKNOWN),
    ])
    def test_reason(
        self,
        cls: type[GFALOperationError],
        scheme: str,
        msg: str,
        reason: str,
        warnings: list[str],
    ) -> None:
        uri = URIS[scheme]
        assert cls._get_reason(msg, uri, scheme) == reason
        # also case-insensitive
        assert cls._get_reason(msg.upper(), uri, scheme) == reason
        assert not warnings

    @pytest.mark.parametrize(("src_scheme", "dst_scheme", "msg", "reason"), [
        ("file", "file", "Could not open source", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "file", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("file", "root", "No such file or directory (source)", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "root", "File exists (destination)", GFALError_filecopy.DST_EXISTS),
        ("file", "gsiftp", "Local system call No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "gsiftp", "File exists", GFALError_filecopy.DST_EXISTS),
        ("file", "srm", "Local system call No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "srm", "File exists", GFALError_filecopy.DST_EXISTS),
        ("file", "dav", "Local system call No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "davs", "Local system call No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "dropbox", "Could not open source", GFALError_filecopy.SRC_NOT_FOUND),
        ("file", "dropbox", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("gsiftp", "file", "No such file or directory on url", GFALError_filecopy.SRC_NOT_FOUND),
        ("gsiftp", "file", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("gsiftp", "gsiftp", "File not found", GFALError_filecopy.SRC_NOT_FOUND),
        ("gsiftp", "gsiftp", "Destination already exist", GFALError_filecopy.DST_EXISTS),
        ("gsiftp", "dav", "Is a directory", GFALError_filecopy.DST_EXISTS),
        ("gsiftp", "davs", "Is a directory", GFALError_filecopy.DST_EXISTS),
        ("srm", "file", "No such file", GFALError_filecopy.SRC_NOT_FOUND),
        ("srm", "file", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("srm", "root", "No such file", GFALError_filecopy.SRC_NOT_FOUND),
        ("srm", "root", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("srm", "srm", "No such file", GFALError_filecopy.SRC_NOT_FOUND),
        ("srm", "srm", "File exists", GFALError_filecopy.DST_EXISTS),
        ("srm", "dav", "No such file", GFALError_filecopy.SRC_NOT_FOUND),
        ("srm", "davs", "File exists", GFALError_filecopy.DST_EXISTS),
        ("root", "file", "No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("root", "file", "File exists (destination)", GFALError_filecopy.DST_EXISTS),
        ("root", "srm", "No such file or directory", GFALError_filecopy.SRC_NOT_FOUND),
        ("root", "srm", "File exists", GFALError_filecopy.DST_EXISTS),
        ("root", "root", "Destination does not support delegation.", GFALError_filecopy.SRC_NOT_FOUND),
        ("root", "root", "File exists", GFALError_filecopy.DST_EXISTS),
        ("root", "dav", "Failed to open file (Block device required)", GFALError_filecopy.DST_EXISTS),
        ("root", "davs", "Failed to open file (Block device required)", GFALError_filecopy.DST_EXISTS),
        ("dav", "file", "File not found", GFALError_filecopy.SRC_NOT_FOUND),
        ("davs", "file", "File not found", GFALError_filecopy.SRC_NOT_FOUND),
        ("dav", "gsiftp", "File not found", GFALError_filecopy.SRC_NOT_FOUND),
        ("davs", "gsiftp", "Not a file", GFALError_filecopy.DST_EXISTS),
        ("dropbox", "file", "Could not open source", GFALError_filecopy.SRC_NOT_FOUND),
        ("dropbox", "file", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("dropbox", "dropbox", "Could not open source", GFALError_filecopy.SRC_NOT_FOUND),
        ("dropbox", "dropbox", "The file exists", GFALError_filecopy.DST_EXISTS),
        ("file", "file", "Permission denied", GFALError_filecopy.UNKNOWN),
        # pairs that are deliberately not parsed
        ("dav", "root", "No such file or directory", GFALError_filecopy.UNKNOWN),
        ("davs", "root", "File exists", GFALError_filecopy.UNKNOWN),
        ("dav", "srm", "No such file or directory", GFALError_filecopy.UNKNOWN),
        ("davs", "srm", "File exists", GFALError_filecopy.UNKNOWN),
    ])
    def test_filecopy_reason(
        self,
        src_scheme: str,
        dst_scheme: str,
        msg: str,
        reason: str,
        warnings: list[str],
    ) -> None:
        src_uri, dst_uri = URIS[src_scheme], URIS[dst_scheme]
        assert GFALError_filecopy._get_reason(msg, src_uri, dst_uri, src_scheme, dst_scheme) == reason
        assert GFALError_filecopy._get_reason(msg.upper(), src_uri, dst_uri, src_scheme, dst_scheme) == reason
        assert not warnings

    @pytest.mark.parametrize("cls", [
        GFALError_chmod, GFALError_unlink, GFALError_rmdir, GFALError_mkdir, GFALError_listdir,
    ])
    def test_unknown_scheme(self, cls: type[GFALOperationError], warnings: list[str]) -> None:
        assert cls._get_reason("No such file or directory", URIS["s3"], "s3") == cls.UNKNOWN
        assert len(warnings) == 1
        assert "'s3'" in warnings[0]
        assert cls.__name__ in warnings[0]

    def test_filecopy_unknown_schemes(self, warnings: list[str]) -> None:
        reason = GFALError_filecopy._get_reason("File exists", URIS["s3"], URIS["root"], "s3", "root")
        assert reason == GFALError_filecopy.UNKNOWN
        assert len(warnings) == 1
        assert "'s3'" in warnings[0]
        assert "'root'" in warnings[0]

    def test_init(self) -> None:
        exc = make_exc("[3011] No such file or directory")
        e = GFALError_unlink(URIS["root"], exc=exc)
        assert isinstance(e, RetryException)
        assert e.uri == URIS["root"]
        assert e.scheme == "root"
        assert e.reason == GFALError_unlink.NOT_FOUND
        assert e.exc_value is exc[1]
        assert str(e) == f"{exc[1]} (GFALError_unlink: {GFALError_unlink.NOT_FOUND})"

    def test_init_without_scheme(self) -> None:
        e = GFALError_listdir("/tmp/law_test", exc=make_exc("Permission denied"))
        assert e.scheme == "file"

    def test_init_from_exc_info(self) -> None:
        orig = GError("[3018] File exists")
        try:
            raise orig
        except GError:
            e = GFALError_mkdir(URIS["root"])
        assert e.reason == GFALError_mkdir.EXISTS
        assert e.exc_type is GError
        assert e.exc_value is orig
        assert e.exc_traceback is not None

    def test_filecopy_init(self) -> None:
        exc = make_exc("No such file or directory (source)")
        e = GFALError_filecopy("/tmp/law_test/file.txt", URIS["root"], exc=exc)
        assert isinstance(e, RetryException)
        assert e.src_uri == "/tmp/law_test/file.txt"
        assert e.dst_uri == URIS["root"]
        assert e.src_scheme == "file"
        assert e.dst_scheme == "root"
        assert e.reason == GFALError_filecopy.SRC_NOT_FOUND
        assert e.exc_value is exc[1]
        assert str(e) == f"{exc[1]} (GFALError_filecopy: {GFALError_filecopy.SRC_NOT_FOUND})"

    def test_filecopy_init_from_exc_info(self) -> None:
        orig = GError("File exists")
        try:
            raise orig
        except GError:
            e = GFALError_filecopy(URIS["srm"], URIS["srm"])
        assert e.reason == GFALError_filecopy.DST_EXISTS
        assert e.exc_value is orig


class TestGFALParseConfig:

    section = "law_test_gfal_fs"

    @pytest.fixture(autouse=True)
    def setup_config(self) -> Iterator[None]:
        cfg = Config.instance()
        cfg.update({self.section: {
            # defined by RemoteFileInterface
            "base": "root://eosuser.cern.ch//eos/user/l/law",
            "retries": 2,
            "retry_delay": "3s",
            "random_base": False,
            # defined by GFALFileInterface
            "gfal_atomic_contexts": True,
            "gfal_transfer_timeout": 100,
            "gfal_transfer_checksum_check": True,
            "gfal_transfer_nbstreams": 4,
            "gfal_transfer_overwrite": False,
            "gfal_transfer_create_parent": True,
            "gfal_transfer_strict_copy": False,
        }})

        yield

        cfg.remove_section(self.section)

    def test_parse(self) -> None:
        config = GFALFileInterface.parse_config(self.section)
        assert config["base"] == ["root://eosuser.cern.ch//eos/user/l/law"]
        assert config["retries"] == 2
        assert config["atomic_contexts"] is True
        assert config["transfer_config"] == {
            "timeout": 100,
            "checksum_check": True,
            "nbstreams": 4,
            "overwrite": False,
            "create_parent": True,
            "strict_copy": False,
        }

    def test_expand_vars(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("LAW_TEST_GFAL_TIMEOUT", "42")
        Config.instance().set(self.section, "gfal_transfer_timeout", "$LAW_TEST_GFAL_TIMEOUT")
        config = GFALFileInterface.parse_config(self.section)
        assert config["transfer_config"]["timeout"] == 42

    def test_keep_existing(self) -> None:
        config = GFALFileInterface.parse_config(self.section, {
            "atomic_contexts": False,
            "transfer_config": {"timeout": 5},
        })
        assert config["atomic_contexts"] is False
        assert config["transfer_config"]["timeout"] == 5
        assert config["transfer_config"]["nbstreams"] == 4

    def test_overwrite(self) -> None:
        config = GFALFileInterface.parse_config(
            self.section,
            {"atomic_contexts": False, "transfer_config": {"timeout": 5}},
            overwrite=True,
        )
        assert config["atomic_contexts"] is True
        assert config["transfer_config"]["timeout"] == 100

    def test_interface_init(self) -> None:
        iface = GFALFileInterface(**GFALFileInterface.parse_config(self.section))
        assert iface.atomic_contexts is True
        assert iface.transfer_config["nbstreams"] == 4
        assert iface.gfal_options == {}
