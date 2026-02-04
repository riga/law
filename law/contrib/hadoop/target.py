# coding: utf-8

"""
Implementation of new HDFS Client interface for LAW project
Mimic GFAL interface to support the Hadoop File System 

Part of the code is taken directly from Luigi implementation:
https://github.com/spotify/luigi/blob/master/luigi/contrib/hdfs/hadoopcli_clients.py
"""

__all__ = ["HDFSFileInterface"]

import os
import sys
import gc
import contextlib
import stat as _stat
import six
import subprocess
import re
import warnings

from law.config import Config
from law.target.file import has_scheme, get_scheme
from law.target.remote.interface import RemoteFileInterface, RetryException
from law.logger import get_logger

logger = get_logger(__name__)

try:
    import luigi
    import luigi.contrib.hadoop
    import luigi.contrib.hdfs
    from luigi.contrib.hdfs.config import tmppath
    from luigi.contrib.hdfs import format as hdfs_format
    from luigi.contrib.hdfs import clients as hdfs_clients
    from luigi.contrib.hdfs import config as hdfs_config
    from luigi.contrib.hdfs.config import load_hadoop_cmd
    from luigi.contrib.hdfs import error as hdfs_error

    HAS_LUIGI_HDFS = True

except (ImportError, TypeError):
    HAS_LUIGI_HDFS = False

    class HDFS2Dummy(object):
        def __getattr__(self, attr):
            raise Exception(
                "trying to access 'hdfs.{}', but luigi-hdfs is not installed".format(
                    attr
                )
            )

    luigi.contrib.hdfs = HDFS2Dummy()


class HDFSFileInterface(RemoteFileInterface):
    @classmethod
    def parse_config(cls, section, config=None, overwrite=False):
        config = super(HDFSFileInterface, cls).parse_config(
            section, config=config, overwrite=overwrite
        )

        cfg = Config.instance()

        # helper to add a config value if it exists, extracted with a config parser method
        def add(option, func, postfix="hdfs_", _config=config):
            if option not in config or overwrite:
                _config[option] = func(section, postfix + option)

        return config

    def __init__(
        self, atomic_contexts=False, hdfs_options=None, transfer_config=None, **kwargs
    ):
        super(HDFSFileInterface, self).__init__(**kwargs)

        self._contexts = {}
        self._transfer_parameters = {}

        self.hdfs_options = hdfs_options or {}
        self.transfer_config = transfer_config or {}

        self.hadoop_cmd = load_hadoop_cmd()

    def sanitize_path(self, p):
        return str(p) if isinstance(p, six.string_types) else p

    def exists(self, path, stat=False, base=None, **kwargs):
        """
        hadoop fs -stat
        """
        uri = self.uri(
            path, base_name="stat" if stat else ("exists", "stat"), base=base
        )
        cmd = self.hadoop_cmd + ["fs", "-stat", uri]
        logger.debug("Running file existence check: %s", subprocess.list2cmdline(cmd))
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            universal_newlines=True,
        )
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            return True
        else:
            not_found_pattern = "^.*No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split("\n"):
                if not_found_re.match(line):
                    return False
            raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)

    @RemoteFileInterface.retry(uri_base_name="stat")
    def stat(self, path, base=None, **kwargs):
        """
        HDFS -stat
        """
        uri = self.uri(path, base_name="stat", base=base)
        cmd = self.hadoop_cmd + ["fs", "-stat", uri]
        try:
            logger.debug(
                "Running file existence check: %s", subprocess.list2cmdline(cmd)
            )
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                close_fds=True,
                universal_newlines=True,
            )
            stdout, stderr = p.communicate()
            if p.returncode == 0:
                return stdout
            else:
                not_found_pattern = "^.*No such file or directory$"
                not_found_re = re.compile(not_found_pattern)
                for line in stderr.split("\n"):
                    if not_found_re.match(line):
                        return "Error: Probably path do not exist!"
                raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)

        except hdfs_error.HDFSCliError:
            raise RetryException()

    def isdir(self, path, stat=None, base=None, **kwargs):
        """
        HDFS -test -d
        """
        uri = self.uri(
            path, base_name="stat" if stat else ("exists", "stat"), base=base
        )
        cmd = f'if $({" ".join(self.hadoop_cmd)} fs -test -d {uri});then echo "true";else echo "false";fi'
        logger.debug("Running file existence check: %s", subprocess.list2cmdline(cmd))
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            universal_newlines=True,
            shell=True,
        )
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            if "true" in stdout:
                return True
            else:
                return False
        else:
            not_found_pattern = "^.*No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split("\n"):
                if not_found_re.match(line):
                    return False
            raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)

    def isfile(self, path, stat=None, base=None, **kwargs):
        """
        HDFS -test -f
        """
        uri = self.uri(
            path, base_name="stat" if stat else ("exists", "stat"), base=base
        )
        cmd = f'if $({" ".join(self.hadoop_cmd)} fs -test -f {uri});then echo "true";else echo "false";fi'
        logger.debug("Running file existence check: %s", subprocess.list2cmdline(cmd))
        p = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            universal_newlines=True,
            shell=True,
        )
        stdout, stderr = p.communicate()
        if p.returncode == 0:
            if "true" in stdout:
                return True
            else:
                return False
        else:
            not_found_pattern = "^.*No such file or directory$"
            not_found_re = re.compile(not_found_pattern)
            for line in stderr.split("\n"):
                if not_found_re.match(line):
                    return False
            raise hdfs_error.HDFSCliError(cmd, p.returncode, stdout, stderr)

    @staticmethod
    def call_check(command):
        """
        Helper to run shell command
        """
        p = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            close_fds=True,
            universal_newlines=True,
        )
        stdout, stderr = p.communicate()
        if p.returncode != 0:
            raise hdfs_error.HDFSCliError(command, p.returncode, stdout, stderr)
        return stdout

    @RemoteFileInterface.retry(uri_base_name="chmod")
    def chmod(self, path, perm, base=None, silent=False, recursive=False, **kwargs):
        """
        HDFS -chmod
        """
        if perm is None:
            return True

        uri = self.uri(path, base_name="chmod", base=base)
        try:
            if recursive:
                cmd = self.hadoop_cmd + ["fs", "-chmod", "-R", perm, uri]
            else:
                cmd = self.hadoop_cmd + ["fs", "-chmod", perm, uri]
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_chmod(uri)
            if e.reason == e.UNKNOWN:
                raise e
            if e.reason in (e.NOT_FOUND, e.NOT_SUPPORTED) and silent:
                return False
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def unlink(self, path, base=None, silent=True, skip_trash=False, **kwargs):
        """
        HDFS -rm
        Optional parameter: skip_trash (False as default)
        """
        uri = self.uri(path, base_name="unlink", base=base)
        cmds = []
        if isinstance(uri, list):
            for f in uri:
                cmd = self.hadoop_cmd + ["fs", "-rm"]
                if skip_trash:
                    cmd = cmd + ["-skipTrash"]
                cmd = cmd + [f]
                cmds.append(cmd)
        else:
            cmd = self.hadoop_cmd + ["fs", "-rm"]
            if skip_trash:
                cmd = cmd + ["-skipTrash"]
            cmd = cmd + [f]
            cmds.append(cmd)

        for cmd in cmds:
            try:
                self.call_check(cmd)
            except hdfs_error.HDFSCliError:
                e = HDFSError_unlink(uri)
                # check if the operation should be retried, can fail silently, or raised immediately
                if e.reason == e.UNKNOWN:
                    raise e
                if e.reason == e.NOT_FOUND and silent:
                    return False
                e.reraise()
        return True

    @RemoteFileInterface.retry(uri_base_name="rmdir")
    def rmdir(self, path, base=None, silent=True, **kwargs):
        """
        HDFS -rmdir
        """
        uri = self.uri(path, base_name="rmdir", base=base)
        cmd = self.hadoop_cmd + ["fs", "-rmdir", uri]

        try:
            logger.debug(
                "Remove recursively directory: %s", subprocess.list2cmdline(cmd)
            )
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_rmdir(uri)
            if e.reason == e.UNKNOWN:
                raise e
            if e.reason in (e.NOT_FOUND) and silent:
                return False
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="unlink")
    def remove(
        self, path, base=None, silent=True, recursive=False, skip_trash=False, **kwargs
    ):
        """
        HDFS -rm (-R) to remove files
        """
        uri = self.uri(path, base_name="rmdir", base=base)
        if recursive:
            cmd = self.hadoop_cmd + ["fs", "-rm", "-R"]
        else:
            cmd = self.hadoop_cmd + ["fs", "-rm"]

        if skip_trash:
            cmd = cmd + ["-skipTrash"]

        cmd = cmd + [uri]

        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_unlink(uri)
            if e.reason == e.UNKNOWN:
                raise e
            if e.reason in (e.NOT_FOUND) and silent:
                return False
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="mkdir")
    def mkdir(self, path, perm, base=None, silent=True, **kwargs):
        """
        HDFS -mkdir
        """
        uri = self.uri(path, base_name="mkdir", base=base)
        cmd = self.hadoop_cmd + ["fs", "-mkdir", uri]

        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_mkdir(uri)
            if e.reason == e.UNKNOWN:
                raise e
            if e.reason == e.EXISTS and silent:
                return False
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name=["mkdir_rec", "mkdir"])
    def mkdir_rec(self, path, perm, base=None, silent=True, **kwargs):
        """
        HDFS -mkdir -p
        """
        uri = self.uri(path, base_name="mkdir", base=base)
        cmd = self.hadoop_cmd + ["fs", "-mkdir", "-p", uri]
        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_mkdir(uri)
            if e.reason == e.UNKNOWN:
                raise e
            if e.reason == e.EXISTS and silent:
                return False
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="listdir")
    def listdir(self, path, base=None, **kwargs):
        """
        Return file list under path
        """
        uri = self.uri(path, base_name="listdir", base=base)
        cmd = self.hadoop_cmd + ["fs", "-find", uri]
        try:
            file_list = self.call_check(cmd)
            file_list = file_list.split("\n")[:-1]
            file_list.remove(uri)
            file_list = [f.split("/")[-1] for f in file_list]

            return file_list

        except hdfs_error.HDFSCliError:
            e = HDFSError_listdir(uri)
            if e.EMPTY:
                return []
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="filecopy")
    def filecopy(self, src, dst, base=None, **kwargs):
        """
        HDFS -cp
        """
        if has_scheme(src):
            src_uri = self.sanitize_path(src)
        else:
            src_uri = self.uri(src, base_name="filecopy", base=base)

        if has_scheme(dst):
            dst_uri = self.sanitize_path(dst)
        else:
            dst_uri = self.uri(dst, base_name="filecopy", base=base)

        cmd = self.hadoop_cmd + ["fs", "-cp", src_uri, dst_uri]
        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_filecopy(src_uri, dst_uri)
            # check if the operation should be retried or raised immediately
            if e.reason == e.UNKNOWN:
                raise e
            e.reraise()

        return src_uri, dst_uri

    @RemoteFileInterface.retry(uri_base_name="touch")
    def touch(self, rpath):
        """
        HDFS -touchz
        """
        uri = self.uri(path, base_name="touch", base=base)
        try:
            self.call_check(self.hadoop_cmd + ["fs", "-touchz", rpath])
        except hdfs_error.HDFSCliError:
            e = HDFSError_touch(rpath)
            if e.reason == e.UNKNOWN:
                raise e
            e.reraise()

    @RemoteFileInterface.retry(uri_base_name="copy_from_local")
    def copy_from_local(self, src, dst, base=None, **kwargs):
        """
        HDFS -copyFromLocal
        """
        if has_scheme(dst):
            dst_uri = self.sanitize_path(dst)
        else:
            dst_uri = self.uri(dst, base_name="filecopy", base=base)

        cmd = self.hadoop_cmd + ["fs", "-copyFromLocal", src, dst_uri]
        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_filecopy(src, dst_uri)
            if e.reason == e.UNKNOWN:
                raise e
            e.reraise()
        return src, dst_uri

    @RemoteFileInterface.retry(uri_base_name="copy_to_local")
    def copy_to_local(self, src, dst, base=None, **kwargs):
        """
        HDFS -copyToLocal
        """
        if has_scheme(src):
            src_uri = self.sanitize_path(src)
        else:
            src_uri = self.uri(src, base_name="filecopy", base=base)

        cmd = self.hadoop_cmd + ["fs", "-copyToLocal", src_uri, dst]
        try:
            self.call_check(cmd)
        except hdfs_error.HDFSCliError:
            e = HDFSError_filecopy(src_uri, dst)
            if e.reason == e.UNKNOWN:
                raise e
            e.reraise()
        return src_uri, dst


class HDFSOperationError(RetryException):
    UNKNOWN = "unknown reason"

    def __init__(self, uri, exc=None):
        # store uri and scheme
        self.uri = str(uri)
        self.scheme = get_scheme(uri)

        # get the original error objects and find the error reason
        exc = exc or sys.exc_info()
        self.reason = self._get_reason(str(exc[1]), self.uri, self.scheme)

        # add the error reason to the message
        msg = "{} ({}: {})".format(exc[1], self.__class__.__name__, self.reason)

        super(HDFSOperationError, self).__init__(msg=msg, exc=exc)

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        raise NotImplementedError()


class HDFSError_chmod(HDFSOperationError):
    NOT_FOUND = "no such file or directory"
    NOT_SUPPORTED = "chmod operation not supported"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()

        if scheme == "hdfs":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "srm":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "operation not supported" in lmsg:
                return cls.NOT_SUPPORTED

        elif scheme in ("dav", "davs"):
            if "protocol not supported" in lmsg:
                return cls.NOT_SUPPORTED

        elif scheme == "dropbox":
            if "protocol not supported" in lmsg:
                return cls.NOT_SUPPORTED

        else:
            logger.warning(
                "scheme '{}' not known to {}, cannot parse '{}'".format(
                    scheme, cls.__name__, msg
                )
            )

        return cls.UNKNOWN


class HDFSError_unlink(HDFSOperationError):
    NOT_FOUND = "target not found"
    IS_DIRECTORY = "target is a directory"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()

        if scheme == "hdfs":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "is a directory" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "is a directory" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a file" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "srm":
            if "no such file" in lmsg:
                return cls.NOT_FOUND
            if "not a file" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme in ("dav", "davs"):
            if "file not found" in lmsg:
                return cls.NOT_FOUND
            if "is a directory" in lmsg:
                return cls.IS_DIRECTORY

        elif scheme == "dropbox":
            if "not_found" in lmsg:
                return cls.NOT_FOUND

        else:
            logger.warning(
                "scheme '{}' not known to {}, cannot parse '{}'".format(
                    scheme, cls.__name__, msg
                )
            )

        return cls.UNKNOWN


class HDFSError_rmdir(HDFSOperationError):
    NOT_FOUND = "target not found"
    IS_FILE = "target is a file"
    NOT_EMPTY = "directory is not empty"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()

        if scheme == "hdfs":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a directory" in lmsg:
                return cls.IS_FILE
            if "directory is not empty" in lmsg:
                return cls.NOT_EMPTY

        elif scheme == "root":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a directory" in lmsg:
                return cls.IS_FILE
            if "no such device" in lmsg:
                # cryptic message for non-empty directory
                return cls.NOT_EMPTY

        elif scheme == "gsiftp":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "not a directory" in lmsg:
                return cls.IS_FILE
            if "directory is not empty" in lmsg:
                return cls.NOT_EMPTY

        elif scheme == "srm":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "this file is not a directory" in lmsg:
                return cls.IS_FILE
            if "directory not empty" in lmsg:
                return cls.NOT_EMPTY

        elif scheme in ("dav", "davs"):
            if "file not found" in lmsg:
                return cls.NOT_FOUND

        elif scheme == "dropbox":
            if "not_found" in lmsg:
                return cls.NOT_FOUND

        else:
            logger.warning(
                "scheme '{}' not known to {}, cannot parse '{}'".format(
                    scheme, cls.__name__, msg
                )
            )

        return cls.UNKNOWN


class HDFSError_mkdir(HDFSOperationError):
    EXISTS = "target already exists"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()

        if scheme == "hdfs":
            if "file exists" in lmsg:
                return cls.EXISTS

        elif scheme == "root":
            if "file exists" in lmsg:
                return cls.EXISTS

        elif scheme == "gsiftp":
            if "directory already exists" in lmsg:
                return cls.EXISTS

        elif scheme == "srm":
            if "directory already exist" in lmsg:
                return cls.EXISTS

        elif scheme == "dropbox":
            if "the directory already exists" in lmsg:
                return cls.EXISTS

        else:
            logger.warning(
                "scheme '{}' not known to {}, cannot parse '{}'".format(
                    scheme, cls.__name__, msg
                )
            )

        return cls.UNKNOWN


class HDFSError_listdir(HDFSOperationError):
    EMPTY = "directory is empty"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()
        if scheme == "hdfs":
            if "No such file or directory" in lmsg:
                return cls.EMPTY

        return cls.UNKNOWN


class HDFSError_touch(HDFSOperationError):
    EMPTY = "directory is empty"

    NOT_FOUND = "no such file or directory"
    NOT_SUPPORTED = "chmod operation not supported"

    @classmethod
    def _get_reason(cls, msg, uri, scheme):
        lmsg = msg.lower()

        if scheme == "hdfs":
            if "no such file or directory" in lmsg:
                return cls.NOT_FOUND
            if "protocol not supported" in lmsg:
                return cls.NOT_SUPPORTED

        return cls.UNKNOWN


class HDFSError_filecopy(HDFSOperationError):
    SRC_NOT_FOUND = "source not found"
    DST_EXISTS = "target already exists"

    def __init__(self, src_uri, dst_uri, exc=None):
        # store uri and scheme
        self.src_uri = str(src_uri)
        self.dst_uri = str(dst_uri)
        self.src_scheme = get_scheme(src_uri)
        self.dst_scheme = get_scheme(dst_uri)

        # get the original error objects and find the error reason
        exc = exc or sys.exc_info()
        self.reason = self._get_reason(
            str(exc[1]), self.src_uri, self.dst_uri, self.src_scheme, self.dst_scheme
        )

        # add the error reason to the message
        msg = "{} ({}: {})".format(exc[1], self.__class__.__name__, self.reason)

        # bypass the GFALOperationError init
        RetryException.__init__(self, msg=msg, exc=exc)

    @classmethod
    def _get_reason(cls, msg, src_uri, dst_uri, src_scheme, dst_scheme):
        # in gfal, error messages on missing source files or existing target files depend on both
        # source and destination protocols, so all cases need to be handled separately
        lmsg = msg.lower()
        if (src_scheme, dst_scheme) == ("file", "file"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "hdfs"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("hdfs", "file"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("hdfs", "root"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "hdfs"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("gsiftp", "hdfs"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("hdfs", "gsiftp"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("hdfs", "srm"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "hdfs"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "root"):
            if "no such file or directory (source)" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists (destination)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "gsiftp"):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("file", "srm"):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("file", "dav"), ("file", "davs")):
            if "local system call no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND

        elif (src_scheme, dst_scheme) == ("file", "dropbox"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("gsiftp", "file"):
            if "no such file or directory on url" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("gsiftp", "gsiftp"):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND
            if "destination already exist" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("gsiftp", "dav"), ("gsiftp", "davs")):
            if "is a directory" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "file"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "root"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("srm", "srm"):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("srm", "dav"), ("srm", "davs")):
            if "no such file" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "file"):
            if "no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists (destination)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "srm"):
            if "no such file or directory" in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("root", "root"):
            if "destination does not support delegation." in lmsg:
                return cls.SRC_NOT_FOUND
            if "file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("root", "dav"), ("root", "davs")):
            if "failed to open file (block device required)" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("dav", "file"), ("davs", "file")):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND

        elif (src_scheme, dst_scheme) in (("dav", "gsiftp"), ("davs", "gsiftp")):
            if "file not found" in lmsg:
                return cls.SRC_NOT_FOUND
            if "not a file" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) in (("dav", "root"), ("davs", "root")):
            # it appears that there is a bug in gfal when copying via davix to xrootd in that
            # the full dst path is repeated, e.g. "root://url.tld:1090/pnfs/.../root://url..."
            # which causes weird behavior, and as long as this issue persists, there should be no
            # error parsing in law
            pass

        elif (src_scheme, dst_scheme) in (("dav", "srm"), ("davs", "srm")):
            # same issue as for davix -> xrootd, wait until this is resolved
            pass

        elif (src_scheme, dst_scheme) == ("dropbox", "file"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        elif (src_scheme, dst_scheme) == ("dropbox", "dropbox"):
            if "could not open source" in lmsg:
                return cls.SRC_NOT_FOUND
            if "the file exists" in lmsg:
                return cls.DST_EXISTS

        else:
            logger.warning(
                "combination of source scheme '{}' and target scheme '{}' not known to "
                "{}, cannot parse '{}'".format(
                    src_scheme, dst_scheme, cls.__name__, msg
                )
            )

        return cls.UNKNOWN
