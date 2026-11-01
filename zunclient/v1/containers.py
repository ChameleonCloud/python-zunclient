# Copyright 2014 NEC Corporation.  All rights reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import os
import posixpath
import shlex
import tarfile
from urllib import parse

from zunclient import api_versions
from zunclient.common import base
from zunclient.common import utils
from zunclient.common.websocketclient import exceptions as ws_exceptions
from zunclient.common.websocketclient import k8s_exec
from zunclient import exceptions


CREATION_ATTRIBUTES = ['name', 'image', 'command', 'cpu', 'memory',
                       'environment', 'workdir', 'labels', 'image_pull_policy',
                       'restart_policy', 'interactive', 'image_driver',
                       'security_groups', 'hints', 'nets', 'auto_remove',
                       'runtime', 'hostname', 'mounts', 'disk',
                       'availability_zone', 'auto_heal', 'privileged',
                       'exposed_ports', 'healthcheck', 'registry', 'tty',
                       'host', 'device_profiles', 'entrypoint']

# For execute_session(): open a pipe to the command's stdin, stdout or
# stderr. See k8s_exec.PIPE.
PIPE = k8s_exec.PIPE


class CommandResult(object):
    """A command that ran in a container and exited."""

    def __init__(self, container, args, returncode, stdout, stderr):
        self.container = container
        self.args = args
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

    def __repr__(self):
        return ("<CommandResult container=%s args=%r returncode=%r>"
                % (self.container, self.args, self.returncode))

    def check_returncode(self):
        """Raise CommandFailed if the exit code isn't 0."""
        if self.returncode:
            raise exceptions.CommandFailed(self.container, self.args,
                                           self.returncode, self.stdout,
                                           self.stderr)


class Container(base.Resource):
    def __repr__(self):
        return "<Container %s>" % self._info


class ContainerManager(base.Manager):
    resource_class = Container

    @staticmethod
    def _path(id=None):

        if id:
            return '/v1/containers/%s' % id
        else:
            return '/v1/containers'

    def list(self, marker=None, limit=None, sort_key=None,
             sort_dir=None, all_projects=False, **kwargs):
        """Retrieve a list of containers.

        :param all_projects: Optional, list containers in all projects

        :param marker: Optional, the UUID of a containers, eg the last
                       containers from a previous result set. Return
                       the next result set.
        :param limit: The maximum number of results to return per
                      request, if:

            1) limit > 0, the maximum number of containers to return.
            2) limit param is NOT specified (None), the number of items
               returned respect the maximum imposed by the ZUN API
               (see Zun's api.max_limit option).

        :param sort_key: Optional, field used for sorting.

        :param sort_dir: Optional, direction of sorting, either 'asc' (the
                         default) or 'desc'.

        :returns: A list of containers.

        """
        if limit is not None:
            limit = int(limit)

        filters = utils.common_filters(marker, limit, sort_key,
                                       sort_dir, all_projects)
        path = ''
        if filters:
            path += '?' + '&'.join(filters)

        if limit is None:
            return self._list(self._path(path),
                              "containers", qparams=kwargs)
        else:
            return self._list_pagination(self._path(path),
                                         "containers",
                                         limit=limit)

    def get(self, id, **kwargs):
        try:
            return self._list(self._path(id),
                              qparams=kwargs)[0]
        except IndexError:
            return None

    def create(self, **kwargs):
        self._process_command(kwargs)
        self._process_mounts(kwargs)
        self._process_tty(kwargs)

        new = {}
        for (key, value) in kwargs.items():
            if key in CREATION_ATTRIBUTES:
                new[key] = value
            else:
                raise exceptions.InvalidAttribute(
                    "Key must be in %s" % ','.join(CREATION_ATTRIBUTES))
        return self._create(self._path(), new)

    def _process_command(self, kwargs):
        cmd_microversion = api_versions.APIVersion("1.20")
        if self.api_version < cmd_microversion:
            command = kwargs.pop('command', None)
            if command:
                kwargs['command'] = utils.parse_command(command)

    def _process_mounts(self, kwargs):
        mounts = kwargs.get('mounts', None)
        if mounts:
            for mount in mounts:
                if mount.get('type') == 'bind':
                    mount['source'] = utils.encode_file_data(mount['source'])

    def _process_tty(self, kwargs):
        tty_microversion = api_versions.APIVersion("1.36")
        if self.api_version >= tty_microversion:
            if 'interactive' in kwargs and 'tty' not in kwargs:
                kwargs['tty'] = kwargs['interactive']

    def delete(self, id, **kwargs):
        return self._delete(self._path(id),
                            qparams=kwargs)

    def _action(self, id, action, method='POST', qparams=None, **kwargs):
        if qparams:
            action = "%s?%s" % (action,
                                parse.urlencode(qparams))
        kwargs.setdefault('headers', {})
        kwargs['headers'].setdefault('Content-Length', '0')
        resp, body = self.api.json_request(method,
                                           self._path(id) + action,
                                           **kwargs)
        return resp, body

    def start(self, id):
        return self._action(id, '/start')

    def stop(self, id, timeout):
        return self._action(id, '/stop',
                            qparams={'timeout': timeout})

    def rebuild(self, id, **kwargs):
        return self._action(id, '/rebuild',
                            qparams=kwargs)

    def restart(self, id, timeout):
        return self._action(id, '/reboot',
                            qparams={'timeout': timeout})

    def pause(self, id):
        return self._action(id, '/pause')

    def unpause(self, id):
        return self._action(id, '/unpause')

    def logs(self, id, **kwargs):
        if kwargs['stdout'] is False and kwargs['stderr'] is False:
            kwargs['stdout'] = True
            kwargs['stderr'] = True
        return self._action(id, '/logs', method='GET',
                            qparams=kwargs)[1]

    def execute(self, id, **kwargs):
        self._process_execute_tty(kwargs)
        return self._action(id, '/execute',
                            qparams=kwargs)[1]

    def execute_run(self, id, args, check=False, input=None, timeout=None):
        """Run a command in the container and wait for it to exit.

        :param args: a list of arguments, or a command line (no shell).
        :param check: if True, raise CommandFailed on a nonzero exit code.
        :param input: bytes, or a binary file, for the command's stdin. The
            command sees the end of its input after the last byte.
        :param timeout: seconds to wait for the command to exit. On a
            timeout the session is closed; the command keeps running.
        :returns: a CommandResult, with stdout and stderr as bytes.
        :raises k8s_exec.SessionError: if the session ended without the
            command's exit code, including when the program doesn't exist.
        :raises k8s_exec.ExecTimeout: if the command ran longer than
            timeout, with the output so far.
        """
        stdin = PIPE if input is not None else None
        session = self.execute_session(id, args, stdin=stdin)
        try:
            stdout, stderr = session.communicate(input, timeout=timeout)
        finally:
            session.close()
        result = CommandResult(id, args, session.exit_code, stdout, stderr)
        if check:
            result.check_returncode()
        return result

    def copy_from(self, id, src_path, dest_dir):
        """Copy a file or directory from the container into dest_dir.

        It runs tar in the container and extracts its archive as it
        arrives, so the files can be any size. Files extracted before an
        error stay.

        :raises CommandFailed: if tar in the container fails, for example
            because src_path doesn't exist.
        :raises tarfile.FilterError: if a file would land outside dest_dir.
        :raises tarfile.ReadError: if the archive is cut off or damaged.
        """
        parent, name = posixpath.split(posixpath.normpath(src_path))
        args = ['tar', 'cf', '-', '-C', parent or '.', name or '.']
        stderr = []
        session = self.execute_session(id, args, stdout=PIPE,
                                       stderr=stderr.append)
        try:
            archive = _ArchiveStream(session.stdout)
            try:
                with tarfile.open(fileobj=archive, mode='r|') as tar:
                    tar.extractall(dest_dir, filter=_copy_from_filter)
                    end = tar.offset
                archive.read_to_end()
                # tar ends an archive with at least 1024 zero bytes.
                # tarfile stops without an error at a damaged header, so
                # check that everything after the last member is zeros.
                if archive.length - end < 1024 or archive.last_data >= end:
                    raise tarfile.ReadError("no end-of-archive marker")
            except tarfile.ReadError as e:
                # If tar failed, that is the error to report.
                read_error = e
            else:
                read_error = None
            returncode = session.wait()
        finally:
            session.close()
        CommandResult(id, args, returncode, b'',
                      b''.join(stderr)).check_returncode()
        if read_error is not None:
            raise tarfile.ReadError(
                "The archive of %s from container %s is cut off or "
                "damaged: %s" % (src_path, id, read_error))

    def copy_to(self, id, src_path, dest_dir):
        """Copy a local file or directory into dest_dir in the container.

        It streams a tar archive to tar in the container, so the files can
        be any size. As with docker cp, the files belong to the user the
        command runs as in the container, not to their local owner, and
        symlinks are copied as links.

        :raises CommandFailed: if tar in the container fails, for example
            because dest_dir doesn't exist.
        :raises OSError: if a local file can't be read. Files that tar in
            the container extracted before it stay.
        """
        arcname = os.path.basename(os.path.normpath(src_path))
        # -o: don't give the files their local owner.
        args = ['tar', 'xof', '-', '-C', dest_dir]
        stdout, stderr = [], []
        session = self.execute_session(id, args, stdin=PIPE,
                                       stdout=stdout.append,
                                       stderr=stderr.append)
        try:
            try:
                with tarfile.open(fileobj=session.stdin, mode='w|') as tar:
                    tar.add(src_path, arcname=arcname)
                session.stdin.close()
            except ws_exceptions.Disconnected:
                # tar exited before reading everything, for example
                # because dest_dir doesn't exist. Its exit code says
                # whether that matters: it may not read all the padding.
                pass
            except Exception:
                # Close tar's input, so that it doesn't wait for the rest.
                try:
                    session.stdin.close()
                except ws_exceptions.Disconnected:
                    pass
                raise
            returncode = session.wait()
        finally:
            session.close()
        CommandResult(id, args, returncode, b''.join(stdout),
                      b''.join(stderr)).check_returncode()

    def execute_session(self, id, args, stdin=None, stdout=None, stderr=None,
                        tty=False):
        """Start a command over a websocket; return its ExecSession.

        :param args: a list of arguments, or a command line (no shell).
        :param stdin: PIPE, to give the command a stdin: write to it with
            session.stdin, or with send() and close_stdin(). If None, the
            command has no stdin.
        :param stdout: where stdout goes as it arrives: a function that
            takes bytes, a binary file, or PIPE, for a pipe to read as
            session.stdout. If None, read it from the session with recv() or
            communicate().
        :param stderr: the same, for stderr. With a tty, stderr is part of
            stdout.
        :param tty: if True, run the command in a terminal, as for
            docker exec -t. Set its size with resize().
        :raises exceptions.UnsupportedVersion: if the server's API is older
            than 1.42 and the command has stdin without a terminal, or a
            terminal without stdin. Before 1.42, stdin always comes with
            a terminal.
        """
        command = args
        if not isinstance(command, str):
            # The server splits the command with shlex.split, which undoes
            # shlex.join exactly.
            command = shlex.join(command)
        kwargs = {}
        if stdin is not None:
            kwargs['interactive'] = True
        if self.api_version >= api_versions.APIVersion("1.42"):
            if stdin is not None or tty:
                kwargs['tty'] = tty
        elif (stdin is not None) != bool(tty):
            raise exceptions.UnsupportedVersion(
                "Zun API %s runs a command with stdin only in a terminal, "
                "and in a terminal only with stdin; 1.42 or later allows "
                "either alone." % self.api_version.get_string())
        # Instead of running when the REST API returns, the command begins
        # execution when the websocket connects.
        response = self.execute(id, command=command, run=False, **kwargs)
        return k8s_exec.connect(response['proxy_url'], stdin=stdin,
                                stdout=stdout, stderr=stderr)

    def _process_execute_tty(self, kwargs):
        tty_microversion = api_versions.APIVersion("1.42")
        if self.api_version >= tty_microversion:
            if 'interactive' in kwargs and 'tty' not in kwargs:
                kwargs['tty'] = kwargs['interactive']

    def execute_resize(self, id, exec_id, width, height):
        self._action(id, '/execute_resize',
                     qparams={'exec_id': exec_id, 'w': width, 'h': height})[1]

    def kill(self, id, signal=None):
        return self._action(id, '/kill',
                            qparams={'signal': signal})[1]

    def run(self, **kwargs):
        self._process_command(kwargs)
        self._process_mounts(kwargs)
        self._process_tty(kwargs)

        if not set(kwargs).issubset(CREATION_ATTRIBUTES):
            raise exceptions.InvalidAttribute(
                "Key must be in %s" % ','.join(CREATION_ATTRIBUTES))
        else:
            return self._create(self._path() + '?run=true', kwargs)

    def rename(self, id, name):
        return self._action(id, '/rename',
                            qparams={'name': name})

    def update(self, id, **patch):
        return self._update(self._path(id), patch)

    def attach(self, id):
        return self._action(id, '/attach', method='GET')[1]

    def resize(self, id, width, height):
        return self._action(id, '/resize',
                            qparams={'w': width, 'h': height})[1]

    def top(self, id, ps_args=None):
        return self._action(id, '/top', method='GET',
                            qparams={'ps_args': ps_args})[1]

    def get_archive(self, id, path):
        res = self._action(id, '/get_archive', method='GET',
                           qparams={'path': path})[1]
        # API version 1.25 or later will return Base64-encoded data
        if self.api_version >= api_versions.APIVersion("1.25"):
            res['data'] = utils.decode_file_data(res['data'])
        else:
            res['data'] = res['data'].encode()
        return res

    def put_archive(self, id, path, data):
        # API version 1.25 or later will expect Base64-encoded data
        if self.api_version >= api_versions.APIVersion("1.25"):
            data = utils.encode_file_data(data)
        return self._action(id, '/put_archive',
                            qparams={'path': path},
                            body={'data': data})

    def stats(self, id):
        return self._action(id, '/stats', method='GET')[1]

    def commit(self, id, repository, tag=None):
        if tag is not None:
            return self._action(id, '/commit', qparams={
                                'repository': repository, 'tag': tag})[1]
        else:
            return self._action(id, '/commit', qparams={
                                'repository': repository})[1]

    def add_security_group(self, id, security_group):
        return self._action(id, '/add_security_group',
                            qparams={'name': security_group})

    def network_detach(self, container, **kwargs):
        return self._action(container, '/network_detach',
                            qparams=kwargs)

    def network_attach(self, container, **kwargs):
        return self._action(container, '/network_attach',
                            qparams=kwargs)

    def network_list(self, container):
        return self._list(self._path(container) + '/network_list',
                          "networks")

    def remove_security_group(self, id, security_group):
        return self._action(id, '/remove_security_group',
                            qparams={'name': security_group})


class _ArchiveStream(object):
    """Reads a tar archive for tarfile, and notes where its data ends.

    :ivar length: how many bytes have been read.
    :ivar last_data: the offset of the last byte that isn't zero, or -1.
    """

    def __init__(self, f):
        self._f = f
        self.length = 0
        self.last_data = -1

    def read(self, size=-1):
        data = self._f.read(size)
        stripped = len(data.rstrip(b'\0'))
        if stripped:
            self.last_data = self.length + stripped - 1
        self.length += len(data)
        return data

    def read_to_end(self):
        while self.read(k8s_exec.CHUNK_SIZE):
            pass


def _copy_from_filter(member, dest_path):
    # The 'tar' filter keeps symlinks that point outside dest_path, which a
    # container's files often have, but only 'data' checks where a hard
    # link points.
    if member.islnk():
        return tarfile.data_filter(member, dest_path)
    return tarfile.tar_filter(member, dest_path)
