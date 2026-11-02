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

"""Run a command in a container over an exec session.

Sessions use Kubernetes' remotecommand protocol, v5.channel.k8s.io,
through Zun's websocket proxy.
"""

from concurrent import futures
import json
import socket
import threading

from oslo_log import log as logging
import websocket

from zunclient.common.websocketclient import exceptions

LOG = logging.getLogger(__name__)

V5_PROTOCOL = 'v5.channel.k8s.io'

STDOUT = 1
STDERR = 2
ERROR = 3   # JSON Status object, includes exit code for all commands.


class SessionError(exceptions.ContainerWebSocketException):
    """The session ended without the command's exit code.

    :ivar status: the Kubernetes Status, as a dict, if one arrived.
    :ivar reason: how the connection ended, as text.
    """

    def __init__(self, status=None, reason=None):
        self.status = status
        self.reason = reason
        if status:
            message = ("Kubernetes couldn't run the command: %s"
                       % status.get('message'))
        else:
            message = ("The exec session ended without the command's "
                       "exit code (%s)" % reason)
        super(SessionError, self).__init__(message=message)


class ExecTimeout(exceptions.ContainerWebSocketException):
    """Timeout while waiting for a command to complete."""

    def __init__(self, timeout):
        self.timeout = timeout
        super(ExecTimeout, self).__init__(
            message="The command was still running after %s seconds"
                    % timeout)


def connect(url, stdout=None, stderr=None):
    """Connect to Zun's websocket proxy, which then starts the remote command.

    :param url: the proxy_url from ContainerManager.execute(..., run=False).
    :param stdout: a binary file object, such as an open file or
        io.BytesIO, that receives the command's standard output as it
        arrives. If None, the session discards the output.
    :param stderr: the same, for standard error.
    """
    ws = websocket.create_connection(url, subprotocols=[V5_PROTOCOL])
    return ExecSession(ws, stdout=stdout, stderr=stderr)


class ExecSession(object):
    """A connection to a command running in a container."""

    def __init__(self, ws, stdout=None, stderr=None):
        self._ws = ws
        files = {STDOUT: stdout, STDERR: stderr}
        self._exit_code = futures.Future()
        self._reader = threading.Thread(target=self._read, args=(files,),
                                        name='k8s-exec-reader', daemon=True)
        self._reader.start()

    def _read(self, files):
        try:
            exit_code = _read_until_exit(self._ws, files)
        except BaseException as e:  # noqa: BLE001
            # Every exception must reach wait(), or wait() blocks forever.
            self._exit_code.set_exception(e)
        else:
            self._exit_code.set_result(exit_code)

    def wait(self, timeout=None):
        """Block until the command exits; return its exit code.

        If writing the output raised an exception, wait() raises it.

        :raises SessionError: if the session ended without the exit code.
        :raises ExecTimeout: if the command is still running after timeout
            seconds. The session is not closed: call wait() again, or
            close().
        """
        _, not_done = futures.wait([self._exit_code], timeout)
        if not_done:
            raise ExecTimeout(timeout)
        return self._exit_code.result()

    def close(self, timeout=3):
        """End the session. The command keeps running if it hasn't exited.

        :param timeout: seconds to wait for the server to acknowledge the
            close message.
        """
        if not self._exit_code.done():
            try:
                self._ws.send_close()
            except (OSError, websocket.WebSocketException):
                pass
            futures.wait([self._exit_code], timeout)
        # Shut down the underlying socket directly. ws.shutdown() doesn't wake 
        # a thread blocked in recv_data(). We also can't use ws.close(), it 
        # receives frames, and this method may be called from a non-reader 
        # thread.
        sock = self._ws.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass


def _read_until_exit(ws, files):
    """Write the command's output to files until the connection ends.

    Return the command's exit code. Close ws before returning or raising.

    :param files: a dict that maps STDOUT and STDERR to binary files, or
        to None to discard that output.
    :raises SessionError: if the connection ended without the exit code.
    """
    status = None
    try:
        while True:
            try:
                opcode, data = ws.recv_data()
            except (OSError, websocket.WebSocketException) as e:
                ended = repr(e)
                break
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                ended = _close_reason(data)
                break
            if not data:
                continue
            channel, payload = data[0], data[1:]
            if channel in (STDOUT, STDERR):
                file = files[channel]
                if file is not None and payload:
                    file.write(payload)
                    # Flush so that the output appears as soon as it arrives.
                    file.flush()
            elif channel == ERROR:
                status = _parse_status(payload)
    finally:
        # ws.close() does nothing once the server has closed the
        # connection, so close the socket with ws.shutdown().
        ws.shutdown()
    exit_code = _exit_code(status)
    if exit_code is None:
        raise SessionError(status, ended)
    return exit_code


def _close_reason(data):
    """The close code and reason in a close message, as text."""
    if len(data) < 2:
        return "closed with no close code"
    reason = "closed with code %d" % int.from_bytes(data[:2], 'big')
    if data[2:]:
        reason += ": " + data[2:].decode('utf-8', 'replace')
    return reason


def _parse_status(payload):
    try:
        status = json.loads(payload)
    except ValueError:
        status = None
    if not isinstance(status, dict):
        LOG.warning("Ignoring an unreadable Kubernetes Status: %r", payload)
        return None
    return status


def _exit_code(status):
    """The exit code in a Kubernetes Status, or None if it has none."""
    if not status:
        return None
    if status.get('status') == 'Success':
        return 0
    causes = (status.get('details') or {}).get('causes') or []
    for cause in causes:
        if cause.get('reason') == 'ExitCode':
            try:
                return int(cause.get('message'))
            except (TypeError, ValueError):
                LOG.warning("Ignoring an unreadable exit code: %r",
                            cause.get('message'))
                return None
    return None
