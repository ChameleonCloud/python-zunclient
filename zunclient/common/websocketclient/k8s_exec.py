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

"""Read from and write to a command running in a container.

The session speaks Kubernetes' remotecommand protocol, v5.channel.k8s.io.
"""

import json

from oslo_log import log as logging
import websocket

from zunclient.common.websocketclient import exceptions

LOG = logging.getLogger(__name__)

V5_PROTOCOL = 'v5.channel.k8s.io'

# The first byte of each message says which channel it is on. ERROR carries
# the command's result as a Kubernetes Status in JSON, sent for success too.
# A message on CLOSE closes the channel named in its one-byte payload.
STDIN = 0
STDOUT = 1
STDERR = 2
ERROR = 3
RESIZE = 4
CLOSE = 255


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


def connect(url):
    """Start an exec session at the URL that containers.execute returned."""
    ws = websocket.create_connection(url, subprotocols=[V5_PROTOCOL])
    return ExecSession(ws)


class ExecSession(object):
    """A command running in a container, read and written over a websocket."""

    def __init__(self, ws):
        self._ws = ws
        self._status = None
        self._ended = None
        self.exit_code = None

    def recv(self):
        """Block until stdout or stderr arrives; return (channel, data).

        data is the bytes that arrived together, not necessarily whole lines.
        Once the command has exited, return None and set exit_code.

        :raises SessionError: if the connection ended before the exit code
            arrived.
        """
        # Most messages are output, returned as they arrive. Kubernetes
        # opens each stream with an empty message, which is skipped. The
        # status message is kept for its exit code, and the loop ends when
        # the connection closes.
        while not self._ended:
            try:
                opcode, data = self._ws.recv_data()
            except (OSError, websocket.WebSocketException) as e:
                # The connection was lost. If the exit code had already
                # arrived, it is still the answer.
                self._ended = repr(e)
                break
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                self._ended = _close_reason(data)
                break
            if not data:
                continue
            channel, payload = data[0], data[1:]
            if channel in (STDOUT, STDERR) and payload:
                return channel, payload
            if channel == ERROR:
                self._status = _parse_status(payload)
        self.exit_code = _exit_code(self._status)
        if self.exit_code is None:
            raise SessionError(self._status, self._ended)
        return None

    def send(self, data):
        """Write bytes to the command's stdin.

        :raises exceptions.Disconnected: if the session has ended.
        """
        self._send(STDIN, data)

    def close_stdin(self):
        """Close the command's stdin, so it sees the end of its input.

        :raises exceptions.Disconnected: if the session has ended.
        """
        self._send(CLOSE, bytes([STDIN]))

    def resize(self, width, height):
        """Set the terminal size, in columns and rows.

        :raises exceptions.Disconnected: if the session has ended.
        """
        size = json.dumps({'Width': width, 'Height': height})
        self._send(RESIZE, size.encode())

    def _send(self, channel, payload):
        # After a close message arrives, websocket-client still sends
        # without an error, so check whether recv() saw the end.
        if self._ended:
            raise exceptions.Disconnected(self._ended)
        try:
            self._ws.send_binary(bytes([channel]) + payload)
        except (OSError, websocket.WebSocketException) as e:
            raise exceptions.Disconnected(e)

    def read_all(self):
        """Block until the command exits; return (stdout, stderr) as bytes.

        :raises SessionError: if the connection ended before the exit code
            arrived.
        """
        out, err = [], []
        while True:
            message = self.recv()
            if message is None:
                return b''.join(out), b''.join(err)
            channel, data = message
            (out if channel == STDOUT else err).append(data)

    def close(self):
        """Close the session's websocket."""
        try:
            self._ws.close()
        except (OSError, websocket.WebSocketException):
            pass


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
