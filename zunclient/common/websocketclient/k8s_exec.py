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

"""Read a command's output and exit code from a Kubernetes exec session."""

import json

from oslo_log import log as logging
import websocket

from zunclient.common.websocketclient import exceptions

LOG = logging.getLogger(__name__)

V5_PROTOCOL = 'v5.channel.k8s.io'

# The first byte of each message says which channel it is on. ERROR carries
# the command's result as a Kubernetes Status in JSON, sent for success too.
STDOUT = 1
STDERR = 2
ERROR = 3


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
    """A command running in a container, read over a websocket."""

    def __init__(self, ws):
        self._ws = ws
        self.exit_code = None

    def read_all(self):
        """Read to the end; return (stdout, stderr) and set exit_code.

        :raises SessionError: if the connection ended before the exit code
            arrived.
        """
        out, err, status, ended = [], [], None, None
        while True:
            try:
                opcode, data = self._ws.recv_data()
            except (OSError, websocket.WebSocketException) as e:
                # The connection was lost. If the exit code had already
                # arrived, it is still the answer.
                ended = repr(e)
                break
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                ended = _close_reason(data)
                break
            if not data:
                continue
            channel, payload = data[0], data[1:]
            if channel == STDOUT:
                out.append(payload)
            elif channel == STDERR:
                err.append(payload)
            elif channel == ERROR:
                status = _parse_status(payload)
        self.exit_code = _exit_code(status)
        if self.exit_code is None:
            raise SessionError(status, ended)
        return b''.join(out), b''.join(err)

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
