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

import io
import queue
import socket
import threading
from unittest import mock

import testtools
import websocket

from zunclient.common.websocketclient import k8s_exec


BINARY = websocket.ABNF.OPCODE_BINARY

# Messages as Kubernetes sends them: a channel byte, then the payload. The
# Status JSON has the form written by writeStatus and codeExitToStatusError
# in k8s.io/apiserver/pkg/util/proxy.
OUT = (BINARY, b'\x01out\n')
OPEN_OUT = (BINARY, b'\x01')  # how Kubernetes opens a stream
ERR = (BINARY, b'\x02err\n')
SUCCESS = (BINARY, b'\x03{"metadata":{},"status":"Success"}')
EXIT_3 = (BINARY, b'\x03{"metadata":{},"status":"Failure","message":'
                  b'"command terminated with non-zero exit code: error '
                  b'executing command [sh -c exit 3], exit code 3",'
                  b'"reason":"NonZeroExitCode","details":{"causes":'
                  b'[{"reason":"ExitCode","message":"3"}]}}')
INTERNAL_ERROR = (BINARY, b'\x03{"metadata":{},"status":"Failure","message":'
                          b'"Internal error occurred: executable file not '
                          b'found","reason":"InternalError","details":'
                          b'{"causes":[{"message":"executable file not '
                          b'found"}]},"code":500}')
CLOSE = (websocket.ABNF.OPCODE_CLOSE, b'\x03\xe8')  # close code 1000

TIMEOUT = 5  # seconds; only reached when a test fails

DISCARD = {k8s_exec.STDOUT: None, k8s_exec.STDERR: None}


class FakeWebSocket(object):
    """recv_data returns the given messages in order, then blocks."""

    def __init__(self, *messages):
        self.messages = queue.Queue()
        for message in messages:
            self.messages.put(message)
        self.sock = mock.Mock()
        self.shut_down = False

    def recv_data(self):
        message = self.messages.get()
        if isinstance(message, Exception):
            raise message
        return message

    def send_close(self):
        self.messages.put(CLOSE)

    def shutdown(self):
        self.shut_down = True


class ConnectTest(testtools.TestCase):

    @mock.patch('websocket.create_connection')
    def test_asks_for_v5_protocol(self, create_connection):
        ws = create_connection.return_value
        ws.recv_data.side_effect = websocket.WebSocketConnectionClosedException
        session = k8s_exec.connect('wss://proxy.example:6784/?token=t')
        create_connection.assert_called_once_with(
            'wss://proxy.example:6784/?token=t',
            subprotocols=['v5.channel.k8s.io'])
        self.assertIs(ws, session._ws)


class ReadUntilExitTest(testtools.TestCase):

    def test_stdout_stderr_and_exit_code(self):
        out, err = io.BytesIO(), io.BytesIO()
        ws = FakeWebSocket(OPEN_OUT, OUT, ERR, EXIT_3, CLOSE)
        exit_code = k8s_exec._read_until_exit(
            ws, {k8s_exec.STDOUT: out, k8s_exec.STDERR: err})
        self.assertEqual(3, exit_code)
        self.assertEqual(b'out\n', out.getvalue())
        self.assertEqual(b'err\n', err.getvalue())
        self.assertTrue(ws.shut_down)

    def test_success_is_exit_code_0(self):
        ws = FakeWebSocket(SUCCESS, CLOSE)
        self.assertEqual(0, k8s_exec._read_until_exit(ws, DISCARD))

    def test_close_without_status_raises(self):
        ws = FakeWebSocket(OUT, CLOSE)
        e = self.assertRaises(k8s_exec.SessionError,
                              k8s_exec._read_until_exit, ws, DISCARD)
        self.assertIsNone(e.status)
        self.assertEqual('closed with code 1000', e.reason)
        self.assertTrue(ws.shut_down)

    def test_close_reason_is_kept(self):
        ws = FakeWebSocket(
            (websocket.ABNF.OPCODE_CLOSE, b'\x03\xf3backend gone'))
        e = self.assertRaises(k8s_exec.SessionError,
                              k8s_exec._read_until_exit, ws, DISCARD)
        self.assertEqual('closed with code 1011: backend gone', e.reason)

    def test_lost_connection_raises(self):
        for error in (websocket.WebSocketConnectionClosedException('gone'),
                      ConnectionResetError('reset')):
            e = self.assertRaises(k8s_exec.SessionError,
                                  k8s_exec._read_until_exit,
                                  FakeWebSocket(error), DISCARD)
            self.assertEqual(repr(error), e.reason)
            self.assertIn(str(error), str(e))

    def test_exit_code_survives_lost_connection(self):
        ws = FakeWebSocket(EXIT_3, ConnectionResetError('reset'))
        self.assertEqual(3, k8s_exec._read_until_exit(ws, DISCARD))

    def test_kubernetes_failure_raises_with_status(self):
        ws = FakeWebSocket(INTERNAL_ERROR, CLOSE)
        e = self.assertRaises(k8s_exec.SessionError,
                              k8s_exec._read_until_exit, ws, DISCARD)
        self.assertEqual('InternalError', e.status['reason'])
        self.assertIn('executable file not found', str(e))

    def test_unreadable_exit_code_raises(self):
        ws = FakeWebSocket(
            (BINARY, EXIT_3[1].replace(b'"3"', b'"three"')), CLOSE)
        self.assertRaises(k8s_exec.SessionError,
                          k8s_exec._read_until_exit, ws, DISCARD)

    def test_unreadable_status_raises(self):
        ws = FakeWebSocket((BINARY, b'\x03not json'), CLOSE)
        e = self.assertRaises(k8s_exec.SessionError,
                              k8s_exec._read_until_exit, ws, DISCARD)
        self.assertIsNone(e.status)


class ExecSessionTest(testtools.TestCase):

    def test_wait_returns_exit_code(self):
        session = k8s_exec.ExecSession(FakeWebSocket(OUT, EXIT_3, CLOSE))
        self.assertEqual(3, session.wait(TIMEOUT))

    def test_wait_timeout_leaves_session_running(self):
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws)
        self.assertRaises(k8s_exec.ExecTimeout, session.wait, 0.01)
        ws.messages.put(SUCCESS)
        ws.messages.put(CLOSE)
        self.assertEqual(0, session.wait(TIMEOUT))

    def test_output_is_written_while_the_command_runs(self):
        stdout = mock.Mock()
        flushed = threading.Event()
        stdout.flush.side_effect = flushed.set
        session = k8s_exec.ExecSession(FakeWebSocket(OUT), stdout=stdout)
        self.addCleanup(session.close, timeout=0)
        self.assertTrue(flushed.wait(TIMEOUT))
        stdout.write.assert_called_once_with(b'out\n')

    def test_wait_raises_what_writing_raised(self):
        for error in (OSError(28, 'No space left on device'),
                      TimeoutError('write timed out'), SystemExit(1)):
            stdout = mock.Mock()
            stdout.write.side_effect = error
            session = k8s_exec.ExecSession(FakeWebSocket(OUT), stdout=stdout)
            self.assertRaises(type(error), session.wait, TIMEOUT)

    def test_close_running_session(self):
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws)
        session.close()
        self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        ws.sock.shutdown.assert_called_once_with(socket.SHUT_RDWR)
