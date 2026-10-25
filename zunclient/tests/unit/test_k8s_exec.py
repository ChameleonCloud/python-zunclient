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

from unittest import mock

import testtools
import websocket

from zunclient.common.websocketclient import k8s_exec


BINARY = websocket.ABNF.OPCODE_BINARY

# Messages as Kubernetes sends them: a channel byte, then the payload. The
# Status JSON has the form written by writeStatus and codeExitToStatusError
# in k8s.io/apiserver/pkg/util/proxy.
OUT = (BINARY, b'\x01out\n')
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


class FakeWebSocket(object):
    """recv_data returns the given messages in order, raising any errors."""

    def __init__(self, messages):
        self.messages = list(messages)
        self.closed = False

    def recv_data(self):
        message = self.messages.pop(0)
        if isinstance(message, Exception):
            raise message
        return message

    def close(self):
        self.closed = True


class ConnectTest(testtools.TestCase):

    @mock.patch('websocket.create_connection')
    def test_asks_for_v5_protocol(self, create_connection):
        session = k8s_exec.connect('wss://proxy.example:6784/?token=t')
        create_connection.assert_called_once_with(
            'wss://proxy.example:6784/?token=t',
            subprotocols=['v5.channel.k8s.io'])
        self.assertIs(create_connection.return_value, session._ws)


class ExecSessionTest(testtools.TestCase):

    def test_stdout_stderr_and_exit_code(self):
        ws = FakeWebSocket([OUT, ERR, EXIT_3, CLOSE])
        session = k8s_exec.ExecSession(ws)
        stdout, stderr = session.read_all()
        self.assertEqual(b'out\n', stdout)
        self.assertEqual(b'err\n', stderr)
        self.assertEqual(3, session.exit_code)

    def test_success_is_exit_code_0(self):
        session = k8s_exec.ExecSession(FakeWebSocket([SUCCESS, CLOSE]))
        self.assertEqual((b'', b''), session.read_all())
        self.assertEqual(0, session.exit_code)

    def test_close_without_status_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket([OUT, CLOSE]))
        e = self.assertRaises(k8s_exec.SessionError, session.read_all)
        self.assertIsNone(e.status)
        self.assertEqual('closed with code 1000', e.reason)
        self.assertIsNone(session.exit_code)

    def test_close_reason_is_kept(self):
        session = k8s_exec.ExecSession(FakeWebSocket([
            (websocket.ABNF.OPCODE_CLOSE, b'\x03\xf3backend gone')]))
        e = self.assertRaises(k8s_exec.SessionError, session.read_all)
        self.assertEqual('closed with code 1011: backend gone', e.reason)

    def test_lost_connection_raises(self):
        for error in (websocket.WebSocketConnectionClosedException('gone'),
                      ConnectionResetError('reset')):
            session = k8s_exec.ExecSession(FakeWebSocket([error]))
            e = self.assertRaises(k8s_exec.SessionError, session.read_all)
            self.assertEqual(repr(error), e.reason)
            self.assertIn(str(error), str(e))

    def test_kubernetes_failure_raises_with_status(self):
        session = k8s_exec.ExecSession(FakeWebSocket([INTERNAL_ERROR, CLOSE]))
        e = self.assertRaises(k8s_exec.SessionError, session.read_all)
        self.assertEqual('InternalError', e.status['reason'])
        self.assertIn('executable file not found', str(e))

    def test_unreadable_exit_code_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket([
            (BINARY, EXIT_3[1].replace(b'"3"', b'"three"')), CLOSE]))
        self.assertRaises(k8s_exec.SessionError, session.read_all)

    def test_unreadable_status_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket([
            (BINARY, b'\x03not json'), CLOSE]))
        e = self.assertRaises(k8s_exec.SessionError, session.read_all)
        self.assertIsNone(e.status)

    def test_close(self):
        ws = FakeWebSocket([])
        k8s_exec.ExecSession(ws).close()
        self.assertTrue(ws.closed)
