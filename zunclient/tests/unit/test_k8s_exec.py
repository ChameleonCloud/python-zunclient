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
import threading
from unittest import mock

import testtools
import websocket

from zunclient.common.websocketclient import k8s_exec
from zunclient.tests.unit import k8s_exec_server


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


class FakeWebSocket(object):
    """A websocket whose server is the test.

    The test queues messages with receive(); the session's reader gets them
    from recv_data. Like a real server, send_close() gets a close message
    in reply, and shutdown() ends a waiting recv_data, as shutting down
    the socket underneath does.
    """

    def __init__(self, *messages):
        self._incoming = queue.Queue()
        self.sent = []
        self.shut_down = False
        self.sock = _FakeSocket(self)
        self.receive(*messages)

    def receive(self, *messages):
        for message in messages:
            self._incoming.put(message)

    def recv_data(self):
        message = self._incoming.get()
        if isinstance(message, Exception):
            raise message
        return message

    def send_binary(self, data):
        if self.shut_down:
            raise websocket.WebSocketConnectionClosedException()
        self.sent.append(data)

    def send_close(self):
        self.sent.append('close')
        self.receive(CLOSE)

    def shutdown(self):
        self.shut_down = True
        self.sock = None
        self.receive(websocket.WebSocketConnectionClosedException())


class _FakeSocket(object):

    def __init__(self, ws):
        self._ws = ws

    def shutdown(self, how):
        self._ws.receive(websocket.WebSocketConnectionClosedException())


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


class ReadTest(testtools.TestCase):

    def test_stdout_stderr_and_exit_code(self):
        session = k8s_exec.ExecSession(FakeWebSocket(OUT, ERR, EXIT_3, CLOSE))
        self.assertEqual((b'out\n', b'err\n'), session.read_all())
        self.assertEqual(3, session.exit_code)

    def test_recv(self):
        session = k8s_exec.ExecSession(FakeWebSocket(
            OPEN_OUT, OUT, ERR, EXIT_3, CLOSE))
        self.assertEqual((k8s_exec.STDOUT, b'out\n'), session.recv(TIMEOUT))
        self.assertEqual((k8s_exec.STDERR, b'err\n'), session.recv(TIMEOUT))
        self.assertIsNone(session.recv(TIMEOUT))
        self.assertEqual(3, session.exit_code)
        self.assertIsNone(session.recv(TIMEOUT))  # stays at the end

    def test_recv_timeout(self):
        session = k8s_exec.ExecSession(FakeWebSocket())
        self.addCleanup(session.close, timeout=0)
        self.assertRaises(k8s_exec.ExecTimeout, session.recv, 0.01)

    def test_success_is_exit_code_0(self):
        session = k8s_exec.ExecSession(FakeWebSocket(SUCCESS, CLOSE))
        self.assertEqual(0, session.wait(TIMEOUT))
        self.assertEqual('Success', session.status['status'])

    def test_wait_does_not_need_output_read(self):
        # Output nobody reads doesn't hold up the exit code.
        session = k8s_exec.ExecSession(FakeWebSocket(OUT, ERR, EXIT_3, CLOSE))
        self.assertEqual(3, session.wait(TIMEOUT))

    def test_one_thread_reads_while_another_waits(self):
        # For example, a terminal shows the output while a notebook cell
        # waits for the exit code.
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws)
        shown = []

        def terminal():
            while True:
                message = session.recv(TIMEOUT)
                if message is None:
                    return
                shown.append(message)
        thread = threading.Thread(target=terminal, daemon=True)
        thread.start()
        ws.receive(OUT, EXIT_3, CLOSE)
        self.assertEqual(3, session.wait(TIMEOUT))
        thread.join(TIMEOUT)
        self.assertEqual([(k8s_exec.STDOUT, b'out\n')], shown)

    def test_wait_timeout_leaves_session_running(self):
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws)
        self.assertRaises(k8s_exec.ExecTimeout, session.wait, 0.01)
        ws.receive(SUCCESS, CLOSE)
        self.assertEqual(0, session.wait(TIMEOUT))

    def test_close_without_status_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket(OUT, CLOSE))
        e = self.assertRaises(k8s_exec.SessionError, session.read_all)
        self.assertIsNone(e.status)
        self.assertEqual('closed with code 1000', e.reason)
        self.assertIsNone(session.exit_code)

    def test_close_reason_is_kept(self):
        session = k8s_exec.ExecSession(FakeWebSocket(
            (websocket.ABNF.OPCODE_CLOSE, b'\x03\xf3backend gone')))
        e = self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        self.assertEqual('closed with code 1011: backend gone', e.reason)

    def test_lost_connection_raises(self):
        for error in (websocket.WebSocketConnectionClosedException('gone'),
                      ConnectionResetError('reset')):
            session = k8s_exec.ExecSession(FakeWebSocket(error))
            e = self.assertRaises(k8s_exec.SessionError, session.wait,
                                  TIMEOUT)
            self.assertEqual(repr(error), e.reason)
            self.assertIn(str(error), str(e))

    def test_exit_code_survives_lost_connection(self):
        session = k8s_exec.ExecSession(FakeWebSocket(
            EXIT_3, ConnectionResetError('reset')))
        self.assertEqual(3, session.wait(TIMEOUT))

    def test_kubernetes_failure_raises_with_status(self):
        session = k8s_exec.ExecSession(FakeWebSocket(INTERNAL_ERROR, CLOSE))
        e = self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        self.assertEqual('InternalError', e.status['reason'])
        self.assertIn('executable file not found', str(e))

    def test_unreadable_exit_code_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket(
            (BINARY, EXIT_3[1].replace(b'"3"', b'"three"')), CLOSE))
        self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)

    def test_unreadable_status_raises(self):
        session = k8s_exec.ExecSession(FakeWebSocket(
            (BINARY, b'\x03not json'), CLOSE))
        e = self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        self.assertIsNone(e.status)


class SinkTest(testtools.TestCase):

    def test_sinks_get_output_as_it_arrives(self):
        out, err = [], io.BytesIO()
        session = k8s_exec.ExecSession(
            FakeWebSocket(OPEN_OUT, OUT, ERR, OUT, EXIT_3, CLOSE),
            stdout=out.append, stderr=err)
        self.assertEqual((b'', b''), session.read_all())
        self.assertEqual([b'out\n', b'out\n'], out)
        self.assertEqual(b'err\n', err.getvalue())

    def test_recv_gets_only_channels_without_a_sink(self):
        out = []
        session = k8s_exec.ExecSession(FakeWebSocket(OUT, ERR, SUCCESS, CLOSE),
                                       stdout=out.append)
        self.assertEqual((k8s_exec.STDERR, b'err\n'), session.recv(TIMEOUT))
        self.assertIsNone(session.recv(TIMEOUT))
        self.assertEqual([b'out\n'], out)

    def test_failing_sink_ends_session(self):
        def full(data):
            raise OSError(28, 'No space left on device')
        ws = FakeWebSocket(OUT)
        session = k8s_exec.ExecSession(ws, stdout=full)
        e = self.assertRaises(OSError, session.wait, TIMEOUT)
        self.assertEqual(28, e.errno)
        self.assertTrue(ws.shut_down)

    def test_failing_sink_raises_even_with_exit_code(self):
        def full(data):
            raise OSError(28, 'No space left on device')
        session = k8s_exec.ExecSession(FakeWebSocket(EXIT_3, OUT, CLOSE),
                                       stdout=full)
        self.assertRaises(OSError, session.wait, TIMEOUT)

    def test_wait_with_sinks_and_more_output_than_the_queue(self):
        # With sinks, output never waits for recv(), so wait() returns.
        out = []
        many = [OUT] * (k8s_exec.QUEUE_SIZE * 2)
        session = k8s_exec.ExecSession(FakeWebSocket(*many, EXIT_3, CLOSE),
                                       stdout=out.append)
        self.assertEqual(3, session.wait(TIMEOUT))
        self.assertEqual(len(many), len(out))

    def test_sink_can_close_the_session(self):
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws, stdout=lambda data: session.close())
        ws.receive(OUT)
        self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        session._reader.join(TIMEOUT)
        self.assertFalse(session._reader.is_alive())


class CloseTest(testtools.TestCase):

    def test_close_running_session(self):
        ws = FakeWebSocket()
        session = k8s_exec.ExecSession(ws)
        session.close()
        self.assertEqual(['close'], ws.sent)
        self.assertTrue(ws.shut_down)
        self.assertRaises(k8s_exec.SessionError, session.wait, TIMEOUT)
        # A recv() after close() ends instead of waiting for more output.
        self.assertRaises(k8s_exec.SessionError, session.recv, TIMEOUT)

    def test_close_after_end(self):
        ws = FakeWebSocket(SUCCESS, CLOSE)
        session = k8s_exec.ExecSession(ws)
        session.wait(TIMEOUT)
        session.close()
        self.assertEqual([], ws.sent)
        self.assertTrue(ws.shut_down)

    def test_reader_shuts_socket_down_when_server_closes(self):
        # websocket-client's own close() does nothing once it has answered
        # the server's close message, so the reader has to.
        ws = FakeWebSocket(SUCCESS, CLOSE)
        k8s_exec.ExecSession(ws).wait(TIMEOUT)
        self.assertTrue(ws.shut_down)

    def test_close_frees_reader_waiting_on_unread_output(self):
        ws = FakeWebSocket(*[OUT] * (k8s_exec.QUEUE_SIZE + 10))
        session = k8s_exec.ExecSession(ws)
        while session._output.qsize() < k8s_exec.QUEUE_SIZE:
            threading.Event().wait(0.001)
        session.close(timeout=TIMEOUT)
        session._reader.join(TIMEOUT)
        self.assertFalse(session._reader.is_alive())


class RealSocketTest(testtools.TestCase):
    """Against a real TCP socket, where unread output fills the buffers."""

    def test_close_wakes_reader_on_silent_server(self):
        # On a real socket, websocket-client's shutdown() leaves a thread
        # blocked in recv_data(); close() must not.
        silent = threading.Event()
        self.addCleanup(silent.set)
        server = k8s_exec_server.ExecServer(lambda conn: silent.wait())
        session = k8s_exec.connect(server.url)
        session.close(timeout=0.1)
        session._reader.join(TIMEOUT)
        self.assertFalse(session._reader.is_alive())
