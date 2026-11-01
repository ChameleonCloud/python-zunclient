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

import fcntl
import io
import os
import signal
import struct
import termios
import threading
from unittest import mock

import testtools

from zunclient.common.websocketclient import exceptions
from zunclient.common.websocketclient import k8s_exec
from zunclient.common.websocketclient import terminal

TIMEOUT = 5  # seconds; only reached when a test fails


class FakeSession(object):
    """An ExecSession whose command exits when the test says."""

    def __init__(self, exit_on_stdin_close=False):
        self.sent = []
        self.sizes = []
        self.stdin_closed = False
        self.closed = False
        self.exited = threading.Event()
        self.exit_code = 0
        self._exit_on_stdin_close = exit_on_stdin_close

    def send(self, data):
        if self.exited.is_set():
            raise exceptions.Disconnected(message='closed')
        self.sent.append(data)

    def resize(self, width, height):
        self.sizes.append((width, height))

    def close_stdin(self):
        self.stdin_closed = True
        if self._exit_on_stdin_close:
            self.exited.set()

    def wait(self, timeout=None):
        self.exited.wait()
        return self.exit_code

    def close(self):
        self.closed = True
        self.exited.set()  # ending the session, without an exit code


def set_size(fd, columns, rows):
    fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack('HHHH', rows, columns,
                                                    0, 0))


def eventually(check):
    for _ in range(int(TIMEOUT / 0.005)):
        if check():
            return
        threading.Event().wait(0.005)
    raise AssertionError('timed out')


class TerminalTest(testtools.TestCase):

    def setUp(self):
        super(TerminalTest, self).setUp()
        self.session = FakeSession()
        self.client = mock.Mock()
        self.client.containers.execute_session.return_value = self.session
        self.stderr = mock.Mock(buffer=io.BytesIO())
        self.useFixture_patch('sys.stderr', self.stderr)

    def useFixture_patch(self, target, value):
        patcher = mock.patch(target, value)
        patcher.start()
        self.addCleanup(patcher.stop)

    def use_terminal(self):
        master, slave = os.openpty()
        self.addCleanup(os.close, slave)
        set_size(slave, 80, 24)
        self.useFixture_patch('sys.stdin', mock.Mock(fileno=lambda: slave))
        self.stdout = mock.Mock(fileno=lambda: slave, buffer=io.BytesIO())
        self.useFixture_patch('sys.stdout', self.stdout)
        return master, slave

    def run_with(self, driver):
        """Run exec_interactive here, and driver on another thread."""
        errors = []

        def drive():
            try:
                driver()
            except Exception as e:
                errors.append(e)
            finally:
                self.session.exited.set()
        thread = threading.Thread(target=drive, daemon=True)
        thread.start()
        try:
            return terminal.exec_interactive(self.client, 'c', ['sh'])
        finally:
            thread.join(TIMEOUT)
            if errors:
                raise errors[0]

    def test_terminal(self):
        master, slave = self.use_terminal()
        self.addCleanup(os.close, master)
        saved = termios.tcgetattr(slave)
        handler = signal.getsignal(signal.SIGWINCH)

        def driver():
            eventually(lambda: self.session.sizes == [(80, 24)])
            # Raw mode: no line editing, and Ctrl-C is a byte to send.
            self.assertFalse(termios.tcgetattr(slave)[3] & termios.ICANON)
            os.write(master, b'ls\r\x03')
            eventually(lambda: b''.join(self.session.sent) == b'ls\r\x03')
            set_size(slave, 100, 30)
            os.kill(os.getpid(), signal.SIGWINCH)
            eventually(lambda: self.session.sizes[-1:] == [(100, 30)])
            self.session.exit_code = 130
        self.assertEqual(130, self.run_with(driver))
        self.client.containers.execute_session.assert_called_once_with(
            'c', ['sh'], stdin=k8s_exec.PIPE, stdout=self.stdout.buffer,
            stderr=self.stderr.buffer, tty=True)
        self.assertEqual(saved, termios.tcgetattr(slave))
        self.assertEqual(handler, signal.getsignal(signal.SIGWINCH))
        self.assertTrue(self.session.closed)

    def test_terminal_hangs_up(self):
        # The command keeps running, as with docker exec -it.
        master, slave = self.use_terminal()
        threading.Thread(target=lambda: (
            eventually(lambda: self.session.sizes), os.close(master)),
            daemon=True).start()
        e = self.assertRaises(exceptions.Disconnected,
                              terminal.exec_interactive, self.client, 'c',
                              ['sh'])
        self.assertIn('terminal closed', str(e))
        self.assertTrue(self.session.closed)

    def test_input_after_exit_is_dropped(self):
        master, slave = self.use_terminal()
        self.addCleanup(os.close, master)

        def driver():
            eventually(lambda: self.session.sizes)
            self.session.exit_code = 3
            self.session.exited.set()
            os.write(master, b'late')
        self.assertEqual(3, self.run_with(driver))

    def test_piped_input(self):
        # As kubectl does: a warning, then no terminal, and the end of our
        # input ends the command's.
        self.session = FakeSession(exit_on_stdin_close=True)
        self.client.containers.execute_session.return_value = self.session
        read_end, write_end = os.pipe()
        self.addCleanup(os.close, read_end)
        os.write(write_end, b'echo hi\n')
        os.close(write_end)
        self.useFixture_patch('sys.stdin', mock.Mock(fileno=lambda: read_end))
        stdout = mock.Mock(buffer=io.BytesIO())
        self.useFixture_patch('sys.stdout', stdout)
        self.assertEqual(0, terminal.exec_interactive(self.client, 'c',
                                                      ['sh']))
        self.client.containers.execute_session.assert_called_once_with(
            'c', ['sh'], stdin=k8s_exec.PIPE, stdout=stdout.buffer,
            stderr=self.stderr.buffer, tty=False)
        self.assertEqual([b'echo hi\n'], self.session.sent)
        self.assertTrue(self.session.stdin_closed)
        self.assertEqual([], self.session.sizes)
        self.stderr.write.assert_called_once_with(
            'Unable to use a TTY - input is not a terminal\n')
