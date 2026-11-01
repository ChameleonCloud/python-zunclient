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

"""Run a command in a container with this process's stdin and stdout.

This is exec --interactive in the CLI, like docker exec -it and kubectl
exec -it. If stdin is a terminal, the command runs in a terminal of the
same size, which follows ours when it is resized, and ours is put in raw
mode, so that every key, Ctrl-C included, goes to the command. If stdin
isn't a terminal, as when input is piped in, the command runs without one,
as kubectl does, and sees the end of its input when ours ends.
"""

import contextlib
import os
import select
import signal
import sys
import termios
import threading
import tty

from zunclient.common.websocketclient import exceptions
from zunclient.common.websocketclient import k8s_exec

READ_SIZE = 4096


def exec_interactive(client, container, args):
    """Run a command with our stdin, stdout and stderr; return its exit code.

    :raises k8s_exec.SessionError: if the session ended without the
        command's exit code.
    """
    in_fd = sys.stdin.fileno()
    use_tty = os.isatty(in_fd)
    if not use_tty:
        sys.stderr.write("Unable to use a TTY - input is not a terminal\n")
        sys.stderr.flush()
    session = client.containers.execute_session(
        container, args, stdin=k8s_exec.PIPE, stdout=sys.stdout.buffer,
        stderr=sys.stderr.buffer, tty=use_tty)
    # The session's end and resizes wake the loop through this pipe.
    wake_r, wake_w = os.pipe()
    os.set_blocking(wake_w, False)
    watcher = threading.Thread(target=_wake_at_exit, args=(session, wake_w),
                               name='k8s-exec-exit', daemon=True)
    watcher.start()
    try:
        if use_tty:
            with _follow_size(session, wake_w), _raw(in_fd):
                ended = _relay(session, in_fd, wake_r, use_tty)
        else:
            ended = _relay(session, in_fd, wake_r, use_tty)
        if not ended:
            # Like a closed docker exec -it, the command keeps running.
            raise exceptions.Disconnected(message="The terminal closed")
        return session.wait()
    finally:
        session.close()
        # Close the pipe only once nothing can write to it, so that a late
        # write can't land in a file that reused its number.
        watcher.join(10)
        os.close(wake_r)
        if not watcher.is_alive():
            os.close(wake_w)


def _relay(session, in_fd, wake_r, use_tty):
    """Send our input to the command until it exits.

    Return True once the session has ended, or False if our terminal
    closed first.
    """
    fds = [wake_r, in_fd]
    while True:
        ready = select.select(fds, [], [])[0]
        if wake_r in ready:
            woken = os.read(wake_r, 64)
            if b'x' in woken:
                return True
            if b'r' in woken:
                _send_size(session)
        if in_fd not in ready:
            continue
        try:
            data = os.read(in_fd, READ_SIZE)
        except OSError:
            data = b''  # the terminal is gone
        try:
            if data:
                session.send(data)
            elif use_tty:
                return False  # the terminal hung up
            else:
                session.close_stdin()
                fds = [wake_r]
        except exceptions.Disconnected:
            return True  # the command has exited; wait() says how


def _wake_at_exit(session, wake_w):
    try:
        session.wait()
    except Exception:
        pass  # exec_interactive's wait() raises it
    _wake(wake_w, b'x')


def _wake(wake_w, why):
    try:
        os.write(wake_w, why)
    except OSError:
        pass  # full, so the loop is already awake


@contextlib.contextmanager
def _follow_size(session, wake_w):
    """Size the command's terminal like ours, and keep it so."""
    _send_size(session)
    try:
        previous = signal.signal(signal.SIGWINCH,
                                 lambda signum, frame: _wake(wake_w, b'r'))
    except ValueError:
        # Not on the main thread, where signals are handled: keep the
        # first size.
        yield
        return
    try:
        yield
    finally:
        signal.signal(signal.SIGWINCH, previous)


def _send_size(session):
    for stream in (sys.stdout, sys.stdin):
        try:
            size = os.get_terminal_size(stream.fileno())
        except (OSError, ValueError):
            continue
        try:
            session.resize(size.columns, size.lines)
        except exceptions.Disconnected:
            pass
        return


@contextlib.contextmanager
def _raw(fd):
    """Put our terminal in raw mode, and restore it afterwards."""
    saved = termios.tcgetattr(fd)
    tty.setraw(fd)
    try:
        yield
    finally:
        try:
            termios.tcsetattr(fd, termios.TCSADRAIN, saved)
        except termios.error:
            pass  # the terminal is gone
