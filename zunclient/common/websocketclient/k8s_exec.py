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

"""Run a command in a container and stream its input and output.

The session speaks Kubernetes' remotecommand protocol, v5.channel.k8s.io,
through Zun's websocket proxy. Each websocket message is one channel byte
followed by a payload.

How a session works:

* A reader thread, started with the session, is the only code that receives
  from the websocket and the only code that changes the session's state. It
  hands stdout and stderr to sinks or pipes, or queues them for recv(), and
  keeps the Status message that carries the exit code.
* Any thread may write: send(), close_stdin() and resize(), or the stdin
  pipe.
* wait() blocks until the reader has finished and returns the exit code.

Because something is always reading, a command can't stall with its output
unread while the client is still writing its input. To pipe a large stream
through a command, give the session sinks, or use communicate(), which
writes from a second thread while it reads.
"""

import io
import json
import queue
import socket
import threading
import time

from oslo_log import log as logging
import websocket

from zunclient.common.websocketclient import exceptions

LOG = logging.getLogger(__name__)

V5_PROTOCOL = 'v5.channel.k8s.io'

# Channel numbers, the first byte of every message. ERROR carries the
# command's result as a Kubernetes Status in JSON, sent for success too. A
# message on CLOSE closes the channel named in its one-byte payload.
STDIN = 0
STDOUT = 1
STDERR = 2
ERROR = 3
RESIZE = 4
CLOSE = 255

# Large writes are split into messages of this size, so that a resize or a
# close can go out between them instead of waiting behind one huge message.
CHUNK_SIZE = 64 * 1024

# How many output messages recv() may fall behind before the reader waits.
# A full queue slows the command down rather than filling memory.
QUEUE_SIZE = 64

_END = object()  # put on the queue after the last output message

# Special value for the stdin, stdout or stderr argument of connect() and
# execute_session(): open a pipe to that stream. The session's stdin,
# stdout or stderr attribute is then a binary file to write or read, like
# subprocess.Popen's with subprocess.PIPE; otherwise the attribute is None.
PIPE = -1


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
    """A timeout passed while the command was still running.

    The command isn't stopped; it keeps running in the container.

    :ivar timeout: the timeout, in seconds.
    :ivar stdout: the output communicate() had read, as bytes, or None if
        recv() or wait() timed out; likewise stderr.
    """

    def __init__(self, timeout, stdout=None, stderr=None):
        self.timeout = timeout
        self.stdout = stdout
        self.stderr = stderr
        super(ExecTimeout, self).__init__(
            message="The command was still running after %s seconds"
                    % timeout)


def connect(url, stdin=None, stdout=None, stderr=None, timeout=30):
    """Start an exec session at the URL that containers.execute returned.

    :param stdin: PIPE, for a stdin pipe as session.stdin. Without one,
        write with send() and close_stdin().
    :param stdout: where stdout goes: a function that takes bytes, a binary
        file, or PIPE, for a pipe to read as session.stdout. If None, read
        it with recv() or communicate().
    :param stderr: the same, for stderr.
    :param timeout: seconds to wait for the connection to open.
    """
    # No Origin header: this isn't a browser, and Zun's proxy accepts a
    # client without one.
    ws = websocket.create_connection(url, subprotocols=[V5_PROTOCOL],
                                     timeout=timeout, suppress_origin=True)
    # The timeout was for connecting. The reader waits for output as long
    # as the command runs; use wait(timeout) to bound that instead.
    ws.settimeout(None)
    return ExecSession(ws, stdin=stdin, stdout=stdout, stderr=stderr)


class ExecSession(object):
    """A command running in a container.

    :ivar exit_code: the command's exit code, once it has exited.
    :ivar status: the Kubernetes Status, as a dict, once it has arrived.
    :ivar stdin: with stdin=PIPE, a binary file that writes to the command's
        stdin. It is buffered: flush() sends what has been written, and
        close() sends it and closes stdin. Otherwise None.
    :ivar stdout: with stdout=PIPE, a binary file to read the command's
        stdout from, until the command exits. Read it, or the command waits
        for room to write, as with any pipe. Otherwise None.
    :ivar stderr: the same, for stderr.
    """

    def __init__(self, ws, stdin=None, stdout=None, stderr=None):
        self._ws = ws
        self._pipes = {}
        sinks = {}
        for channel, target in ((STDOUT, stdout), (STDERR, stderr)):
            if target == PIPE:
                self._pipes[channel] = _OutputPipe()
                sinks[channel] = self._pipes[channel].put
            else:
                sinks[channel] = _as_sink(target)
        self._sinks = sinks
        self.stdin = None
        if stdin == PIPE:
            self.stdin = io.BufferedWriter(_InputPipe(self), CHUNK_SIZE)
        self.stdout = self._reader_for(STDOUT)
        self.stderr = self._reader_for(STDERR)
        self._output = queue.Queue(QUEUE_SIZE)
        self._discard = False  # set by close(); later output is dropped
        self._done = threading.Event()
        self._ended = None  # how the connection ended, as text
        self._sink_error = None  # what a sink raised, for wait()
        self.status = None
        self.exit_code = None
        self._reader = threading.Thread(target=self._read,
                                        name='k8s-exec-reader', daemon=True)
        self._reader.start()

    def _reader_for(self, channel):
        pipe = self._pipes.get(channel)
        return io.BufferedReader(pipe) if pipe else None

    # Reading. Only the reader thread runs _read and _deliver.

    def _read(self):
        try:
            self._ended = self._read_messages()
        except Exception as e:
            # A sink raised. Stop reading; wait() reports the error.
            LOG.debug("Exec session reader stopped", exc_info=True)
            self._sink_error = e
            self._ended = "stopped reading: %r" % e
        finally:
            self.exit_code = _exit_code(self.status)
            # Close the socket here, not only in close(): after the
            # server's close message, websocket-client marks the socket as
            # disconnected and its close() would no longer shut it down.
            self._ws.shutdown()
            self._done.set()
            self._output.put(_END)
            for pipe in self._pipes.values():
                pipe.end()

    def _read_messages(self):
        """Receive until the connection ends; return how it ended."""
        while True:
            try:
                opcode, data = self._ws.recv_data()
            except (OSError, websocket.WebSocketException) as e:
                # The connection was lost. If the exit code had already
                # arrived, it is still the answer.
                return repr(e)
            if opcode == websocket.ABNF.OPCODE_CLOSE:
                return _close_reason(data)
            if not data:
                continue
            channel, payload = data[0], data[1:]
            if channel in (STDOUT, STDERR):
                # Kubernetes opens each stream with an empty message.
                if payload:
                    self._deliver(channel, payload)
            elif channel == ERROR:
                self.status = _parse_status(payload)

    def _deliver(self, channel, payload):
        sink = self._sinks[channel]
        if sink is not None:
            sink(payload)
        elif not self._discard:
            self._output.put((channel, payload))

    def recv(self, timeout=None):
        """Return the next (channel, data) of output that has no sink.

        data is the bytes that arrived together, not necessarily whole lines.
        Once the command has exited, return None and set exit_code.

        :raises SessionError: if the session ended without the exit code.
        :raises ExecTimeout: if nothing arrived within timeout seconds.
        """
        try:
            message = self._output.get(timeout=timeout)
        except queue.Empty:
            raise ExecTimeout(timeout)
        if message is _END:
            self._output.put(_END)  # so that a later recv() ends too
            self.wait()
            return None
        return message

    def wait(self, timeout=None):
        """Block until the command exits; return its exit code.

        If a sink raised an exception, wait() raises it.

        :raises SessionError: if the session ended without the exit code.
        :raises ExecTimeout: if the command is still running after timeout
            seconds. The session stays open.
        """
        if not self._done.wait(timeout):
            raise ExecTimeout(timeout)
        if self._sink_error is not None:
            raise self._sink_error
        if self.exit_code is None:
            raise SessionError(self.status, self._ended)
        return self.exit_code

    # Writing. Any thread may call these.

    def send(self, data):
        """Write bytes to the command's stdin.

        :raises exceptions.Disconnected: if the session has ended.
        """
        view = memoryview(data)
        for start in range(0, len(view), CHUNK_SIZE):
            self._send(STDIN, view[start:start + CHUNK_SIZE])

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
        # websocket-client doesn't fail a send after the server's close
        # message, so check whether the reader has seen the end.
        if self._done.is_set():
            raise exceptions.Disconnected(self._ended)
        try:
            self._ws.send_binary(bytes([channel]) + bytes(payload))
        except (OSError, websocket.WebSocketException) as e:
            raise exceptions.Disconnected(e)

    # Whole-command helpers.

    def communicate(self, input=None, timeout=None):
        """Write input, close stdin, and read until the command exits.

        Like subprocess.Popen.communicate. Input is written from a second
        thread while this one reads, so neither side waits on the other.
        If reading the input raises an exception, communicate() raises it
        once the command has exited.

        :param input: bytes, or a binary file to copy from. If None, stdin
            is left as it is.
        :param timeout: seconds to wait for the command to exit.
        :returns: (stdout, stderr) as bytes. A channel that has a sink or
            a pipe returns b''.
        :raises SessionError: if the session ended without the exit code.
        :raises ExecTimeout: if the command is still running after timeout
            seconds, with the output read so far. The session stays open.
        """
        writer, input_errors = None, []
        if input is not None:
            writer = threading.Thread(target=self._write_input,
                                      args=(input, input_errors),
                                      name='k8s-exec-writer', daemon=True)
            writer.start()
        deadline = _Deadline(timeout)
        output = {STDOUT: [], STDERR: []}
        while True:
            try:
                message = self.recv(timeout=deadline.remaining())
            except ExecTimeout:
                raise ExecTimeout(timeout, b''.join(output[STDOUT]),
                                  b''.join(output[STDERR]))
            if message is None:
                break
            channel, data = message
            output[channel].append(data)
        if writer is not None:
            writer.join()
            if input_errors:
                raise input_errors[0]
        return b''.join(output[STDOUT]), b''.join(output[STDERR])

    def read_all(self):
        """Block until the command exits; return (stdout, stderr) as bytes."""
        return self.communicate()

    def _write_input(self, input, errors):
        try:
            if isinstance(input, (bytes, bytearray, memoryview)):
                self.send(input)
            else:
                while True:
                    data = input.read(CHUNK_SIZE)
                    if not data:
                        break
                    self.send(data)
            self.close_stdin()
        except exceptions.Disconnected:
            # The command exited before reading all its input, like a
            # broken pipe. wait() reports how the session ended.
            pass
        except Exception as e:
            # Reading the input failed. Close stdin so that the command
            # doesn't wait for the rest, and let communicate() raise it.
            errors.append(e)
            try:
                self.close_stdin()
            except exceptions.Disconnected:
                pass

    def close(self, timeout=3):
        """End the session. The command keeps running if it hasn't exited.

        Output that hasn't been read is dropped, and the stdout and stderr
        pipes end.
        """
        self._discard = True
        for pipe in self._pipes.values():
            pipe.discard()
        self._drain()  # frees the reader if it is waiting for queue room
        if not self._done.is_set():
            try:
                self._ws.send_close()
            except (OSError, websocket.WebSocketException):
                pass
            if threading.current_thread() is not self._reader:
                # (A sink that calls close() runs on the reader thread.)
                self._done.wait(timeout)
                self._drain()
        # websocket-client's shutdown() doesn't wake a thread blocked
        # receiving, so shut the socket down underneath the reader, which
        # then releases it.
        sock = self._ws.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    def _drain(self):
        while True:
            try:
                self._output.get_nowait()
            except queue.Empty:
                break
        if self._done.is_set():
            # Keep the end marker, so a recv() after close() returns None
            # instead of waiting forever.
            self._output.put_nowait(_END)


class _Deadline(object):
    def __init__(self, timeout):
        self._timeout = timeout
        self._start = time.monotonic()

    def remaining(self):
        if self._timeout is None:
            return None
        return max(0, self._timeout - (time.monotonic() - self._start))


class _OutputPipe(io.RawIOBase):
    """The read end of a pipe from stdout or stderr.

    The reader thread puts output in with put() and end(); the caller reads
    it, through an io.BufferedReader, until the command exits.
    """

    def __init__(self):
        super(_OutputPipe, self).__init__()
        self._chunks = queue.Queue(QUEUE_SIZE)
        self._rest = b''
        self._eof = False
        self._discard = False  # set by close(); later output is dropped

    def readable(self):
        return True

    def readinto(self, buffer):
        if not self._rest and not self._eof:
            chunk = self._chunks.get()
            if chunk is _END:
                self._eof = True
            else:
                self._rest = chunk
        n = min(len(buffer), len(self._rest))
        buffer[:n] = self._rest[:n]
        self._rest = self._rest[n:]
        return n

    def put(self, data):
        if not self._discard:
            self._chunks.put(data)

    def end(self):
        if self._discard:
            self._put_end()
        else:
            self._chunks.put(_END)

    def discard(self):
        """Drop unread output, and end the pipe for its reader."""
        self._discard = True
        while True:
            try:
                self._chunks.get_nowait()
            except queue.Empty:
                break
        self._put_end()

    def _put_end(self):
        try:
            self._chunks.put_nowait(_END)
        except queue.Full:
            pass


class _InputPipe(io.RawIOBase):
    """The write end of a pipe to stdin: writes are sends."""

    def __init__(self, session):
        super(_InputPipe, self).__init__()
        self._session = session

    def writable(self):
        return True

    def write(self, data):
        self._session.send(data)
        return len(data)

    def close(self):
        if not self.closed:
            try:
                # After the command has exited there is no stdin to close.
                # (This also keeps garbage collection from raising.)
                if not self._session._done.is_set():
                    self._session.close_stdin()
            finally:
                super(_InputPipe, self).close()


def _as_sink(target):
    """A function that takes bytes, from a function or a binary file."""
    if target is None or not hasattr(target, 'write'):
        return target

    def write(data):
        target.write(data)
        target.flush()
    return write


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
