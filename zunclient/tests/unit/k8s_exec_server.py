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

"""A small websocket server that acts like a Kubernetes exec endpoint.

It serves one connection on a real TCP socket, so tests see real socket
buffers: a client that writes without reading stalls here just as it would
against Kubernetes.
"""

import base64
import hashlib
import json
import socket
import struct
import threading

SUCCESS = {'metadata': {}, 'status': 'Success'}

_GUID = b'258EAFA5-E914-47DA-95CA-C5AB0DC85B11'
_BINARY, _CLOSE, _PING, _PONG = 0x2, 0x8, 0x9, 0xA


class ExecServer(object):
    """Run command(conn) for the one client that connects.

    command gets a _Connection, reads stdin with conn.stdin(), writes with
    conn.stdout(data), and returns the Status to send before closing.
    """

    def __init__(self, command):
        self._command = command
        self._listener = socket.create_server(('127.0.0.1', 0))
        self.url = 'ws://127.0.0.1:%d/exec' % self._listener.getsockname()[1]
        self.error = None
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def join(self, timeout=10):
        self._thread.join(timeout)
        self._listener.close()
        if self.error:
            raise self.error

    def _serve(self):
        sock, _ = self._listener.accept()
        try:
            conn = _Connection(sock)
            conn.handshake()
            status = self._command(conn)
            conn.send(_BINARY, b'\x03' + json.dumps(status).encode())
            conn.send(_CLOSE, struct.pack('!H', 1000))
            conn.wait_for_close()
        except Exception as e:
            self.error = e
        finally:
            sock.close()


class _Connection(object):

    def __init__(self, sock):
        self._sock = sock
        self._file = sock.makefile('rb')

    def handshake(self):
        headers = {}
        while True:
            line = self._file.readline().strip()
            if not line:
                break
            name, _, value = line.partition(b':')
            headers[name.strip().lower()] = value.strip()
        assert b'v5.channel.k8s.io' in headers[b'sec-websocket-protocol']
        accept = base64.b64encode(hashlib.sha1(
            headers[b'sec-websocket-key'] + _GUID).digest())
        self._sock.sendall(
            b'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n'
            b'Connection: Upgrade\r\nSec-WebSocket-Accept: ' + accept +
            b'\r\nSec-WebSocket-Protocol: v5.channel.k8s.io\r\n\r\n')

    def stdin(self):
        """The next stdin data, or None once the client closes stdin."""
        while True:
            opcode, data = self._recv()
            if opcode == _PING:
                self.send(_PONG, data)
            elif data[:1] == b'\x00':
                return data[1:]
            elif data == b'\xff\x00':
                return None

    def stdout(self, data):
        self.send(_BINARY, b'\x01' + data)

    def stderr(self, data):
        self.send(_BINARY, b'\x02' + data)

    def send(self, opcode, payload):
        n = len(payload)
        if n < 126:
            header = struct.pack('!BB', 0x80 | opcode, n)
        elif n < 1 << 16:
            header = struct.pack('!BBH', 0x80 | opcode, 126, n)
        else:
            header = struct.pack('!BBQ', 0x80 | opcode, 127, n)
        self._sock.sendall(header + payload)

    def wait_for_close(self):
        while self._recv()[0] != _CLOSE:
            pass

    def _recv(self):
        b0, b1 = self._read(2)
        assert b0 & 0x80, "fragmented client messages aren't supported"
        n = b1 & 0x7F
        if n == 126:
            n, = struct.unpack('!H', self._read(2))
        elif n == 127:
            n, = struct.unpack('!Q', self._read(8))
        mask = self._read(4)  # clients always mask
        data = self._read(n)
        # Unmask all at once: XOR with the mask repeated to the same length.
        key = (mask * (n // 4 + 1))[:n]
        data = (int.from_bytes(data, 'big') ^
                int.from_bytes(key, 'big')).to_bytes(n, 'big')
        return b0 & 0x0F, data

    def _read(self, n):
        data = self._file.read(n)
        if len(data) < n:
            raise ConnectionError("client went away")
        return data
