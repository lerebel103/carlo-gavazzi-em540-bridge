"""PTY-based virtual serial port helpers for integration testing."""

from __future__ import annotations

import errno
import os
import pty
import selectors
import struct
import termios
import threading
import time


def disable_pty_echo(fd: int) -> None:
    """Disable echo and canonical mode on a PTY file descriptor."""
    attrs = termios.tcgetattr(fd)
    attrs[0] &= ~(termios.IXON | termios.IXOFF | termios.ICRNL | termios.INLCR)
    attrs[1] &= ~termios.OPOST
    attrs[3] &= ~(termios.ECHO | termios.ECHOE | termios.ECHOK | termios.ECHONL | termios.ICANON)
    termios.tcsetattr(fd, termios.TCSANOW, attrs)


class SerialCable:
    """Virtual serial cable: two PTY pairs with a relay thread bridging them.

    Both slave fds are kept open so the PTY line discipline stays active.
    The relay thread forwards raw bytes between the two master fds.
    """

    def __init__(self, label: str) -> None:
        self.label = label
        self._left_master, left_slave = pty.openpty()
        self._right_master, right_slave = pty.openpty()
        disable_pty_echo(left_slave)
        disable_pty_echo(right_slave)
        self.left_path = os.ttyname(left_slave)
        self.right_path = os.ttyname(right_slave)
        # Close slave fds immediately.  The PTYs stay alive via the master fds.
        # Not holding slave fds allows subprocesses to exclusively lock either
        # path without hitting macOS EAGAIN from the parent holding the fd.
        os.close(left_slave)
        os.close(right_slave)
        os.set_blocking(self._left_master, False)
        os.set_blocking(self._right_master, False)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"serial-cable-{label}")
        self._thread.start()

    def _write_all(self, fd: int, payload: bytes) -> None:
        """Write payload to fd, handling potential partial writes."""
        os.write(fd, payload)

    def _run(self) -> None:
        """Relay thread: forward bytes between left and right PTYs."""
        selector = selectors.DefaultSelector()
        selector.register(self._left_master, selectors.EVENT_READ, self._right_master)
        selector.register(self._right_master, selectors.EVENT_READ, self._left_master)
        try:
            while not self._stop.is_set():
                for key, _ in selector.select(timeout=0.1):
                    try:
                        data = os.read(key.fileobj, 4096)
                    except BlockingIOError:
                        continue
                    except OSError:
                        return
                    if not data:
                        continue
                    self._write_all(key.data, data)
        finally:
            selector.close()

    def close(self) -> None:
        """Stop the relay thread and close file descriptors."""
        self._stop.set()
        self._thread.join(timeout=2.0)
        for fd in (self._left_master, self._right_master):
            try:
                os.close(fd)
            except OSError:
                pass

    @property
    def pty_fds(self) -> tuple[int, ...]:
        """Return all PTY fds held by this cable (for preexec_fn in subprocess.Popen)."""
        return (self._left_master, self._right_master)


def compute_modbus_crc(payload: bytes) -> bytes:
    """Compute CRC-16 checksum for Modbus RTU."""
    crc = 0xFFFF
    for byte in payload:
        crc ^= byte
        for _ in range(8):
            if crc & 1:
                crc = (crc >> 1) ^ 0xA001
            else:
                crc >>= 1
    return struct.pack("<H", crc)


class ModbusRtuServer:
    """Lightweight Modbus RTU server over a PTY.

    Implements Modbus function code 3 (read holding registers) to serve
    a register map to connected clients.
    """

    def __init__(self, label: str, slave_id: int) -> None:
        self.label = label
        self.slave_id = slave_id
        self._master_fd, slave_fd = pty.openpty()
        disable_pty_echo(slave_fd)
        self.port = os.ttyname(slave_fd)
        os.close(slave_fd)
        self._thread: threading.Thread | None = None
        self._ready = threading.Event()
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._registers: dict[int, int] = {}
        self._requests: list[tuple[float, int, int]] = []
        self._connect_events: list[bool] = []

    def _trace_connect(self, connect: bool) -> None:
        """Record a connection state change."""
        with self._lock:
            self._connect_events.append(connect)

    def set_registers(self, registers: dict[int, int]) -> None:
        """Replace the register map."""
        with self._lock:
            self._registers = dict(registers)

    def start(self) -> None:
        """Start the server thread."""

        def _thread_main() -> None:
            try:
                self._trace_connect(True)
                self._ready.set()
                buffer = bytearray()
                while not self._stop.is_set():
                    try:
                        chunk = os.read(self._master_fd, 256)
                    except OSError as exc:
                        # Linux PTY masters raise EIO/ENXIO while no slave endpoint
                        # is opened yet. Keep waiting so the harness can recover once
                        # the client opens the slave device path.
                        if exc.errno in (errno.EIO, errno.ENXIO):
                            time.sleep(0.01)
                            continue
                        break
                    if not chunk:
                        continue
                    buffer.extend(chunk)
                    self._process_requests(buffer)
            finally:
                self._trace_connect(False)

        self._thread = threading.Thread(target=_thread_main, daemon=True, name=f"modbus-rtu-{self.label}")
        self._thread.start()
        if not self._ready.wait(5.0):
            raise TimeoutError(f"{self.label} Modbus RTU server did not start")

    def _process_requests(self, buffer: bytearray) -> None:
        """Process one or more complete Modbus RTU requests from the buffer."""
        while len(buffer) >= 8:
            request = bytes(buffer[:8])
            if compute_modbus_crc(request[:6]) != request[6:8]:
                del buffer[0]
                continue
            unit_id, function_code = request[0], request[1]
            if unit_id != self.slave_id or function_code != 3:
                del buffer[:8]
                continue
            address = (request[2] << 8) | request[3]
            count = (request[4] << 8) | request[5]
            with self._lock:
                registers = [self._registers.get(addr, 0) for addr in range(address, address + count)]
                self._requests.append((time.monotonic(), address, count))
            response = self._build_response(unit_id, function_code, registers)
            os.write(self._master_fd, response)
            del buffer[:8]

    def _build_response(self, unit_id: int, function_code: int, registers: list[int]) -> bytes:
        """Build a Modbus RTU response frame."""
        response = bytearray([unit_id, function_code, len(registers) * 2])
        for value in registers:
            response.extend(struct.pack(">H", value & 0xFFFF))
        response.extend(compute_modbus_crc(bytes(response)))
        return bytes(response)

    def stop(self) -> None:
        """Stop the server thread and close file descriptors."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
        try:
            os.close(self._master_fd)
        except OSError:
            pass

    @property
    def pty_fds(self) -> tuple[int, ...]:
        """Return all PTY fds held by this server (for preexec_fn in subprocess.Popen)."""
        return (self._master_fd,)

    @property
    def requests(self) -> list[tuple[float, int, int]]:
        """Return a snapshot of all requests received."""
        with self._lock:
            return list(self._requests)

    @property
    def connect_events(self) -> list[bool]:
        """Return a snapshot of all connection state changes."""
        with self._lock:
            return list(self._connect_events)
