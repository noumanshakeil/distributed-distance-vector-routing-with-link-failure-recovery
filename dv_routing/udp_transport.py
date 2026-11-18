"""UDP transport and wire protocol abstraction for distributed distance-vector routing."""

import json
import socket
import threading
from typing import Callable, Tuple, Any, Dict, Optional

MSG_TYPE_ADVERT = "ADVERT"
MSG_TYPE_DATA = "DATA"
MSG_TYPE_CONTROL = "CONTROL"


class UDPTransport:
    """UDP datagram transport abstraction over IPv4.
    
    Binds to a specified (host, port), listens for JSON-encoded datagrams in a background
    daemon thread, and dispatches them to an on_message callback.
    """

    def __init__(
        self,
        host: str,
        port: int,
        on_message: Callable[[dict, Tuple[str, int]], None],
    ) -> None:
        self.host = host
        self.port = port
        self.on_message = on_message
        self._sock: Optional[socket.socket] = None
        self._thread: Optional[threading.Thread] = None
        self._running: bool = False
        self._lock = threading.Lock()

    def start(self) -> None:
        """Binds the UDP socket with SO_REUSEADDR and starts the listener thread."""
        with self._lock:
            if self._running:
                return

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Suppress WSAECONNRESET on Windows when remote port is unreachable
            if hasattr(socket, "SIO_UDP_CONNRESET"):
                try:
                    sock.ioctl(socket.SIO_UDP_CONNRESET, False)
                except Exception:
                    pass

            sock.bind((self.host, self.port))
            # Capture actual port bound (in case port 0 was passed)
            self.port = sock.getsockname()[1]
            sock.settimeout(0.5)

            self._sock = sock
            self._running = True
            self._thread = threading.Thread(
                target=self._listen_loop,
                daemon=True,
                name=f"UDPTransport-{self.host}:{self.port}",
            )
            self._thread.start()

    def _listen_loop(self) -> None:
        """Background thread loop receiving UDP datagrams and dispatching parsed JSON."""
        while self._running:
            try:
                sock = self._sock
                if sock is None:
                    break

                try:
                    data, addr = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except (ConnectionResetError, ConnectionRefusedError):
                    continue
                except OSError:
                    # Socket closed or invalid file descriptor
                    break

                try:
                    payload = json.loads(data.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    # Malformed or non-JSON datagram; drop gracefully
                    continue

                try:
                    self.on_message(payload, addr)
                except Exception:
                    # Suppress callback exceptions to preserve listener thread stability
                    pass

            except Exception:
                break

    def send_json(self, dest_host: str, dest_port: int, payload: dict) -> None:
        """Encodes payload as UTF-8 JSON bytes and transmits to (dest_host, dest_port)."""
        data = json.dumps(payload).encode("utf-8")
        sock = self._sock
        if sock is None or not self._running:
            raise RuntimeError("Cannot send_json: UDPTransport is not running")
        sock.sendto(data, (dest_host, dest_port))

    def close(self) -> None:
        """Stops the listener loop, closes the socket, and joins the listener thread."""
        with self._lock:
            if not self._running and self._sock is None:
                return
            self._running = False
            sock = self._sock
            self._sock = None

        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass

        if self._thread is not None and self._thread.is_alive():
            if threading.current_thread() != self._thread:
                self._thread.join(timeout=2.0)
