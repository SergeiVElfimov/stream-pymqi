"""Connection module for stream-pymqi."""

import contextlib
import threading
import time
from dataclasses import dataclass
from typing import Any

import pymqi

from stream_pymqi.config import StreamConfig


@dataclass
class Connection:
    """IBM MQ connection manager."""

    config: StreamConfig
    _queue_manager: Any | None = None
    _connected: bool = False
    _reconnect_thread: threading.Thread | None = None
    _stop_reconnect: threading.Event | None = None

    def connect(self) -> None:
        """Connect to IBM MQ queue manager."""
        if self._connected:
            return

        try:
            self._queue_manager = pymqi.QueueManager(None)
            self._queue_manager.connect_tcp_client(
                self.config.queue_manager,
                pymqi.CD(),
                self.config.channel,
                f"{self.config.host}({self.config.port})",
                self.config.user,
                self.config.password,
            )

            self._connected = True
        except pymqi.MQMIError:
            if self.config.auto_reconnect:
                self._start_reconnect()
            raise

    def disconnect(self) -> None:
        """Disconnect from IBM MQ queue manager."""
        if self._stop_reconnect:
            self._stop_reconnect.set()
        if self._reconnect_thread:
            self._reconnect_thread.join(timeout=5)

        if self._queue_manager:
            with contextlib.suppress(pymqi.MQMIError, pymqi.PYIFError):
                self._queue_manager.disconnect()
            self._queue_manager = None

        self._connected = False

    def _start_reconnect(self) -> None:
        """Start reconnect thread."""
        self._stop_reconnect = threading.Event()
        self._reconnect_thread = threading.Thread(
            target=self._reconnect_loop,
            daemon=True,
        )
        self._reconnect_thread.start()

    def _reconnect_loop(self) -> None:
        """Reconnect loop."""
        while self._stop_reconnect and not self._stop_reconnect.is_set():
            try:
                self.disconnect()
                time.sleep(self.config.reconnect_interval)
                self.connect()
                break
            except Exception:
                pass

    @property
    def queue_manager(self) -> Any:
        """Get queue manager."""
        if not self._connected:
            self.connect()
        return self._queue_manager

    def __enter__(self) -> "Connection":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
