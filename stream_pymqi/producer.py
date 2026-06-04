"""Producer module for stream-pymqi - Synchronous FastStream-like interface."""

from dataclasses import dataclass
from typing import Any

import pymqi

from stream_pymqi.config import StreamConfig
from stream_pymqi.connection import Connection
from stream_pymqi.message import Message


@dataclass
class Producer:
    """IBM MQ Producer with FastStream-like synchronous interface."""

    config: StreamConfig
    _connection: Connection | None = None
    _running: bool = False

    def connect(self) -> None:
        """Connect to IBM MQ."""
        if self._running:
            return

        self._connection = Connection(self.config)
        self._connection.connect()
        self._running = True

    def disconnect(self) -> None:
        """Disconnect from IBM MQ."""
        self._running = False
        if self._connection:
            self._connection.disconnect()
            self._connection = None

    def send(self, message: Message) -> None:
        """Send message to queue.

        Args:
            message: Message object to send.
        """
        if not self._running:
            self.connect()

        # Put message to queue
        if self._connection:
            self._connection.queue_manager.put(
                self.config.queue_name, message.data, pymqi.CMQC.MQMD_NONE, pymqi.CMQC.MQPMO_NONE
            )

    def send_string(self, data: str) -> None:
        """Send string as message.

        Args:
            data: String data to send.
        """
        message = Message.from_string(data)
        self.send(message)

    def send_json(self, data: Any) -> None:
        """Send JSON-serializable data as message.

        Args:
            data: JSON-serializable data to send.
        """
        message = Message.from_json(data)
        self.send(message)

    def __enter__(self) -> "Producer":
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.disconnect()
