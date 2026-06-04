"""Consumer module for stream-pymqi - Synchronous FastStream-like interface."""

from collections.abc import Callable
from dataclasses import dataclass

import pymqi

from stream_pymqi.config import StreamConfig
from stream_pymqi.connection import Connection
from stream_pymqi.message import Message


@dataclass
class Consumer:
    """IBM MQ Consumer with FastStream-like synchronous interface."""

    config: StreamConfig
    _connection: Connection | None = None
    _callback: Callable | None = None
    _running: bool = False
    _queue: pymqi.Queue | None = None

    def on_message(self) -> Callable:
        """Register message handler.

        Example:
            consumer = Consumer(config)

            @consumer.on_message()
            def handle_message(message):
                print(f"Received: {message}")
                return True  # Acknowledge message

            consumer.start()
        """

        def decorator(func: Callable) -> Callable:
            self._callback = func
            return func

        return decorator

    def start(self) -> None:
        """Start consuming messages synchronously.

        This method blocks and continuously consumes messages until
        an exception occurs or stop() is called.
        """
        if self._callback is None:
            raise ValueError("Callback is not set. Use @consumer.on_message() decorator.")

        self._connection = Connection(self.config)
        self._connection.connect()

        try:
            while True:
                # Get message from queue
                gmo = pymqi.GMO()
                gmo.WaitInterval = pymqi.CMQC.MQWI_UNLIMITED
                gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING

                message = self._connection.queue_manager.get(self.config.queue_name, pymqi.CMQC.MQOD_NONE, gmo)

                # Create Message object
                msg = Message.from_bytes(message)

                # Call handler
                result = self._callback(msg)

                # If handler returns True, acknowledge message
                if result is not False:
                    # Message is automatically acknowledged by pymqi in default mode
                    pass

        except KeyboardInterrupt:
            self.stop()
        except pymqi.MQMIError as e:
            if e.mqrc == pymqi.CMQC.MQRC_Q_MGR_QUIESCING:
                self.stop()
            raise

    def stop(self) -> None:
        """Stop consuming messages."""
        self._running = False
        if self._connection:
            self._connection.disconnect()
            self._connection = None

    def __enter__(self) -> "Consumer":
        """Context manager entry."""
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()
