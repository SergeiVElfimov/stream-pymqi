"""Broker module for stream-pymqi - FastStream-like interface."""

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import pymqi

from stream_pymqi.config import StreamConfig
from stream_pymqi.connection import Connection
from stream_pymqi.message import Message


@dataclass
class IbmMqBroker:
    """IBM MQ Broker with FastStream-like synchronous interface.

    The Broker automatically handles connection and message processing
    for multiple queues. You just need to specify the queue_name and
    provide a handler function.

    Example:
        config = StreamConfig(
            host='localhost',
            port=1414,
            channel='CHANNEL1',
            queue_manager='QM1',
            queue_name='QUEUE1'
        )

        broker = IbmMqBroker(config=config)

        @broker.queue('QUEUE1')
        def handle_queue1(message):
            print(f"Queue1: {message}")

        @broker.queue('QUEUE2')
        def handle_queue2(message):
            print(f"Queue2: {message}")

        broker.run()  # Start all queues
    """

    config: StreamConfig
    _queues: dict[str, Callable[[Message], Any]] = field(default_factory=dict)
    _threads: dict[str, threading.Thread] = field(default_factory=dict)
    _running: bool = False

    def queue(self, queue_name: str):
        """Register a queue handler.

        Args:
            queue_name: Name of the queue to consume from.

        Example:
            @broker.queue(queue_name='QUEUE1')
            def handle_message(message):
                print(f"Received: {message}")
        """

        def decorator(func: Callable) -> Callable:
            self._queues[queue_name] = func
            return func

        return decorator

    def add_queue(self, queue_name: str, handler: Callable[[Message], Any]) -> None:
        """Add a queue handler directly.

        Args:
            queue_name: Name of the queue to consume from.
            handler: Function to process messages.
        """
        self._queues[queue_name] = handler

    def start(self) -> None:
        """Start all queues in separate threads."""
        if self._running:
            return

        self._running = True

        for queue_name, handler in self._queues.items():
            thread = threading.Thread(
                target=self._run_queue, args=(queue_name, handler), daemon=True, name=f"Broker-{queue_name}"
            )
            self._threads[queue_name] = thread
            thread.start()

    def _run_queue(self, queue_name: str, handler: Callable[[Message], Any]) -> None:
        """Run a single queue in a thread."""
        connection = Connection(self.config)
        connection.connect()

        try:
            while self._running:
                # Get message from queue
                gmo = pymqi.GMO()
                gmo.WaitInterval = pymqi.CMQC.MQWI_UNLIMITED
                gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING

                # Create queue object with open options
                queue = pymqi.Queue(connection.queue_manager, queue_name, pymqi.CMQC.MQOO_INPUT_AS_Q_DEF)

                try:
                    # Get message with md and gmo
                    message = queue.get(None, pymqi.md(), gmo)

                    # Create Message object
                    msg = Message.from_bytes(message)

                    # Call handler
                    result = handler(msg)

                    # If handler returns True, acknowledge message
                    if result is not False:
                        # Message is automatically acknowledged by pymqi in default mode
                        pass

                except pymqi.MQMIError as e:
                    if e.mqrc == pymqi.CMQC.MQRC_Q_MGR_QUIESCING:
                        break
                    if e.mqrc == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                        continue
                    raise
                finally:
                    queue.close()

        except KeyboardInterrupt:
            pass
        finally:
            connection.disconnect()

    def stop(self) -> None:
        """Stop all queues."""
        self._running = False

        # Wait for all threads to finish
        for _queue_name, thread in self._threads.items():
            thread.join(timeout=10)

        self._threads.clear()

    def run(self) -> None:
        """Start all queues and block until stopped.

        This method starts all queues in separate threads and blocks
        until KeyboardInterrupt is raised.
        """
        self.start()

        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def __enter__(self) -> "IbmMqBroker":
        """Context manager entry."""
        self.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()
