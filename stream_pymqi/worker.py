"""Worker module for stream-pymqi - FastStream-like interface."""

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
class Worker:
    """IBM MQ Worker with FastStream-like synchronous interface.

    The Worker automatically handles connection and message processing.
    You just need to specify the queue_name and provide a handler function.

    Example:
        worker = Worker(
            config=config,
            queue_name='QUEUE1',
            handler=lambda msg: print(f"Received: {msg}")
        )
        worker.run()
    """

    config: StreamConfig
    queue_name: str
    handler: Callable[[Message], Any] | None = None
    _connection: Connection | None = None
    _running: bool = False

    def on_message(self) -> Callable:
        """Register message handler.

        Example:
            worker = Worker(config=config, queue_name='QUEUE1')

            @worker.on_message()
            def handle_message(message):
                print(f"Received: {message}")
                return True

            worker.run()
        """

        def decorator(func: Callable) -> Callable:
            self.handler = func
            return func

        return decorator

    def run(self) -> None:
        """Start the worker and process messages.

        This method blocks and continuously consumes messages until
        an exception occurs or KeyboardInterrupt is raised.
        """
        if self.handler is None:
            raise ValueError("Handler is not set. Use @worker.on_message() decorator.")

        self._connection = Connection(self.config)
        self._connection.connect()

        try:
            while True:
                # Get message from queue
                gmo = pymqi.GMO()
                gmo.WaitInterval = pymqi.CMQC.MQWI_UNLIMITED
                gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING

                message = self._connection.queue_manager.get(self.queue_name, pymqi.CMQC.MQOD_NONE, gmo)

                # Create Message object
                msg = Message.from_bytes(message)

                # Call handler
                result = self.handler(msg)

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
        """Stop the worker."""
        self._running = False
        if self._connection:
            self._connection.disconnect()
            self._connection = None

    def __enter__(self) -> "Worker":
        """Context manager entry."""
        self.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()


@dataclass
class WorkerPool:
    """Pool of workers to process messages from multiple queues.

    Example:
        config = StreamConfig(host='localhost', port=1414, ...)

        pool = WorkerPool(config=config)

        @pool.worker(queue_name='QUEUE1')
        def handle_queue1(message):
            print(f"Queue1: {message}")

        @pool.worker(queue_name='QUEUE2')
        def handle_queue2(message):
            print(f"Queue2: {message}")

        pool.run()  # Start all workers
    """

    config: StreamConfig
    _workers: dict[str, Worker] = field(default_factory=dict)
    _threads: dict[str, threading.Thread] = field(default_factory=dict)
    _running: bool = False

    def worker(self, queue_name: str):
        """Register a worker for a specific queue.

        Args:
            queue_name: Name of the queue to consume from.

        Example:
            @pool.worker(queue_name='QUEUE1')
            def handle_message(message):
                print(f"Received: {message}")
        """

        def decorator(func: Callable) -> Callable:
            worker = Worker(config=self.config, queue_name=queue_name, handler=func)
            self._workers[queue_name] = worker
            return func

        return decorator

    def add_worker(self, queue_name: str, handler: Callable[[Message], Any]) -> None:
        """Add a worker with handler directly.

        Args:
            queue_name: Name of the queue to consume from.
            handler: Function to process messages.
        """
        worker = Worker(config=self.config, queue_name=queue_name, handler=handler)
        self._workers[queue_name] = worker

    def start(self) -> None:
        """Start all workers in separate threads."""
        if self._running:
            return

        self._running = True

        for queue_name, worker in self._workers.items():
            thread = threading.Thread(target=self._run_worker, args=(worker,), daemon=True, name=f"Worker-{queue_name}")
            self._threads[queue_name] = thread
            thread.start()

    def _run_worker(self, worker: Worker) -> None:
        """Run a single worker in a thread."""
        worker.run()

    def stop(self) -> None:
        """Stop all workers."""
        self._running = False

        for _queue_name, worker in self._workers.items():
            worker.stop()

        # Wait for all threads to finish
        for _queue_name, thread in self._threads.items():
            thread.join(timeout=10)

        self._threads.clear()

    def run(self) -> None:
        """Start all workers and block until stopped.

        This method starts all workers in separate threads and blocks
        until KeyboardInterrupt is raised.
        """
        self.start()

        try:
            while self._running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop()

    def __enter__(self) -> "WorkerPool":
        """Context manager entry."""
        self.run()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop()
