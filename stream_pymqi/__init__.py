"""stream-pymqi - High-level IBM MQ broker library for Python.

This library provides a FastStream-like synchronous interface for IBM MQ
based on pymqi.

Example usage:

    from stream_pymqi import IbmMqBroker, StreamConfig

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

from stream_pymqi.broker import IbmMqBroker
from stream_pymqi.config import StreamConfig
from stream_pymqi.connection import Connection
from stream_pymqi.consumer import Consumer
from stream_pymqi.message import Message
from stream_pymqi.producer import Producer
from stream_pymqi.worker import Worker, WorkerPool

__all__ = [
    "StreamConfig",
    "Message",
    "IbmMqBroker",
    "Connection",
    "Consumer",
    "Producer",
    "Worker",
    "WorkerPool",
]
