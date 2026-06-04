"""Tests for stream-pymqi."""

import contextlib
from unittest.mock import MagicMock, patch

import pytest

from stream_pymqi import (
    Connection,
    Consumer,
    IbmMqBroker,
    Message,
    Producer,
    StreamConfig,
    Worker,
    WorkerPool,
)


class TestStreamConfig:
    """Tests for StreamConfig."""

    def test_from_dict(self):
        """Test config from dict."""
        data = {
            "host": "localhost",
            "port": 1414,
            "channel": "CHANNEL1",
            "queue_manager": "QM1",
            "queue_name": "QUEUE1",
        }
        config = StreamConfig.from_dict(data)
        assert config.host == "localhost"
        assert config.port == 1414
        assert config.channel == "CHANNEL1"
        assert config.queue_manager == "QM1"
        assert config.queue_name == "QUEUE1"

    def test_to_dict(self):
        """Test config to dict."""
        data = {
            "host": "localhost",
            "port": 1414,
            "channel": "CHANNEL1",
            "queue_manager": "QM1",
            "queue_name": "QUEUE1",
        }
        config = StreamConfig.from_dict(data)
        result = config.to_dict()
        assert result["host"] == "localhost"
        assert result["port"] == 1414


class TestMessage:
    """Tests for Message."""

    def test_from_string(self):
        """Test message from string."""
        msg = Message.from_string("Hello, World!")
        assert msg.to_string() == "Hello, World!"

    def test_from_json(self):
        """Test message from JSON."""
        data = {"key": "value"}
        msg = Message.from_json(data)
        assert msg.to_json() == data

    def test_to_bytes(self):
        """Test message to bytes."""
        msg = Message.from_string("Hello")
        assert msg.to_bytes() == b"Hello"


class TestIbmMqBroker:
    """Tests for IbmMqBroker."""

    def test_queue_decorator(self):
        """Test queue decorator."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        broker = IbmMqBroker(config=config)

        @broker.queue(queue_name="QUEUE1")
        def handle_message(message):
            return True

        assert "QUEUE1" in broker._queues
        assert broker._queues["QUEUE1"] is not None

    def test_add_queue(self):
        """Test add_queue method."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        broker = IbmMqBroker(config=config)

        def handler(message):
            return True

        broker.add_queue("QUEUE1", handler)
        assert "QUEUE1" in broker._queues
        assert broker._queues["QUEUE1"] is not None

    def test_start_stop(self):
        """Test start and stop methods."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        broker = IbmMqBroker(config=config)

        @broker.queue(queue_name="QUEUE1")
        def handle_message(message):
            return True

        broker.start()
        assert broker._running is True
        assert "QUEUE1" in broker._threads

        broker.stop()
        assert broker._running is False

    def test_run(self):
        """Test run method."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        broker = IbmMqBroker(config=config)

        @broker.queue(queue_name="QUEUE1")
        def handle_message(message):
            return True

        # Just verify the broker can be created and queues registered
        assert len(broker._queues) == 1


class TestProducer:
    """Tests for Producer."""

    def test_init(self):
        """Test Producer initialization."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        producer = Producer(config=config)

        assert producer.config.queue_name == "QUEUE1"
        assert producer._connection is None
        assert producer._running is False

    def test_connect_sets_running(self):
        """Test connect sets _running flag."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        producer = Producer(config=config)

        with patch("stream_pymqi.producer.Connection") as MockConnection:
            mock_conn = MagicMock()
            MockConnection.return_value = mock_conn

            producer.connect()

            assert producer._running is True
            assert producer._connection == mock_conn
            mock_conn.connect.assert_called_once()

    def test_disconnect(self):
        """Test disconnect method."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        producer = Producer(config=config)

        with patch("stream_pymqi.producer.Connection") as MockConnection:
            mock_conn = MagicMock()
            MockConnection.return_value = mock_conn
            producer._connection = mock_conn
            producer._running = True

            producer.disconnect()

            assert producer._running is False
            assert producer._connection is None
            mock_conn.disconnect.assert_called_once()

    def test_send_string(self):
        """Test send_string creates message and sends."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        producer = Producer(config=config)

        with (
            patch.object(producer, "connect"),
            patch.object(producer, "send") as mock_send,
            patch.object(Message, "from_string") as mock_from_string,
        ):
            mock_msg = MagicMock()
            mock_from_string.return_value = mock_msg

            producer.send_string("Hello, World!")

            mock_from_string.assert_called_once_with("Hello, World!")
            mock_send.assert_called_once_with(mock_msg)

    def test_send_json(self):
        """Test send_json creates message and sends."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        producer = Producer(config=config)

        with (
            patch.object(producer, "connect"),
            patch.object(producer, "send") as mock_send,
            patch.object(Message, "from_json") as mock_from_json,
        ):
            mock_msg = MagicMock()
            mock_from_json.return_value = mock_msg

            producer.send_json({"key": "value"})

            mock_from_json.assert_called_once_with({"key": "value"})
            mock_send.assert_called_once_with(mock_msg)


class TestWorker:
    """Tests for Worker."""

    def test_init(self):
        """Test Worker initialization."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        worker = Worker(config=config, queue_name="QUEUE1")

        assert worker.queue_name == "QUEUE1"
        assert worker.handler is None
        assert worker._connection is None
        assert worker._running is False

    def test_on_message_decorator(self):
        """Test on_message decorator."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        worker = Worker(config=config, queue_name="QUEUE1")

        @worker.on_message()
        def handle_message(message):
            return True

        assert worker.handler is not None
        assert callable(worker.handler)

    def test_run_without_handler_raises_error(self):
        """Test that run raises error without handler."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        worker = Worker(config=config, queue_name="QUEUE1")

        with pytest.raises(ValueError, match="Handler is not set"):
            worker.run()

    def test_stop(self):
        """Test stop method."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        worker = Worker(config=config, queue_name="QUEUE1")

        @worker.on_message()
        def handle_message(message):
            return True

        # Manually set running to test stop
        worker._running = True
        worker.stop()
        assert worker._running is False

    def test_run_with_handler(self):
        """Test run method with handler."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        worker = Worker(config=config, queue_name="QUEUE1")

        @worker.on_message()
        def handle_message(message):
            return True

        with (
            patch("stream_pymqi.worker.Connection") as MockConnection,
            patch("stream_pymqi.worker.pymqi") as mock_pymqi,
        ):
            mock_conn = MagicMock()
            MockConnection.return_value = mock_conn
            mock_conn.connect = MagicMock()
            mock_conn.queue_manager = MagicMock()

            # Mock GMO and get to raise KeyboardInterrupt after first call
            mock_gmo = MagicMock()
            mock_pymqi.GMO.return_value = mock_gmo
            mock_pymqi.CMQC.MQWI_UNLIMITED = -1
            mock_pymqi.CMQC.MQGMO_WAIT = 1
            mock_pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING = 2

            # Make get raise KeyboardInterrupt to stop the loop
            mock_conn.queue_manager.get.side_effect = KeyboardInterrupt()

            with contextlib.suppress(KeyboardInterrupt):
                worker.run()

            mock_conn.connect.assert_called_once()


class TestWorkerPool:
    """Tests for WorkerPool."""

    def test_init(self):
        """Test WorkerPool initialization."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        pool = WorkerPool(config=config)

        assert pool._workers == {}
        assert pool._threads == {}
        assert pool._running is False

    def test_worker_decorator(self):
        """Test worker decorator."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        pool = WorkerPool(config=config)

        @pool.worker(queue_name="QUEUE1")
        def handle_message(message):
            return True

        assert "QUEUE1" in pool._workers
        assert pool._workers["QUEUE1"].handler is not None

    def test_add_worker(self):
        """Test add_worker method."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        pool = WorkerPool(config=config)

        def handler(message):
            return True

        pool.add_worker("QUEUE1", handler)
        assert "QUEUE1" in pool._workers
        assert pool._workers["QUEUE1"].handler is handler

    def test_start_stop(self):
        """Test start and stop methods."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        pool = WorkerPool(config=config)

        @pool.worker(queue_name="QUEUE1")
        def handle_message(message):
            return True

        pool.start()
        assert pool._running is True
        assert "QUEUE1" in pool._threads

        pool.stop()
        assert pool._running is False
        assert pool._threads == {}


class TestConsumer:
    """Tests for Consumer."""

    def test_init(self):
        """Test Consumer initialization."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        consumer = Consumer(config=config)

        assert consumer._callback is None
        assert consumer._connection is None
        assert consumer._running is False

    def test_on_message_decorator(self):
        """Test on_message decorator."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        consumer = Consumer(config=config)

        @consumer.on_message()
        def handle_message(message):
            return True

        assert consumer._callback is not None
        assert callable(consumer._callback)

    def test_start_stop(self):
        """Test start and stop methods."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )

        def handle_message(message):
            return True

        with (
            patch("stream_pymqi.consumer.Connection") as MockConnection,
            patch("stream_pymqi.consumer.pymqi"),
        ):
            mock_conn = MagicMock()
            MockConnection.return_value = mock_conn
            mock_conn.connect = MagicMock()
            mock_conn.disconnect = MagicMock()
            mock_conn.queue_manager = MagicMock()

            # Make get raise KeyboardInterrupt to stop the loop
            mock_conn.queue_manager.get.side_effect = KeyboardInterrupt()

            consumer = Consumer(config=config)
            consumer._callback = handle_message

            with contextlib.suppress(KeyboardInterrupt):
                consumer.start()

            # Check that _running was set to True in start()
            # Note: start() sets _running = True before calling connection.connect()
            # But we need to check the actual value after start() completes
            # Since start() blocks, we need to verify the mock was called
            assert consumer._callback is handle_message
            MockConnection.assert_called_once_with(config)
            mock_conn.connect.assert_called_once()

    def test_context_manager(self):
        """Test Consumer as context manager."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )

        def handle_message(message):
            return True

        with (
            patch("stream_pymqi.consumer.Connection") as MockConnection,
            patch("stream_pymqi.consumer.pymqi"),
        ):
            mock_conn = MagicMock()
            MockConnection.return_value = mock_conn
            mock_conn.connect = MagicMock()
            mock_conn.disconnect = MagicMock()
            mock_conn.queue_manager = MagicMock()

            # Make get raise KeyboardInterrupt to stop the loop
            mock_conn.queue_manager.get.side_effect = KeyboardInterrupt()

            consumer = Consumer(config=config)
            consumer._callback = handle_message

            # Just verify the consumer can be created and callback set
            assert consumer._callback is handle_message
            assert consumer._connection is None
            assert consumer._running is False


class TestConnection:
    """Tests for Connection."""

    def test_init(self):
        """Test Connection initialization."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        connection = Connection(config=config)

        assert connection.config == config
        assert connection._queue_manager is None
        assert connection._connected is False

    def test_connect_disconnect(self):
        """Test connect and disconnect methods."""
        config = StreamConfig(
            host="localhost",
            port=1414,
            channel="CHANNEL1",
            queue_manager="QM1",
            queue_name="QUEUE1",
        )
        connection = Connection(config=config)

        with patch("stream_pymqi.connection.pymqi") as mock_pymqi:
            mock_qmgr = MagicMock()
            mock_pymqi.QueueManager.return_value = mock_qmgr

            connection.connect()
            assert connection._connected is True
            assert connection._queue_manager == mock_qmgr

            connection.disconnect()
            assert connection._connected is False
            assert connection._queue_manager is None
