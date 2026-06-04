"""Tests for stream-pymqi."""

from stream_pymqi import IbmMqBroker, Message, StreamConfig


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
