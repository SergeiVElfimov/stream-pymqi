# stream-pymqi

High-level IBM MQ consumer and producer library for Python

## Features

- Simple consumer/producer API for IBM MQ
- Automatic connection management and reconnection
- Stream-style message processing (async/await ready)
- Configuration via Python dict or YAML files
- Thread-safe operations
- Error handling and retry mechanisms
- Compatible with Python 3.8+

## Installation

```bash
pip install stream-pymqi
```

## Usage

### Consumer

```python
from stream_pymqi import Consumer, StreamConfig

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1'
)

consumer = Consumer(config)

@consumer.on_message()
def handle_message(message):
    print(f"Received: {message}")

consumer.start()
```

### Producer

```python
from stream_pymqi import Producer, StreamConfig, Message

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1'
)

producer = Producer(config)

# Send a message
producer.send(Message(data=b"Hello, IBM MQ!"))

# Or send a string directly
producer.send_string("Hello, IBM MQ!")

producer.disconnect()
```

### Configuration via YAML

```python
from stream_pymqi import Consumer, StreamConfig

config = StreamConfig.from_yaml('config.yaml')
consumer = Consumer(config)
consumer.start()
```

Example `config.yaml`:
```yaml
host: localhost
port: 1414
channel: CHANNEL1
queue_manager: QM1
queue_name: QUEUE1
ssl: false
auto_reconnect: true
```

### With SSL

```python
from stream_pymqi import Consumer, StreamConfig

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1',
    ssl=True,
    ssl_cipher_spec='TLS_RSA_WITH_AES_128_CBC_SHA'
)

consumer = Consumer(config)
consumer.start()
```

### Error Handling

```python
from stream_pymqi import Consumer, StreamConfig

def on_error(exception):
    print(f"Error: {exception}")

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1',
    on_error=on_error
)

consumer = Consumer(config)
consumer.start()
```

## API Reference

### StreamConfig

Configuration class for IBM MQ connection.

**Parameters:**
- `host`: IBM MQ host address
- `port`: IBM MQ port (default: 1414)
- `channel`: IBM MQ channel name
- `queue_manager`: IBM MQ queue manager name
- `queue_name`: IBM MQ queue name
- `user`: IBM MQ user (optional)
- `password`: IBM MQ password (optional)
- `connect_retries`: Number of connection retries (default: 3)
- `connect_retry_interval`: Interval between retries in seconds (default: 5)
- `message_wait_interval`: Wait interval for message consumption in seconds (default: 5)
- `auto_reconnect`: Enable automatic reconnection (default: True)
- `max_message_size`: Maximum message size in bytes (default: 1048576)
- `ssl`: Enable SSL connection (default: False)
- `ssl_cipher_spec`: SSL cipher specification (optional)
- `cert_store_location`: Certificate store location (optional)

### Consumer

IBM MQ consumer with automatic connection management.

**Methods:**
- `on_message()`: Decorator to register a message handler
- `register_handler(handler)`: Register a message handler
- `start()`: Start consuming messages in a background thread
- `stop()`: Stop consuming messages
- `is_running()`: Check if consumer is running

### Producer

IBM MQ producer with automatic connection management.

**Methods:**
- `connect()`: Establish connection to IBM MQ
- `disconnect()`: Disconnect from IBM MQ
- `send(message)`: Send a message to the queue
- `send_string(text)`: Send a string message
- `send_bytes(data)`: Send a bytes message
- `is_connected()`: Check if connected to IBM MQ

## License

Apache-2.0
