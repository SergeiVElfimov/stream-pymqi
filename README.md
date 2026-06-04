# stream-pymqi

[![build-status-image]][build-status]
[![codeql-image]][codeql]
[![pypi-version]][pypi]
[![pypi-downloads]][pypi]

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

### Prerequisites

This library requires IBM MQ client libraries to be installed on your system.

**Ubuntu/Debian:**
```bash
# Download IBM MQ client from https://www.ibm.com/support/pages/ibm-mq-client-downloads
# Extract and install
# Note: IBM MQ client is not available in standard Ubuntu repositories
# You need to download and install it manually from IBM website
export LD_LIBRARY_PATH=/opt/mqm/lib64:$LD_LIBRARY_PATH
```

**Windows:**
1. Download IBM MQ client from https://www.ibm.com/support/pages/ibm-mq-client-downloads
2. Run the installer
3. Add MQ client bin directory to PATH

**Docker:**
```dockerfile
FROM python:3.11-slim

# Install IBM MQ client
RUN sudo mkdir -p /opt/mqm /var/mqm /IBM /.mqm && \
    sudo chmod 777 -R /opt/mqm /var/mqm /IBM /.mqm && \
    curl -L "https://public.dhe.ibm.com/ibmdl/export/pub/software/websphere/messaging/mqdev/redist/9.4.3.0-IBM-MQC-Redist-LinuxX64.tar.gz" -o mqclient.tar.gz && \
    sudo tar -zxf mqclient.tar.gz -C /opt/mqm && \
    sudo chmod 777 -R /opt/mqm

ENV LD_LIBRARY_PATH=/opt/mqm/lib64:$LD_LIBRARY_PATH

# Install your application
COPY . /app
WORKDIR /app
RUN pip install .
```

## Usage

### WorkerPool (Recommended for multiple queues)

The `WorkerPool` allows you to process messages from multiple queues concurrently using separate threads.

```python
from stream_pymqi import WorkerPool, StreamConfig

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1'
)

pool = WorkerPool(config=config)

@pool.worker(queue_name='QUEUE1')
def handle_queue1(message):
    print(f"[QUEUE1] Received: {message}")

@pool.worker(queue_name='QUEUE2')
def handle_queue2(message):
    print(f"[QUEUE2] Received: {message}")

pool.run()  # Start all workers
```

### Worker (Single queue)

For processing messages from a single queue:

```python
from stream_pymqi import Worker, StreamConfig

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1'
)

worker = Worker(config=config, queue_name='QUEUE1')

@worker.on_message()
def handle_message(message):
    print(f"Received: {message}")
    return True

worker.run()
```

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
    ssl_cipher_spec='TLS_RSA_WITH_AES_128_CBC_SHA256'
)

consumer = Consumer(config)
consumer.start()
```

### Using context managers

```python
from stream_pymqi import WorkerPool, StreamConfig

config = StreamConfig(
    host='localhost',
    port=1414,
    channel='CHANNEL1',
    queue_manager='QM1',
    queue_name='QUEUE1'
)

with WorkerPool(config=config) as pool:
    @pool.worker(queue_name='QUEUE1')
    def handle_queue1(message):
        print(f"[QUEUE1] Received: {message}")

    @pool.worker(queue_name='QUEUE2')
    def handle_queue2(message):
        print(f"[QUEUE2] Received: {message}")

    # Workers run automatically in context manager
    pass  # Workers will be stopped automatically
```

## Examples

See the `examples/` directory for more examples:
- `simple_broker.py` - Simple broker with one queue
- `multi_queue_broker.py` - Broker with multiple queues using WorkerPool

## API Reference

### StreamConfig

Configuration class for IBM MQ connection.

**Parameters:**
- `host` (str): Hostname of the IBM MQ server
- `port` (int): Port of the IBM MQ server
- `channel` (str): Channel name
- `queue_manager` (str): Queue manager name
- `queue_name` (str): Default queue name
- `ssl` (bool): Enable SSL
- `ssl_cipher_spec` (str): SSL cipher specification
- `user` (str): User name for authentication
- `password` (str): Password for authentication
- `reconnect_interval` (int): Reconnect interval in seconds

### WorkerPool

Pool of workers to process messages from multiple queues.

**Methods:**
- `worker(queue_name: str)` - Decorator to register a worker for a specific queue
- `add_worker(queue_name: str, handler: Callable)` - Add a worker with handler directly
- `start()` - Start all workers in separate threads
- `stop()` - Stop all workers
- `run()` - Start all workers and block until stopped

### Worker

Single queue worker for message processing.

**Methods:**
- `on_message()` - Decorator to register message handler
- `run()` - Start the worker and process messages
- `stop()` - Stop the worker

### Producer

Message producer for sending messages to queues.

**Methods:**
- `connect()` - Connect to IBM MQ
- `disconnect()` - Disconnect from IBM MQ
- `send(message: Message)` - Send a Message object
- `send_string(data: str)` - Send a string directly
- `send_json(data: Any)` - Send JSON data

### Consumer

Message consumer for receiving messages from queues.

**Methods:**
- `on_message()` - Decorator to register message handler
- `start()` - Start consuming messages
- `stop()` - Stop consuming messages

## License

Apache-2.0

[build-status-image]: https://github.com/SergeiVElfimov/stream-pymqi/actions/workflows/python-package.yml/badge.svg
[build-status]: https://github.com/SergeiVElfimov/stream-pymqi/actions/workflows/python-package.yml
[codeql-image]: https://github.com/SergeiVElfimov/stream-pymqi/actions/workflows/codeql.yml/badge.svg
[codeql]: https://github.com/SergeiVElfimov/stream-pymqi/actions/workflows/codeql.yml
[pypi-version]: https://img.shields.io/pypi/v/stream-pymqi.svg
[pypi-downloads]: https://img.shields.io/pypi/dm/stream-pymqi?color=%232E73B2&logo=python&logoColor=%23F9D25F
[pypi]: https://pypi.org/project/stream-pymqi/
