# IBM MQ Broker Examples

This directory contains examples of using the `stream-pymqi` library with environment variables configuration.

## Examples

### 1. Simple Broker (`simple_broker.py`)

A basic example of an IBM MQ broker that consumes messages from a single queue.

**Usage:**
```bash
export IBM_MQ_HOST=localhost
export IBM_MQ_PORT=1414
export IBM_MQ_CHANNEL=CHANNEL1
export IBM_MQ_QUEUE_MANAGER=QM1
export IBM_MQ_QUEUE_NAME=QUEUE1

python simple_broker.py
```

### 2. Multi-Queue Broker (`multi_queue_broker.py`)

An example of an IBM MQ broker that consumes messages from multiple queues.

**Usage:**
```bash
export IBM_MQ_HOST=localhost
export IBM_MQ_PORT=1414
export IBM_MQ_CHANNEL=CHANNEL1
export IBM_MQ_QUEUE_MANAGER=QM1
export IBM_MQ_QUEUE_1=QUEUE1
export IBM_MQ_QUEUE_2=QUEUE2

python multi_queue_broker.py
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `IBM_MQ_HOST` | `localhost` | Hostname of the IBM MQ server |
| `IBM_MQ_PORT` | `1414` | Port of the IBM MQ server |
| `IBM_MQ_CHANNEL` | `CHANNEL1` | Channel name |
| `IBM_MQ_QUEUE_MANAGER` | `QM1` | Queue manager name |
| `IBM_MQ_QUEUE_NAME` | `QUEUE1` | Queue name to consume from |
| `IBM_MQ_QUEUE_1` | `QUEUE1` | First queue name (multi-queue example) |
| `IBM_MQ_QUEUE_2` | `QUEUE2` | Second queue name (multi-queue example) |
| `IBM_MQ_SSL` | `false` | Enable SSL |
| `IBM_MQ_SSL_CIPHER_SPEC` | (optional) | SSL cipher specification |
| `IBM_MQ_USER` | (optional) | User name for authentication |
| `IBM_MQ_PASSWORD` | (optional) | Password for authentication |

## API

### IbmMqBroker

```python
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
def handle_message(message):
    print(f"Received: {message}")

broker.run()
```
