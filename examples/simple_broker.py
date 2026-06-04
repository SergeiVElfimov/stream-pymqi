"""Example: IBM MQ Broker with environment variables configuration.

Environment variables:
    IBM_MQ_HOST - Hostname of the IBM MQ server (default: localhost)
    IBM_MQ_PORT - Port of the IBM MQ server (default: 1414)
    IBM_MQ_CHANNEL - Channel name (default: CHANNEL1)
    IBM_MQ_QUEUE_MANAGER - Queue manager name (default: QM1)
    IBM_MQ_QUEUE_NAME - Queue name to consume from (default: QUEUE1)
    IBM_MQ_SSL - Enable SSL (default: false)
    IBM_MQ_SSL_CIPHER_SPEC - SSL cipher specification (optional)
    IBM_MQ_USER - User name for authentication (optional)
    IBM_MQ_PASSWORD - Password for authentication (optional)

To use .env file, install python-dotenv:
    pip install python-dotenv
"""

import os

from dotenv import load_dotenv

from stream_pymqi import IbmMqBroker, StreamConfig

# Load environment variables from .env file
load_dotenv()


def get_config_from_env() -> StreamConfig:
    """Get configuration from environment variables."""
    return StreamConfig(
        host=os.getenv("IBM_MQ_HOST", "localhost"),
        port=int(os.getenv("IBM_MQ_PORT", "1414")),
        channel=os.getenv("IBM_MQ_CHANNEL", "CHANNEL1"),
        queue_manager=os.getenv("IBM_MQ_QUEUE_MANAGER", "QM1"),
        queue_name=os.getenv("IBM_MQ_QUEUE_NAME", "QUEUE1"),
        ssl=os.getenv("IBM_MQ_SSL", "false").lower() == "true",
        ssl_cipher_spec=os.getenv("IBM_MQ_SSL_CIPHER_SPEC"),
        user=os.getenv("IBM_MQ_USER"),
        password=os.getenv("IBM_MQ_PASSWORD"),
    )


def main():
    """Entry point for the application."""
    config = get_config_from_env()

    print(f"Connecting to {config.queue_manager} on {config.host}:{config.port}")
    print(f"Channel: {config.channel}")
    print(f"Queue: {config.queue_name}")

    broker = IbmMqBroker(config=config)

    @broker.queue(config.queue_name)
    def handle_message(message):
        print(f"Received: {message}")

    broker.run()


if __name__ == "__main__":
    main()
