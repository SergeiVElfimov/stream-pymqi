"""Configuration module for stream-pymqi."""

from dataclasses import dataclass
from typing import Any

import yaml


@dataclass
class StreamConfig:
    """Configuration for IBM MQ stream connection."""

    host: str
    port: int
    channel: str
    queue_manager: str
    queue_name: str
    ssl: bool = False
    ssl_cipher_spec: str | None = None
    user: str | None = None
    password: str | None = None
    auto_reconnect: bool = True
    reconnect_interval: int = 5
    max_reconnect_attempts: int = 3

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "StreamConfig":
        """Create config from dictionary."""
        return cls(**data)

    @classmethod
    def from_yaml(cls, path: str) -> "StreamConfig":
        """Create config from YAML file."""
        with open(path) as f:
            data = yaml.safe_load(f)
        return cls.from_dict(data)

    def to_dict(self) -> dict[str, Any]:
        """Convert config to dictionary."""
        return {
            "host": self.host,
            "port": self.port,
            "channel": self.channel,
            "queue_manager": self.queue_manager,
            "queue_name": self.queue_name,
            "ssl": self.ssl,
            "ssl_cipher_spec": self.ssl_cipher_spec,
            "user": self.user,
            "password": self.password,
            "auto_reconnect": self.auto_reconnect,
            "reconnect_interval": self.reconnect_interval,
            "max_reconnect_attempts": self.max_reconnect_attempts,
        }
