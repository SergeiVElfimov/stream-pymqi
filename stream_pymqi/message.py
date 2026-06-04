"""Message module for stream-pymqi."""

import json
from dataclasses import dataclass, field
from typing import Any


@dataclass
class Message:
    """IBM MQ message wrapper."""

    data: bytes
    message_id: bytes | None = None
    correlation_id: bytes | None = None
    priority: int = 0
    persistence: bool = True
    timestamp: float | None = None
    user_id: bytes | None = None
    reply_to: str | None = None
    headers: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_bytes(cls, data: bytes) -> "Message":
        """Create message from bytes."""
        return cls(data=data)

    @classmethod
    def from_string(cls, data: str, encoding: str = "utf-8") -> "Message":
        """Create message from string."""
        return cls(data=data.encode(encoding))

    @classmethod
    def from_json(cls, data: Any) -> "Message":
        """Create message from JSON-serializable data."""
        return cls(data=json.dumps(data).encode("utf-8"))

    def to_bytes(self) -> bytes:
        """Get message data as bytes."""
        return self.data

    def to_string(self, encoding: str = "utf-8") -> str:
        """Get message data as string."""
        return self.data.decode(encoding)

    def to_json(self) -> Any:
        """Parse message data as JSON."""
        return json.loads(self.data.decode("utf-8"))

    def __str__(self) -> str:
        """Return string representation."""
        try:
            return self.to_string()
        except UnicodeDecodeError:
            return f"<binary data: {len(self.data)} bytes>"
