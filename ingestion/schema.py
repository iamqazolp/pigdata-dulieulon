from __future__ import annotations

import json
from dataclasses import asdict, dataclass, fields
from typing import Optional

SOURCES = ("binance", "blockchain", "paysim", "injector")


@dataclass(frozen=True)
class Event:
    event_id: str
    timestamp: int
    source: str
    sender: str
    receiver: str
    amount: float
    price: Optional[float]
    type: str
    is_injected: bool = False
    fraud_pattern: Optional[str] = None

    def __post_init__(self) -> None:
        if self.source not in SOURCES:
            raise ValueError(f"unknown source: {self.source}")
        if not isinstance(self.timestamp, int) or isinstance(self.timestamp, bool):
            raise ValueError("timestamp must be int milliseconds")
        if not self.event_id or not self.sender or not self.receiver:
            raise ValueError("event_id, sender and receiver are required")

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def from_json(cls, data: bytes | str) -> Event:
        obj = json.loads(data)
        expected = {f.name for f in fields(cls)}
        if set(obj) != expected:
            raise ValueError(f"field mismatch: {sorted(set(obj) ^ expected)}")
        return cls(**obj)
