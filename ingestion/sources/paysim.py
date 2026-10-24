from __future__ import annotations

import asyncio
import csv
import time
from typing import AsyncIterator, Dict, List

from ingestion.schema import Event

HOUR_MS = 3_600_000


def parse(row: Dict[str, str], row_no: int, start_ms: int) -> List[Event]:
    fraud = row["isFraud"] == "1"
    return [
        Event(
            event_id=f"paysim:{row_no}",
            timestamp=start_ms + int(row["step"]) * HOUR_MS,
            source="paysim",
            sender=row["nameOrig"],
            receiver=row["nameDest"],
            amount=float(row["amount"]),
            price=None,
            type=row["type"],
            is_injected=fraud,
            fraud_pattern="paysim_fraud" if fraud else None,
        )
    ]


async def replay(path: str, rate: float) -> AsyncIterator[List[Event]]:
    start_ms = int(time.time() * 1000)
    interval = 1.0 / rate
    next_at = time.monotonic()
    with open(path, newline="") as f:
        for row_no, row in enumerate(csv.DictReader(f)):
            next_at += interval
            delay = next_at - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            yield parse(row, row_no, start_ms)
