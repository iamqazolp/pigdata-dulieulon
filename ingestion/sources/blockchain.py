from __future__ import annotations

from typing import AsyncIterator, List

from ingestion.schema import Event
from ingestion.sources.ws import messages

URL = "wss://ws.blockchain.info/inv"
SATOSHI = 1e8


def parse(raw: dict) -> List[Event]:
    if raw.get("op") != "utx":
        return []
    tx = raw["x"]
    sender = next((i["prev_out"]["addr"] for i in tx["inputs"] if (i.get("prev_out") or {}).get("addr")), None)
    if sender is None:
        return []
    return [
        Event(
            event_id=f"blockchain:{tx['hash']}:{out['n']}",
            timestamp=int(tx["time"]) * 1000,
            source="blockchain",
            sender=sender,
            receiver=out["addr"],
            amount=out["value"] / SATOSHI,
            price=None,
            type="transfer",
        )
        for out in tx["out"]
        if out.get("addr")
    ]


async def stream() -> AsyncIterator[List[Event]]:
    async for raw in messages([URL], subscribe={"op": "unconfirmed_sub"}):
        yield parse(raw)
