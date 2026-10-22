from __future__ import annotations

from typing import AsyncIterator, List, Sequence

from ingestion.schema import Event
from ingestion.sources.ws import messages

DEFAULT_SYMBOLS = ("btcusdt", "ethusdt", "bnbusdt", "solusdt", "xrpusdt")
HOSTS = ("wss://stream.binance.com:9443", "wss://data-stream.binance.vision")


def parse(raw: dict) -> List[Event]:
    data = raw.get("data", raw)
    if data.get("e") != "trade":
        return []
    symbol = data["s"]
    price = float(data["p"])
    return [
        Event(
            event_id=f"binance:{symbol}:{data['t']}",
            timestamp=int(data["T"]),
            source="binance",
            sender=symbol,
            receiver=symbol,
            amount=price * float(data["q"]),
            price=price,
            type="trade_sell" if data["m"] else "trade_buy",
        )
    ]


def urls(symbols: Sequence[str]) -> List[str]:
    streams = "/".join(f"{s.lower()}@trade" for s in symbols)
    return [f"{host}/stream?streams={streams}" for host in HOSTS]


async def stream(symbols: Sequence[str] = DEFAULT_SYMBOLS) -> AsyncIterator[List[Event]]:
    async for raw in messages(urls(symbols)):
        yield parse(raw)
