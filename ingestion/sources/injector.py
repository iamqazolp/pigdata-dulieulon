from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass
from typing import AsyncIterator, Callable, Optional

from ingestion.schema import Event
from ingestion.sources.binance import DEFAULT_SYMBOLS

BECH32_CHARS = "023456789acdefghjklmnpqrstuvwxyz"

REFERENCE_PRICES: dict[str, float] = {
    "BTCUSDT": 68500.0,
    "ETHUSDT": 2500.0,
    "BNBUSDT": 590.0,
    "SOLUSDT": 175.0,
    "XRPUSDT": 0.52,
    "DOGEUSDT": 0.14,
    "ADAUSDT": 0.35,
    "TRXUSDT": 0.16,
    "AVAXUSDT": 26.0,
    "LINKUSDT": 11.5,
    "LTCUSDT": 71.0,
    "SUIUSDT": 1.95,
    "NEARUSDT": 4.2,
    "UNIUSDT": 7.8,
    "AAVEUSDT": 150.0,
    "ZECUSDT": 38.0,
    "PEPEUSDT": 0.0000095,
    "WLDUSDT": 2.1,
    "ENAUSDT": 0.38,
    "TAOUSDT": 520.0,
}

PatternFn = Callable[[random.Random, int], list[Event]]


@dataclass(frozen=True)
class PatternSpec:
    fn: PatternFn
    event_share: float
    avg_events: float

    @property
    def incident_weight(self) -> float:
        return self.event_share / self.avg_events


def _incident_id(rng: random.Random) -> str:
    return f"{rng.getrandbits(48):012x}"


def _wallet(rng: random.Random) -> str:
    return "bc1q" + "".join(rng.choices(BECH32_CHARS, k=38))


def _distinct_wallets(rng: random.Random, count: int, exclude: Optional[set[str]] = None) -> list[str]:
    seen = set(exclude or ())
    wallets: list[str] = []
    while len(wallets) < count:
        w = _wallet(rng)
        if w not in seen:
            seen.add(w)
            wallets.append(w)
    return wallets


def large_amount(rng: random.Random, now_ms: int) -> list[Event]:
    inc_id = _incident_id(rng)
    if rng.random() < 0.5:
        sender, receiver = _distinct_wallets(rng, 2)
        return [
            Event(
                event_id=f"injector:large_amount:{inc_id}:0",
                timestamp=now_ms,
                source="injector",
                sender=sender,
                receiver=receiver,
                amount=round(rng.uniform(25.0, 150.0), 8),
                price=None,
                type="transfer",
                is_injected=True,
                fraud_pattern="large_amount",
            )
        ]
    symbol = rng.choice(DEFAULT_SYMBOLS).upper()
    base_price = REFERENCE_PRICES.get(symbol, 100.0)
    price = round(base_price * rng.uniform(0.99, 1.01), 8)
    trade_type = rng.choice(("trade_buy", "trade_sell"))
    return [
        Event(
            event_id=f"injector:large_amount:{inc_id}:0",
            timestamp=now_ms,
            source="injector",
            sender=symbol,
            receiver=symbol,
            amount=round(rng.uniform(75_000.0, 250_000.0), 2),
            price=price,
            type=trade_type,
            is_injected=True,
            fraud_pattern="large_amount",
        )
    ]


def burst(rng: random.Random, now_ms: int) -> list[Event]:
    inc_id = _incident_id(rng)
    count = rng.randint(15, 25)
    sender = _wallet(rng)
    receivers = _distinct_wallets(rng, count, exclude={sender})
    ts = now_ms
    events: list[Event] = []
    for i, receiver in enumerate(receivers):
        if i > 0:
            ts += rng.randint(50, 300)
        events.append(
            Event(
                event_id=f"injector:burst:{inc_id}:{i}",
                timestamp=ts,
                source="injector",
                sender=sender,
                receiver=receiver,
                amount=round(rng.uniform(0.05, 0.8), 8),
                price=None,
                type="transfer",
                is_injected=True,
                fraud_pattern="burst",
            )
        )
    return events


def smurfing(rng: random.Random, now_ms: int) -> list[Event]:
    inc_id = _incident_id(rng)
    count = rng.randint(8, 12)
    sender = _wallet(rng)
    receivers = _distinct_wallets(rng, count, exclude={sender})
    return [
        Event(
            event_id=f"injector:smurfing:{inc_id}:{i}",
            timestamp=now_ms,
            source="injector",
            sender=sender,
            receiver=receiver,
            amount=round(rng.uniform(0.08, 0.095), 8),
            price=None,
            type="transfer",
            is_injected=True,
            fraud_pattern="smurfing",
        )
        for i, receiver in enumerate(receivers)
    ]


def rapid_relay(rng: random.Random, now_ms: int) -> list[Event]:
    inc_id = _incident_id(rng)
    hops = rng.randint(4, 6)
    wallets = _distinct_wallets(rng, hops + 1)
    amount = rng.uniform(5.0, 25.0)
    ts = now_ms
    events: list[Event] = []
    for i in range(hops):
        if i > 0:
            ts += rng.randint(200, 800)
            amount = amount * rng.uniform(0.985, 0.995)
        events.append(
            Event(
                event_id=f"injector:rapid_relay:{inc_id}:{i}",
                timestamp=ts,
                source="injector",
                sender=wallets[i],
                receiver=wallets[i + 1],
                amount=amount,
                price=None,
                type="transfer",
                is_injected=True,
                fraud_pattern="rapid_relay",
            )
        )
    return events


PATTERNS: dict[str, PatternSpec] = {
    "large_amount": PatternSpec(large_amount, event_share=0.25, avg_events=1.0),
    "burst": PatternSpec(burst, event_share=0.35, avg_events=20.0),
    "smurfing": PatternSpec(smurfing, event_share=0.25, avg_events=10.0),
    "rapid_relay": PatternSpec(rapid_relay, event_share=0.15, avg_events=5.0),
}


def generate_incident(pattern: str, rng: random.Random, now_ms: int) -> list[Event]:
    spec = PATTERNS.get(pattern)
    if spec is None:
        raise ValueError(f"unknown fraud pattern: {pattern}")
    return spec.fn(rng, now_ms)


async def stream(rate: float = 4.0, seed: Optional[int] = None) -> AsyncIterator[list[Event]]:
    rng = random.Random(seed)
    names = list(PATTERNS.keys())
    raw_weights = {name: PATTERNS[name].incident_weight for name in names}
    total_weight = sum(raw_weights.values())
    weights = {name: raw_weights[name] / total_weight for name in names}
    credits = {name: rng.uniform(0.0, weights[name]) for name in names}
    while True:
        for name in names:
            credits[name] += weights[name]
        pattern = max(names, key=lambda name: credits[name])
        credits[pattern] -= 1.0
        events = generate_incident(pattern, rng, int(time.time() * 1000))
        yield events
        await asyncio.sleep(len(events) / rate)
