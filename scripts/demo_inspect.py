from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from ingestion.producer import TOPIC, partition_key
from ingestion.schema import Event
from ingestion.sources import binance, blockchain, injector


async def live_direct(seconds: float) -> None:
    src_counts: Counter = Counter()
    pat_counts: Counter = Counter()
    part_counts: Counter = Counter()
    samples: dict[str, Event] = {}

    async def pump(aiter):
        async for batch in aiter:
            for e in batch:
                src_counts[e.source] += 1
                part_counts[hash(partition_key(e)) % 3] += 1
                key = f"injector:{e.fraud_pattern}" if e.is_injected else e.source
                if e.is_injected and e.fraud_pattern:
                    pat_counts[e.fraud_pattern] += 1
                samples.setdefault(key, e)

    tasks = [
        asyncio.create_task(pump(binance.stream())),
        asyncio.create_task(pump(blockchain.stream())),
        asyncio.create_task(pump(injector.stream(rate=6.0, seed=7))),
    ]
    await asyncio.sleep(seconds)
    for t in tasks:
        t.cancel()

    total = sum(src_counts.values())
    print(f"=== Live Stream Window ({seconds:.0f}s) | Total: {total} events ({total / seconds:.1f} ev/s) ===")
    print("By source:       ", dict(src_counts))
    print("Fraud patterns:  ", dict(pat_counts))
    print("Partition split: ", {f"P{p}": f"{c * 100 / max(total, 1):.1f}%" for p, c in sorted(part_counts.items())})
    for k, e in sorted(samples.items()):
        print(f"\n[{k}]\n{e.to_json().decode()}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--seconds", type=float, default=8.0)
    args = p.parse_args()
    asyncio.run(live_direct(args.seconds))


if __name__ == "__main__":
    main()
