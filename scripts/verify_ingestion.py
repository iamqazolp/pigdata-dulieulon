import argparse
import asyncio
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from aiokafka import AIOKafkaConsumer

from ingestion.producer import TOPIC, partition_key
from ingestion.schema import Event


async def consume(bootstrap: str, n: int, timeout: float, to_end: bool) -> tuple[Counter, Counter, list[str]]:
    consumer = AIOKafkaConsumer(TOPIC, bootstrap_servers=bootstrap, auto_offset_reset="earliest", enable_auto_commit=False)
    await consumer.start()
    counts: Counter = Counter()
    patterns: Counter = Counter()
    errors: list[str] = []
    try:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        if to_end:
            while not consumer.assignment():
                await asyncio.sleep(0.1)
            ends = await consumer.end_offsets(list(consumer.assignment()))

        async def done() -> bool:
            if to_end:
                positions = {tp: await consumer.position(tp) for tp in ends}
                return all(positions[tp] >= end for tp, end in ends.items())
            return sum(counts.values()) + len(errors) >= n

        while not await done() and loop.time() < deadline:
            batch = await consumer.getmany(timeout_ms=1000, max_records=5000 if to_end else n - sum(counts.values()) - len(errors))
            for records in batch.values():
                for record in records:
                    try:
                        event = Event.from_json(record.value)
                        expected_key = partition_key(event)
                        if record.key != expected_key:
                            raise ValueError(f"key {record.key!r} != expected {expected_key!r}")
                        counts[event.source] += 1
                        if event.is_injected:
                            patterns[event.fraud_pattern or "unknown"] += 1
                    except Exception as exc:
                        errors.append(f"partition {record.partition} offset {record.offset}: {exc!r}")
    finally:
        await consumer.stop()
    return counts, patterns, errors


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("-n", type=int, default=500)
    p.add_argument("--require", nargs="*", default=["binance", "blockchain"])
    p.add_argument("--to-end", action="store_true", help="read the whole topic as of startup instead of N messages")
    p.add_argument("--timeout", type=float, default=60)
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    args = p.parse_args()

    counts, patterns, errors = asyncio.run(consume(args.bootstrap, args.n, args.timeout, args.to_end))
    total = sum(counts.values()) + len(errors)
    print(f"consumed {total} messages from {TOPIC}")
    for source, count in sorted(counts.items()):
        print(f"  {source}: {count}")
    if patterns:
        print(f"injected: {sum(patterns.values())}")
        for pattern, count in sorted(patterns.items()):
            print(f"  fraud_pattern={pattern}: {count}")
    for err in errors[:10]:
        print(f"INVALID {err}")
    missing = [s for s in args.require if counts[s] == 0]
    if missing:
        print(f"MISSING sources: {missing}")
    if not args.to_end and total < args.n:
        print(f"SHORT: wanted {args.n}, got {total}")
    ok = not errors and not missing and (args.to_end or total >= args.n)
    print("OK" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
