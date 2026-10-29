from __future__ import annotations

import argparse
import asyncio
import logging
import os

from ingestion.producer import EventProducer
from ingestion.sources import binance, blockchain, injector, paysim


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="python -m ingestion")
    p.add_argument("--source", required=True, choices=["binance", "blockchain", "paysim", "injector"])
    p.add_argument("--csv", help="PaySim CSV path (paysim only)")
    p.add_argument("--rate", type=float, default=None, help="Events per second (paysim/injector)")
    p.add_argument("--symbols", nargs="+", default=list(binance.DEFAULT_SYMBOLS), help="Binance symbols")
    p.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092"))
    args = p.parse_args()
    if args.rate is None:
        args.rate = 4.0 if args.source == "injector" else 100.0
    if args.source == "paysim" and not args.csv:
        p.error("--csv is required for --source paysim")
    return args


async def run(args: argparse.Namespace) -> None:
    if args.source == "binance":
        batches = binance.stream(args.symbols)
    elif args.source == "blockchain":
        batches = blockchain.stream()
    elif args.source == "injector":
        batches = injector.stream(args.rate)
    else:
        batches = paysim.replay(args.csv, args.rate)
    async with EventProducer(args.bootstrap) as producer:
        async for events in batches:
            await producer.send(events)
        logging.info("done, sent %d events", producer.sent)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(parse_args()))


if __name__ == "__main__":
    main()
