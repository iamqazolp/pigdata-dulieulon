# Live Demo Command Cheatsheet (IT4931)

All commands run from `/Users/iamqazolp/Documents/Projects/pigdata`.

## 1. Instant Live Demo (No Docker Required — Runs in 6 Seconds)

Use this command first to show live Binance WebSocket trades, live Blockchain unconfirmed Bitcoin transactions, all 4 synthetic Fraud Injector patterns, and the 3-partition balance in one screen:

```bash
uv run scripts/demo_inspect.py --seconds 6
```

## 2. Unit Tests & Schema/Pattern Verification (< 1 Second)

Show that all 18 tests pass across Binance, Blockchain, PaySim, the 4 Fraud Injector patterns (`large_amount`, `burst`, `smurfing`, `rapid_relay`), and the Kafka `partition_key` router:

```bash
uv run pytest -v
```

## 3. Inspect Individual Fraud Injector Patterns (One-Liners)

**Show a 4–6 hop `rapid_relay` peeling chain (`receiver` of hop `i` becomes `sender` of hop `i+1`):**
```bash
uv run python -c "
import random
from ingestion.sources.injector import generate_incident
for i, e in enumerate(generate_incident('rapid_relay', random.Random(7), 1793319600000)):
    print(f'Hop {i+1}: {e.sender[:14]}... -> {e.receiver[:14]}... | {e.amount:.4f} BTC | t=+{e.timestamp-1793319600000}ms')
"
```

**Show a `smurfing` split (1 sender wallet splitting funds across 8–12 distinct receivers at the exact same millisecond):**
```bash
uv run python -c "
import random
from ingestion.sources.injector import generate_incident
for i, e in enumerate(generate_incident('smurfing', random.Random(7), 1793319600000)):
    print(f'Out {i+1:02d}: {e.sender[:14]}... -> {e.receiver[:14]}... | {e.amount:.5f} BTC | pattern={e.fraud_pattern}')
"
```

## 4. Full Docker + Kafka Stack Commands

If Colima is asleep after laptop sleep, restart Colima first:
```bash
colima restart
```

**Start Kafka KRaft, Kafka UI (`http://localhost:8081`), `ingest-binance`, `ingest-blockchain`, and `ingest-injector`:**
```bash
docker compose up -d --build
docker compose ps
```

**Verify 2,000 live messages from Kafka topic `transactions` across all 3 live services:**
```bash
uv run scripts/verify_ingestion.py -n 2000 --require binance blockchain injector
```

**Replay the PaySim fallback dataset into Kafka at 500 events/s:**
```bash
uv run scripts/make_paysim_sample.py data/paysim_sample.csv 1000
uv run python -m ingestion --source paysim --csv data/paysim_sample.csv --rate 500
```

**Demonstrate Fault Tolerance (Restart Kafka Broker Mid-Stream):**
```bash
docker compose restart kafka
sleep 20
uv run scripts/verify_ingestion.py -n 500 --require binance blockchain injector
```

## 5. Git Commit Timeline Check

```bash
git log --format="%h  %ai  %s" --reverse
```
