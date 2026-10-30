# Ingestion & Fraud Injector Walkthrough (Phase 0, 1, and 2)

## What Was Built

Live Binance trades (20 USDT pairs), Blockchain unconfirmed transactions, the synthetic **Fraud Injector** (`large_amount`, `burst`, `smurfing`, `rapid_relay`), and the PaySim CSV replay source all publish normalized [`Event`](../ingestion/schema.py) records to the 3-partition Kafka topic `transactions`.

| File | Role |
|---|---|
| [schema.py](../ingestion/schema.py) | Frozen `Event` dataclass with strict JSON serialization and boundary validation. |
| [producer.py](../ingestion/producer.py) | `ensure_topic`, `partition_key(event)` (`event_id` for `binance`, `sender` for wallet sources), and `EventProducer`. |
| [ws.py](../ingestion/sources/ws.py) | Reconnecting WebSocket loop with exponential backoff (1s to 30s), URL rotation, and macOS CA bundle fallback. |
| [binance.py](../ingestion/sources/binance.py) | Combined `@trade` stream parser for 20 USDT pairs with `data-stream.binance.vision` fallback. |
| [blockchain.py](../ingestion/sources/blockchain.py) | Unconfirmed Bitcoin transaction parser emitting one `sender -> receiver` event per output with an address. |
| [injector.py](../ingestion/sources/injector.py) | Synthetic fraud generator (`large_amount`, `burst`, `smurfing`, `rapid_relay`) with smooth weighted round-robin scheduling. |
| [paysim.py](../ingestion/sources/paysim.py) | Configurable-rate PaySim CSV replay source mapping `isFraud` to `is_injected`. |
| [docker-compose.yml](../docker-compose.yml) | Kafka KRaft broker, Kafka UI, `ingest-binance`, `ingest-blockchain`, and `ingest-injector`. |
| [verify_ingestion.py](../scripts/verify_ingestion.py) | Validates schema, partition key routing, per-source counts, and `fraud_pattern` breakdowns. |

## Commits (`origin/main` at `https://github.com/iamqazolp/pigdata-dulieulon`)

```text
96c7d56 2026-10-16 14:23:18 +0700 Add project architecture plan and presentation slides
475e5f3 2026-10-17 10:41:52 +0700 Initialize uv Python 3.11 project and gitignore
45d6e7a 2026-10-18 16:12:05 +0700 Define unified Event schema with JSON serialization and validation
93d5aaa 2026-10-19 20:34:47 +0700 Add async Kafka EventProducer and topic bootstrap helper
2b19d36 2026-10-21 15:08:33 +0700 Add reconnecting WebSocket client with exponential backoff
8feea3c 2026-10-22 11:27:14 +0700 Implement Binance trade stream parser and live test fixtures
c0ba926 2026-10-23 19:45:29 +0700 Implement Blockchain unconfirmed transaction parser and fixtures
9f6406a 2026-10-24 14:19:03 +0700 Add PaySim CSV replay source and synthetic sample generator
fea8189 2026-10-25 17:52:41 +0700 Add parser unit test suite across Binance, Blockchain, and PaySim
0c2e358 2026-10-26 10:15:38 +0700 Add ingestion CLI entrypoint for stream and replay sources
30a07b1 2026-10-27 21:06:12 +0700 Check existing Kafka topics before creation and handle response codes
9e3b325 2026-10-28 15:39:54 +0700 Containerize ingestion service and add Kafka KRaft compose stack
00a2908 2026-10-29 11:22:09 +0700 Add Kafka ingestion verification script
6937da5 2026-10-29 20:48:17 +0700 Expand Binance stream to 20 USDT pairs and load macOS CA bundle
044d927 2026-10-30 01:15:22 +0700 Add synthetic fraud injector patterns and fix Binance partition key skew
e294d4f 2026-10-30 06:42:10 +0700 Wire fraud injector into CLI, Docker Compose, and Kafka verification
```

## Verification Receipts

| Check | Result |
|---|---|
| `uv run pytest -q` | `18 passed in 0.91s` (covers all parsers, `partition_key`, all 4 fraud patterns, and 200-incident stream distribution) |
| Combined live stream + injector capture (`phase2_capture.json`) | 11,572 events (`binance`: 11,471, `blockchain`: 53, `injector`: 48 across `burst`: 25, `smurfing`: 10, `large_amount`: 9, `rapid_relay`: 4) |
| Partition balance (`partition_key(event)`) | Partition 0: **34.15%** (3,952), Partition 1: **32.47%** (3,757), Partition 2: **33.38%** (3,863) |
| 400-incident fraud pattern distribution | 1,257 events: `burst` **36.0%** (target 35%), `large_amount` **24.7%** (target 25%), `smurfing` **24.3%** (target 25%), `rapid_relay` **15.0%** (target 15%) |
