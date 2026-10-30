# Ingestion Data Visual Report

This report visualizes a 25-second live capture from [binance.py](../ingestion/sources/binance.py) and [blockchain.py](../ingestion/sources/blockchain.py), alongside the 1,000-row sample from [paysim.py](../ingestion/sources/paysim.py).

## 1. Binance Market Stream (5,508 trades in 25s)

![Binance stream distribution](./binance_distribution.png)

| Metric | Measured Value | Fraud Detection Impact |
|---|---|---|
| Throughput | 220.3 trades/s (5,508 trades in 25s) | High baseline volume stresses Kafka partitioning and Spark window state |
| Total notional volume | $1,258,379.72 USDT | Concentrated in `BTCUSDT` ($432.1k), `UNIUSDT` ($271.3k), and `ETHUSDT` ($172.1k) |
| Trade size percentiles | p50 = $5.68, p95 = $998.41, p99 = $5,002.01, max = $31,043.93 | A cluster around $5–$6 reflects exchange minimum notional order sizes |
| Top frequency pairs | `ETHUSDT` (1,685), `BTCUSDT` (1,465), `PEPEUSDT` (399), `UNIUSDT` (391) | Top 2 pairs generate 57.2% of all Binance events |

> [!IMPORTANT]
> Because `BTCUSDT` p99 exceeds $15,000 while smaller altcoins sit near $500, the Phase 3 batch layer and Phase 4 speed layer must track `large_amount` thresholds per symbol rather than using a single global USDT threshold.

## 2. Blockchain UTXO Fan-Out & PaySim Fallback

![Blockchain and PaySim structure](./blockchain_paysim_structure.png)

| Metric | Measured Value | Fraud Detection Impact |
|---|---|---|
| UTXO fan-out | 107 distinct transactions produced 184 output events (1.72 events/tx) | 56 transactions had 1 output, 43 had 2 outputs (payment + change), and 8 had 3+ outputs |
| Self-change outputs | 68 of 184 events (37.0%) had `sender == receiver` | Filtering out `sender == receiver` prevents normal wallet change from triggering `burst` or `smurfing` alerts |
| BTC output percentiles | p50 = 0.0010 BTC, p95 = 0.3082 BTC, p99 = 16.5833 BTC, max = 41.3353 BTC | Heavy right tail spans 6 orders of magnitude (10 µBTC to 41.3 BTC) |
| PaySim fallback | 1,000 events across 5 types, 10 labeled fraud (`is_injected=true`) | All fraud rows occur in `TRANSFER` and `CASH_OUT` |

## 3. Kafka Partition Skew & Phase 2 Fraud Mix

![Partition skew and Phase 2 pattern mix](./partition_skew_and_phase2.png)

- **Measured skew.** Across 1,327,457 Kafka records, Partition 2 received 85.2% (1,131,211 records), Partition 0 received 12.1% (161,097), and Partition 1 received 2.6% (35,149).
- **Root cause.** [`EventProducer.send`](../ingestion/producer.py#L48-L54) keys every record by `event.sender`. For Binance, `sender` is the trading pair name, and both `ETHUSDT` and `BTCUSDT` hash to Partition 2.
- **Measured fix.** Keying Binance events by `event.event_id` while keeping `event.sender` for address-based sources (`blockchain`, `paysim`, `injector`) distributes the 5,692 captured events into 34.3% / 32.9% / 32.8% across Partitions 0, 1, and 2.
