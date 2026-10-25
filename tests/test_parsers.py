import asyncio
import copy
import csv
import json
from pathlib import Path

import pytest

from ingestion.schema import Event
from ingestion.sources import binance, blockchain, paysim

FIXTURES = Path(__file__).parent / "fixtures"


def load(name):
    return json.loads((FIXTURES / name).read_text())


def test_binance_trade_maps_to_event():
    raw = load("binance_trades.json")[0]
    assert binance.parse(raw) == [
        Event(
            event_id="binance:ETHUSDT:4460249656",
            timestamp=1792665713097,
            source="binance",
            sender="ETHUSDT",
            receiver="ETHUSDT",
            amount=pytest.approx(2408.39 * 0.534),
            price=2408.39,
            type="trade_sell",
        )
    ]


def test_binance_every_recorded_message_parses_to_one_event():
    raws = load("binance_trades.json")
    events = [e for raw in raws for e in binance.parse(raw)]
    assert len(events) == len(raws)
    assert {e.sender for e in events} == {"BTCUSDT", "ETHUSDT"}
    assert all(e.amount > 0 and e.price > 0 for e in events)
    assert all(e.type in ("trade_buy", "trade_sell") for e in events)


def test_binance_buyer_taker_is_trade_buy():
    raw = copy.deepcopy(load("binance_trades.json")[0])
    raw["data"]["m"] = False
    assert binance.parse(raw)[0].type == "trade_buy"


def test_binance_ignores_non_trade_messages():
    assert binance.parse({"result": None, "id": 1}) == []


def test_blockchain_tx_becomes_one_event_per_output():
    raw = load("blockchain_utx.json")[0]
    assert blockchain.parse(raw) == [
        Event(
            event_id="blockchain:" + raw["x"]["hash"] + ":0",
            timestamp=1792665711000,
            source="blockchain",
            sender="14NyAhctbFmbCikPnzqLLDr8FsAhJsUGG3",
            receiver="bc1q7tc2q70nz3z04swjgj3za8d5gm5wnjtvpjj67l",
            amount=0.00024372,
            price=None,
            type="transfer",
        ),
        Event(
            event_id="blockchain:" + raw["x"]["hash"] + ":1",
            timestamp=1792665711000,
            source="blockchain",
            sender="14NyAhctbFmbCikPnzqLLDr8FsAhJsUGG3",
            receiver="1ED5rePQn9qJYGi1aZwwNsSVFNMpAD5up2",
            amount=0.00029156,
            price=None,
            type="transfer",
        ),
    ]


def test_blockchain_every_recorded_tx_parses():
    raws = load("blockchain_utx.json")
    for raw in raws:
        events = blockchain.parse(raw)
        assert len(events) == len(raw["x"]["out"])
        assert len({e.event_id for e in events}) == len(events)


def test_blockchain_drops_outputs_without_address():
    raw = copy.deepcopy(load("blockchain_utx.json")[0])
    del raw["x"]["out"][0]["addr"]
    events = blockchain.parse(raw)
    assert [e.receiver for e in events] == ["1ED5rePQn9qJYGi1aZwwNsSVFNMpAD5up2"]


def test_blockchain_ignores_non_utx_messages():
    assert blockchain.parse({"op": "pong"}) == []


PAYSIM_HEADER = "step,type,amount,nameOrig,oldbalanceOrg,newbalanceOrig,nameDest,oldbalanceDest,newbalanceDest,isFraud,isFlaggedFraud"
PAYSIM_ROWS = [
    "1,PAYMENT,9839.64,C1231006815,170136.0,160296.36,M1979787155,0.0,0.0,0,0",
    "3,TRANSFER,181.0,C1305486145,181.0,0.0,C553264065,0.0,0.0,1,0",
]


def test_paysim_rows_map_to_events(tmp_path):
    path = tmp_path / "paysim.csv"
    path.write_text("\n".join([PAYSIM_HEADER, *PAYSIM_ROWS]) + "\n")
    with path.open() as f:
        rows = list(csv.DictReader(f))
    start = 1_000_000
    events = [e for i, row in enumerate(rows) for e in paysim.parse(row, i, start)]
    assert events == [
        Event("paysim:0", start + 3_600_000, "paysim", "C1231006815", "M1979787155", 9839.64, None, "PAYMENT"),
        Event("paysim:1", start + 3 * 3_600_000, "paysim", "C1305486145", "C553264065", 181.0, None, "TRANSFER", True, "paysim_fraud"),
    ]


async def collect(gen):
    return [e async for batch in gen for e in batch]


def test_paysim_replay_reads_whole_file(tmp_path):
    path = tmp_path / "paysim.csv"
    path.write_text("\n".join([PAYSIM_HEADER, *PAYSIM_ROWS]) + "\n")
    events = asyncio.run(collect(paysim.replay(str(path), rate=1000)))
    assert [e.event_id for e in events] == ["paysim:0", "paysim:1"]


def test_event_json_round_trip():
    event = binance.parse(load("binance_trades.json")[0])[0]
    assert Event.from_json(event.to_json()) == event


def test_event_from_json_rejects_missing_fields():
    payload = json.loads(binance.parse(load("binance_trades.json")[0])[0].to_json())
    del payload["sender"]
    with pytest.raises(ValueError):
        Event.from_json(json.dumps(payload))
