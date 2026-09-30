"""Tests for the synthetic webhook generator.

Purpose: check determinism, the raw file contracts, the required data properties and that the
    generator's ground truth summary reconciles with the written files.
Inputs: a generated dataset of 60,000 transactions in a temporary directory.
Outputs: pytest assertions. Files are re-read with the standard ``json`` and ``csv`` modules, a
    code path that is independent of the Polars writer used by the generator.
"""

import csv
import json
from collections import Counter
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from data_gen.generate import GeneratorConfig, generate

N_TRANSACTIONS = 60_000
CURRENCY_BY_COUNTRY = {"MX": "MXN", "CO": "COP", "CL": "CLP", "BR": "BRL"}
METHODS_BY_COUNTRY = {
    "MX": {"card", "oxxo", "spei"},
    "CO": {"card", "pse"},
    "CL": {"card", "webpay"},
    "BR": {"card", "pix", "boleto"},
}
REASONS_BY_STATUS = {
    "declined": {"insufficient_funds", "card_declined", "fraud_suspected"},
    "failed": {"processor_error", "network_timeout"},
    "expired": {"expired"},
}
STATUSES = {"approved", "declined", "failed", "expired", "pending", "refunded"}
Dataset = tuple[dict[str, Any], list[dict[str, Any]], Path]


def _read_csv(path: Path) -> list[dict[str, str]]:
    """Read a CSV file into a list of row dictionaries."""
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def _latest_by_transaction(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Return the latest event per transaction by ``event_at``, ignoring arrival order."""
    latest: dict[str, dict[str, Any]] = {}
    for row in rows:
        current = latest.get(row["transaction_id"])
        if current is None or row["event_at"] > current["event_at"]:
            latest[row["transaction_id"]] = row
    return latest


def _local(timestamp: str, zone: str) -> datetime:
    """Parse an ISO 8601 UTC timestamp and convert it to a local time zone."""
    return datetime.fromisoformat(timestamp.replace("Z", "+00:00")).astimezone(ZoneInfo(zone))


@pytest.fixture(scope="module")
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Dataset:
    """Generate one dataset and return its summary, its deliveries and its directory."""
    raw_dir = tmp_path_factory.mktemp("raw")
    summary = generate(GeneratorConfig(n_transactions=N_TRANSACTIONS, output_dir=raw_dir))
    rows = [json.loads(line) for line in (raw_dir / "webhooks.jsonl").read_text().splitlines()]
    return summary, rows, raw_dir


def test_same_seed_produces_identical_files(tmp_path: Path) -> None:
    """Two runs with the same seed write byte identical files."""
    first, second = tmp_path / "first", tmp_path / "second"
    generate(GeneratorConfig(n_transactions=2_000, output_dir=first))
    generate(GeneratorConfig(n_transactions=2_000, output_dir=second))

    names = sorted(path.name for path in first.iterdir())
    assert names == ["merchants.csv", "planted_anomalies.json", "psp_fees.csv", "webhooks.jsonl"]
    for name in names:
        assert (first / name).read_bytes() == (second / name).read_bytes()


def test_different_seed_produces_different_data(tmp_path: Path) -> None:
    """Changing the seed changes the generated outcomes."""
    first = generate(GeneratorConfig(n_transactions=2_000, seed=1, output_dir=tmp_path / "a"))
    second = generate(GeneratorConfig(n_transactions=2_000, seed=2, output_dir=tmp_path / "b"))
    assert first["status_counts"] != second["status_counts"]


def test_rejects_non_positive_transaction_count(tmp_path: Path) -> None:
    """A transaction count of zero is rejected before any file is written."""
    with pytest.raises(ValueError, match="n_transactions and days must be positive"):
        generate(GeneratorConfig(n_transactions=0, output_dir=tmp_path))
    assert not (tmp_path / "webhooks.jsonl").exists()


def test_summary_reconciles_with_the_file(dataset: Dataset) -> None:
    """Delivery, event, duplicate, transaction and status counts match a plain re-read."""
    summary, rows, _ = dataset
    event_ids = {row["event_id"] for row in rows}
    latest = _latest_by_transaction(rows)

    assert len(rows) == summary["n_deliveries"]
    assert len(event_ids) == summary["n_events_unique"]
    assert len(rows) - len(event_ids) == summary["n_duplicate_deliveries"]
    assert len(latest) == summary["n_transactions"] == N_TRANSACTIONS
    counts = Counter(row["status"] for row in latest.values())
    assert {status: counts[status] for status in summary["status_counts"]} == summary[
        "status_counts"
    ]


def test_deliveries_respect_the_raw_contract(dataset: Dataset) -> None:
    """Every delivery has the agreed fields, vocabulary, money and timestamps."""
    _, rows, _ = dataset
    expected_fields = [
        "event_id", "transaction_id", "merchant_id", "country", "currency", "amount_minor",
        "payment_method", "card_brand", "psp", "status", "decline_reason", "created_at",
        "event_at",
    ]  # fmt: skip
    for row in rows:
        assert list(row) == expected_fields
        assert row["currency"] == CURRENCY_BY_COUNTRY[row["country"]]
        assert row["payment_method"] in METHODS_BY_COUNTRY[row["country"]]
        assert row["status"] in STATUSES
        assert isinstance(row["amount_minor"], int)
        assert row["amount_minor"] > 0
        if row["currency"] == "COP":
            assert row["amount_minor"] % 100 == 0
        if row["payment_method"] == "card":
            assert row["card_brand"] in {"visa", "mastercard"}
        else:
            assert row["card_brand"] is None
        if row["status"] in REASONS_BY_STATUS:
            assert row["decline_reason"] in REASONS_BY_STATUS[row["status"]]
        else:
            assert row["decline_reason"] is None
        assert row["created_at"].endswith("Z")
        assert row["event_at"] >= row["created_at"]
        assert (row["status"] == "pending") == (row["event_at"] == row["created_at"])


def test_window_covers_the_last_90_local_days(dataset: Dataset) -> None:
    """Transactions are created on each of the 90 local days ending on the end date."""
    _, rows, _ = dataset
    zones = {
        "MX": "America/Mexico_City",
        "CO": "America/Bogota",
        "CL": "America/Santiago",
        "BR": "America/Sao_Paulo",
    }
    days = {_local(row["created_at"], zones[row["country"]]).date() for row in rows}
    assert min(days) == date(2026, 7, 3)
    assert max(days) == date(2026, 9, 30)
    assert len(days) == 90


def test_final_status_mix_is_roughly_as_specified(dataset: Dataset) -> None:
    """Final states are roughly approved 75%, declined 18%, failed 3%, and the rest."""
    summary, _, _ = dataset
    share = {name: count / N_TRANSACTIONS for name, count in summary["status_counts"].items()}
    assert share["approved"] == pytest.approx(0.75, abs=0.03)
    assert share["declined"] == pytest.approx(0.18, abs=0.03)
    assert share["failed"] == pytest.approx(0.03, abs=0.01)
    assert all(share[name] > 0 for name in ("pending", "expired", "refunded"))


def test_new_psps_are_live_in_colombia_for_the_last_45_days_only(dataset: Dataset) -> None:
    """PSP_C and PSP_D appear only in Colombia and only from 2026-08-17 (local) onwards."""
    _, rows, _ = dataset
    assert {row["psp"] for row in rows} == {"PSP_A", "PSP_B", "PSP_C", "PSP_D"}
    new_psp_rows = [row for row in rows if row["psp"] in {"PSP_C", "PSP_D"}]
    assert {row["country"] for row in new_psp_rows} == {"CO"}
    first_day = min(_local(row["created_at"], "America/Bogota").date() for row in new_psp_rows)
    assert first_day == date(2026, 8, 17)


def test_merchants_have_a_long_tail(dataset: Dataset) -> None:
    """There are 120 merchants and the top 10 carry far more than their 8% headcount share."""
    _, rows, raw_dir = dataset
    merchants = _read_csv(raw_dir / "merchants.csv")
    assert list(merchants[0]) == ["merchant_id", "country", "category", "size_tier"]
    assert len({merchant["merchant_id"] for merchant in merchants}) == 120
    assert {merchant["size_tier"] for merchant in merchants} == {"enterprise", "mid", "small"}

    latest = _latest_by_transaction(rows)
    volume = Counter(row["merchant_id"] for row in latest.values())
    assert set(volume) <= {merchant["merchant_id"] for merchant in merchants}
    country = {merchant["merchant_id"]: merchant["country"] for merchant in merchants}
    assert all(country[row["merchant_id"]] == row["country"] for row in latest.values())
    top_10 = sum(count for _, count in volume.most_common(10))
    assert top_10 / N_TRANSACTIONS > 0.4


def test_psp_fees_cover_every_traded_segment(dataset: Dataset) -> None:
    """Every PSP, country and method combination in the events has exactly one fee row."""
    _, rows, raw_dir = dataset
    fees = _read_csv(raw_dir / "psp_fees.csv")
    assert list(fees[0]) == ["psp", "country", "payment_method", "pct_fee", "fixed_fee_usd"]
    fee_keys = [(fee["psp"], fee["country"], fee["payment_method"]) for fee in fees]
    assert len(fee_keys) == len(set(fee_keys))
    assert {(row["psp"], row["country"], row["payment_method"]) for row in rows} == set(fee_keys)
    assert all(float(fee["pct_fee"]) > 0 and float(fee["fixed_fee_usd"]) > 0 for fee in fees)


def test_duplicates_repeat_the_payload(dataset: Dataset) -> None:
    """About 1% of events are delivered twice with an identical payload."""
    summary, rows, _ = dataset
    payloads: dict[str, set[str]] = {}
    for row in rows:
        payloads.setdefault(row["event_id"], set()).add(json.dumps(row, sort_keys=True))

    assert all(len(variants) == 1 for variants in payloads.values())
    assert summary["n_duplicate_deliveries"] / summary["n_events_unique"] == pytest.approx(
        0.01, abs=0.003
    )


def test_some_events_arrive_out_of_order(dataset: Dataset) -> None:
    """Some transactions deliver their "pending" event after a later status change."""
    _, rows, _ = dataset
    seen_later_status: set[str] = set()
    out_of_order: set[str] = set()
    for row in rows:
        if row["status"] != "pending":
            seen_later_status.add(row["transaction_id"])
        elif row["transaction_id"] in seen_later_status:
            out_of_order.add(row["transaction_id"])
    assert out_of_order


def test_seasonality_by_hour_and_weekday(dataset: Dataset) -> None:
    """Evenings are busier than nights and Fridays are busier than Sundays."""
    _, rows, _ = dataset
    created = [
        _local(row["created_at"], "America/Bogota")
        for row in _latest_by_transaction(rows).values()
        if row["country"] == "CO"
    ]
    hours = Counter(moment.hour for moment in created)
    weekdays = Counter(moment.weekday() for moment in created)
    assert hours[20] > 3 * hours[3]
    # The window holds 13 Fridays and 12 Sundays, so compare per day averages.
    assert weekdays[4] / 13 > weekdays[6] / 12


def test_planted_anomalies_are_recoverable(dataset: Dataset) -> None:
    """The ground truth file describes anomalies that are visible in the events."""
    _, rows, raw_dir = dataset
    planted = json.loads((raw_dir / "planted_anomalies.json").read_text())
    anomalies = {anomaly["id"]: anomaly for anomaly in planted["anomalies"]}
    assert set(anomalies) == {"a", "b", "c", "d", "e"}
    assert anomalies["d"]["expected_diagnosis"] == "processing"
    assert anomalies["e"]["expected_diagnosis"] == "ux"
    latest = list(_latest_by_transaction(rows).values())

    # a) PSP_C card declines in Colombia: spike days versus the other PSP_C days.
    spike = anomalies["a"]
    assert (spike["psp"], spike["country"], spike["payment_method"]) == ("PSP_C", "CO", "card")
    assert (spike["start_date"], spike["end_date"]) == ("2026-09-01", "2026-09-03")
    segment = [
        row
        for row in latest
        if (row["psp"], row["country"], row["payment_method"]) == ("PSP_C", "CO", "card")
    ]
    in_spike = [
        row
        for row in segment
        if spike["start_date"]
        <= _local(row["created_at"], "America/Bogota").date().isoformat()
        <= spike["end_date"]
    ]
    spike_rate = sum(row["status"] == "declined" for row in in_spike) / len(in_spike)
    normal = len(segment) - len(in_spike)
    normal_rate = (
        sum(row["status"] == "declined" for row in segment)
        - sum(row["status"] == "declined" for row in in_spike)
    ) / normal
    assert spike_rate / normal_rate == pytest.approx(2.0, abs=0.6)

    # b) One Mexican merchant with about 95% OXXO expiration among resolved vouchers.
    voucher = anomalies["b"]
    merchant_oxxo = [
        row
        for row in latest
        if row["merchant_id"] == voucher["merchant_id"]
        and row["payment_method"] == "oxxo"
        and row["status"] != "pending"
    ]
    assert len(merchant_oxxo) > 50
    expired = sum(row["status"] == "expired" for row in merchant_oxxo) / len(merchant_oxxo)
    assert expired == pytest.approx(0.95, abs=0.06)

    # c) Timeout window: one weekend, 02:00 to 04:00 local time, a single PSP.
    timeout = anomalies["c"]
    assert timeout["dates"] == ["2026-09-12", "2026-09-13"]
    assert [date.fromisoformat(day).weekday() for day in timeout["dates"]] == [5, 6]
    assert timeout["local_hours"] == [2, 3]
    assert timeout["decline_reason"] == "network_timeout"
