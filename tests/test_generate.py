"""Tests for the synthetic webhook generator.

Purpose: check determinism, the raw JSON contract and that the manifest reconciles with the files.
Inputs: a small generated dataset (scale 0.005, about 6,000 transactions) in a temporary directory.
Outputs: pytest assertions. Files are re-read with the standard ``json`` module, a code path that
    is independent of the Polars writer used by the generator.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from data_gen.generate import (
    CURRENCIES,
    METHODS,
    PSPS,
    STATUSES,
    GeneratorConfig,
    generate,
)

SCALE = 0.005
CURRENCY_BY_COUNTRY = {"MX": "MXN", "CO": "COP", "CL": "CLP"}


def _read_deliveries(raw_dir: Path) -> list[dict[str, Any]]:
    """Read every delivery, tagging each with the partition date it was stored under."""
    rows: list[dict[str, Any]] = []
    for path in sorted((raw_dir / "events").glob("received_date=*/events.jsonl")):
        partition_date = path.parent.name.split("=")[1]
        for line in path.read_text().splitlines():
            rows.append({**json.loads(line), "_partition_date": partition_date})
    return rows


@pytest.fixture(scope="module")
def dataset(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Generate one small dataset and return its manifest and deliveries."""
    raw_dir = tmp_path_factory.mktemp("raw")
    manifest = generate(GeneratorConfig(scale=SCALE, output_dir=raw_dir))
    return manifest, _read_deliveries(raw_dir)


def test_same_seed_produces_identical_files(tmp_path: Path) -> None:
    """Two runs with the same seed write byte identical partitions and manifests."""
    first, second = tmp_path / "first", tmp_path / "second"
    generate(GeneratorConfig(scale=SCALE, output_dir=first))
    generate(GeneratorConfig(scale=SCALE, output_dir=second))

    first_files = sorted(p.relative_to(first) for p in first.rglob("*") if p.is_file())
    second_files = sorted(p.relative_to(second) for p in second.rglob("*") if p.is_file())
    assert first_files == second_files
    assert len(first_files) > 1
    for relative in first_files:
        assert (first / relative).read_bytes() == (second / relative).read_bytes()


def test_different_seed_produces_different_data(tmp_path: Path) -> None:
    """Changing the seed changes the generated counts."""
    first = generate(GeneratorConfig(scale=SCALE, seed=1, output_dir=tmp_path / "a"))
    second = generate(GeneratorConfig(scale=SCALE, seed=2, output_dir=tmp_path / "b"))
    assert first["status_counts"] != second["status_counts"]


def test_manifest_reconciles_with_files(
    dataset: tuple[dict[str, Any], list[dict[str, Any]]],
) -> None:
    """Delivery, event, duplicate and transaction counts match a plain re-read of the files."""
    manifest, rows = dataset
    event_ids = {row["event_id"] for row in rows}

    assert manifest["n_transactions_generated"] == 6000  # 400,000 * 90 / 30 * 0.005
    assert len(rows) == manifest["n_deliveries"]
    assert len(event_ids) == manifest["n_events_unique"]
    assert len(rows) - len(event_ids) == manifest["n_duplicate_deliveries"]
    assert manifest["n_duplicate_deliveries"] > 0
    assert len({row["transaction_id"] for row in rows}) == manifest["n_transactions"]
    assert sum(manifest["status_counts"].values()) == manifest["n_transactions"]


def test_manifest_status_counts_match_latest_delivered_event(
    dataset: tuple[dict[str, Any], list[dict[str, Any]]],
) -> None:
    """The latest event by ``occurred_at`` per transaction reproduces the manifest counts."""
    manifest, rows = dataset
    latest: dict[str, tuple[str, str]] = {}
    for row in rows:
        candidate = (row["occurred_at"], row["status"])
        if row["transaction_id"] not in latest or candidate > latest[row["transaction_id"]]:
            latest[row["transaction_id"]] = candidate

    counts = {status: 0 for status in STATUSES}
    for _, status in latest.values():
        counts[status] += 1
    assert counts == manifest["status_counts"]


def test_deliveries_respect_the_raw_contract(
    dataset: tuple[dict[str, Any], list[dict[str, Any]]],
) -> None:
    """Every delivery has valid vocabulary values, money and timestamps."""
    _, rows = dataset
    for row in rows:
        assert row["currency"] == CURRENCY_BY_COUNTRY[row["country"]]
        assert row["currency"] in CURRENCIES
        assert row["status"] in STATUSES
        assert row["payment_method"] in METHODS
        assert row["psp"] in PSPS
        assert isinstance(row["amount_minor"], int)
        assert row["amount_minor"] > 0
        if row["currency"] == "COP":
            assert row["amount_minor"] % 100 == 0
        assert (row["card_brand"] is not None) == (row["payment_method"] == "card")
        assert (row["error_code"] is not None) == (row["status"] in {"declined", "failed"})
        assert row["occurred_at"].endswith("Z")
        assert row["received_at"] >= row["occurred_at"]
        assert row["received_at"][:10] == row["_partition_date"]


def test_duplicates_repeat_the_payload_and_arrive_later(
    dataset: tuple[dict[str, Any], list[dict[str, Any]]],
) -> None:
    """A duplicated ``event_id`` differs from the original only in its arrival time."""
    _, rows = dataset
    by_event: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_event.setdefault(row["event_id"], []).append(row)

    duplicated = [group for group in by_event.values() if len(group) > 1]
    assert duplicated
    for group in duplicated:
        payloads = {
            json.dumps(
                {k: v for k, v in row.items() if k not in {"received_at", "_partition_date"}},
                sort_keys=True,
            )
            for row in group
        }
        assert len(payloads) == 1
        assert len({row["received_at"] for row in group}) == len(group)


def test_some_events_arrive_out_of_order(
    dataset: tuple[dict[str, Any], list[dict[str, Any]]],
) -> None:
    """At least one transaction receives its "pending" event after a later event."""
    _, rows = dataset
    first_pending: dict[str, str] = {}
    first_other: dict[str, str] = {}
    for row in rows:
        target = first_pending if row["status"] == "pending" else first_other
        previous = target.get(row["transaction_id"])
        if previous is None or row["received_at"] < previous:
            target[row["transaction_id"]] = row["received_at"]

    out_of_order = [
        txn
        for txn, received in first_pending.items()
        if txn in first_other and received > first_other[txn]
    ]
    assert out_of_order


def test_rejects_non_positive_scale(tmp_path: Path) -> None:
    """A scale of zero is rejected before any file is written."""
    with pytest.raises(ValueError, match="scale and days must be positive"):
        generate(GeneratorConfig(scale=0.0, output_dir=tmp_path))
    assert not (tmp_path / "events").exists()
