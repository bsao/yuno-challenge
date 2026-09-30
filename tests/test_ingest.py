"""Tests for raw to staging ingestion.

Purpose: check deduplication, latest status selection and the derived columns against hand
    computed expectations.
Inputs: a tiny in memory fixture of webhook deliveries covering duplicates, out of order arrival,
    timestamp ties and refunds.
Outputs: pytest assertions.
"""

from datetime import UTC, datetime

import polars as pl
import pytest

from pipeline.ingest import (
    add_derived_columns,
    count_out_of_order,
    deduplicate_events,
    latest_per_transaction,
)


def _at(minute: int) -> datetime:
    """Return a UTC timestamp ``minute`` minutes after 2026-09-01 12:00."""
    return datetime(2026, 9, 1, 12, minute, tzinfo=UTC)


# (event_id, transaction_id, status, decline_reason, event_at minute), listed in arrival order.
DELIVERIES: list[tuple[str, str, str, str | None, int]] = [
    # txn_1: in order, with the approved event delivered twice.
    ("evt_01", "txn_1", "pending", None, 0),
    ("evt_02", "txn_1", "approved", None, 1),
    ("evt_02", "txn_1", "approved", None, 1),
    # txn_2: out of order, the final status arrives before "pending".
    ("evt_04", "txn_2", "declined", "insufficient_funds", 6),
    ("evt_03", "txn_2", "pending", None, 5),
    # txn_3: never resolved.
    ("evt_05", "txn_3", "pending", None, 10),
    # txn_4: fully reversed arrival, and the late "pending" is also delivered twice.
    ("evt_08", "txn_4", "refunded", None, 30),
    ("evt_07", "txn_4", "approved", None, 21),
    ("evt_06", "txn_4", "pending", None, 20),
    ("evt_06", "txn_4", "pending", None, 20),
    # txn_5: both events share a timestamp, so the lifecycle rank decides.
    ("evt_10", "txn_5", "failed", "network_timeout", 40),
    ("evt_09", "txn_5", "pending", None, 40),
]


def _deliveries(rows: list[tuple[str, str, str, str | None, int]]) -> pl.LazyFrame:
    """Build a typed delivery frame in the given arrival order."""
    return pl.DataFrame(
        {
            "arrival_index": list(range(len(rows))),
            "event_id": [row[0] for row in rows],
            "transaction_id": [row[1] for row in rows],
            "merchant_id": ["mrc_001"] * len(rows),
            "country": ["MX"] * len(rows),
            "currency": ["MXN"] * len(rows),
            "amount_minor": [12_345] * len(rows),
            "payment_method": ["card"] * len(rows),
            "card_brand": ["visa"] * len(rows),
            "psp": ["PSP_A"] * len(rows),
            "status": [row[2] for row in rows],
            "decline_reason": [row[3] for row in rows],
            "created_at": [_at(0)] * len(rows),
            "event_at": [_at(row[4]) for row in rows],
        },
        schema_overrides={"decline_reason": pl.String},
    ).lazy()


def _staging(rows: list[tuple[str, str, str, str | None, int]]) -> pl.DataFrame:
    """Run deduplication and latest status selection on a delivery list."""
    return latest_per_transaction(deduplicate_events(_deliveries(rows))).collect()


def test_deduplicate_keeps_one_row_per_event() -> None:
    """12 deliveries hold 10 distinct events; the 2 repeats are dropped."""
    events = deduplicate_events(_deliveries(DELIVERIES)).collect()

    assert events.height == 10
    assert events.get_column("event_id").n_unique() == 10
    # The first arrival of each event is kept, in arrival order.
    assert events.get_column("arrival_index").to_list() == [0, 1, 3, 4, 5, 6, 7, 8, 10, 11]


def test_latest_event_wins_regardless_of_arrival_order() -> None:
    """Each transaction keeps the status of its latest event by event time."""
    staging = _staging(DELIVERIES)

    assert staging.get_column("transaction_id").to_list() == [
        "txn_1",
        "txn_2",
        "txn_3",
        "txn_4",
        "txn_5",
    ]
    assert staging.get_column("final_status").to_list() == [
        "approved",
        "declined",
        "pending",
        "refunded",
        "failed",
    ]
    assert staging.get_column("decline_reason").to_list() == [
        None,
        "insufficient_funds",
        None,
        None,
        "network_timeout",
    ]
    assert staging.get_column("n_events").to_list() == [2, 2, 1, 3, 2]
    assert staging.get_column("updated_at").to_list() == [
        _at(1),
        _at(6),
        _at(10),
        _at(30),
        _at(40),
    ]


def test_out_of_order_events_are_counted() -> None:
    """Three events arrive after a later event of their transaction."""
    events = deduplicate_events(_deliveries(DELIVERIES))
    # txn_2 pending (1), txn_4 approved and pending (2). The txn_5 tie is not out of order.
    assert count_out_of_order(events) == 3


def test_ingestion_is_idempotent_under_replay_and_reordering() -> None:
    """Replaying every delivery twice, in reverse order, yields the same staging rows."""
    replayed = list(reversed(DELIVERIES + DELIVERIES))
    assert _staging(replayed).equals(_staging(DELIVERIES))


def test_amount_usd_uses_the_currency_exponent_and_fixed_rate() -> None:
    """USD amounts match hand computed values, including zero decimal CLP."""
    frame = pl.DataFrame(
        {
            "country": ["MX", "CO", "CL"],
            "currency": ["MXN", "COP", "CLP"],
            "amount_minor": [12_345, 5_000_000, 10_000],
            "created_at": [_at(0)] * 3,
        }
    ).lazy()

    amounts = add_derived_columns(frame).collect().get_column("amount_usd").to_list()

    # 123.45 MXN * 0.054; 50,000.00 COP * 0.00025; 10,000 CLP * 0.00105.
    assert amounts == pytest.approx([6.6663, 12.5, 10.5])


def test_local_time_follows_each_country_time_zone() -> None:
    """Local hour and weekday are derived per country, including Chile's daylight saving."""
    frame = pl.DataFrame(
        {
            "country": ["MX", "CO", "CL", "CL"],
            "currency": ["MXN", "COP", "CLP", "CLP"],
            "amount_minor": [100, 100, 100, 100],
            "created_at": [
                # Tuesday 03:30 UTC is Monday 21:30 in Mexico City (UTC-6).
                datetime(2026, 9, 1, 3, 30, tzinfo=UTC),
                # Tuesday 03:30 UTC is Monday 22:30 in Bogota (UTC-5).
                datetime(2026, 9, 1, 3, 30, tzinfo=UTC),
                # Saturday 12:00 UTC is 08:00 in Santiago before the change (UTC-4).
                datetime(2026, 9, 5, 12, 0, tzinfo=UTC),
                # Monday 12:00 UTC is 09:00 in Santiago after the change (UTC-3).
                datetime(2026, 9, 7, 12, 0, tzinfo=UTC),
            ],
        }
    ).lazy()

    result = add_derived_columns(frame).collect()

    assert result.get_column("created_at_local").to_list() == [
        datetime(2026, 8, 31, 21, 30),
        datetime(2026, 8, 31, 22, 30),
        datetime(2026, 9, 5, 8, 0),
        datetime(2026, 9, 7, 9, 0),
    ]
    assert result.get_column("local_hour").to_list() == [21, 22, 8, 9]
    assert result.get_column("local_weekday").to_list() == [1, 1, 6, 1]
