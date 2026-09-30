"""Tests for the data quality assertions.

Purpose: confirm every check passes on valid data and raises ``DataQualityError`` on a violation.
Inputs: tiny in memory frames.
Outputs: pytest assertions.
"""

import polars as pl
import pytest

from pipeline.quality import (
    DataQualityError,
    check_enum_values,
    check_positive_amounts,
    check_row_count_reconciliation,
    check_status_mix,
    check_unique,
)


def test_check_unique_raises_on_duplicate_transaction_id() -> None:
    """One repeated key out of three rows is reported."""
    check_unique(pl.DataFrame({"transaction_id": ["a", "b", "c"]}), "transaction_id")
    with pytest.raises(DataQualityError, match="transaction_id is not unique: 1 duplicated"):
        check_unique(pl.DataFrame({"transaction_id": ["a", "b", "a"]}), "transaction_id")


def test_check_enum_values_raises_on_unknown_value() -> None:
    """A value outside the vocabulary is reported with an example."""
    allowed = {"country": ("MX", "CO", "CL")}
    check_enum_values(pl.DataFrame({"country": ["MX", "CL"]}), allowed)
    with pytest.raises(DataQualityError, match=r"country has 1 invalid values.*BR"):
        check_enum_values(pl.DataFrame({"country": ["MX", "BR"]}), allowed)


def test_check_enum_values_accepts_null_only_in_nullable_columns() -> None:
    """Null is valid for a nullable column and invalid otherwise."""
    frame = pl.DataFrame({"card_brand": ["visa", None]})
    allowed = {"card_brand": ("visa", "mastercard")}
    check_enum_values(frame, allowed, nullable=("card_brand",))
    with pytest.raises(DataQualityError, match="card_brand has 1 invalid values"):
        check_enum_values(frame, allowed)


@pytest.mark.parametrize("bad_amount", [-1, 0, None])
def test_check_positive_amounts_raises(bad_amount: int | None) -> None:
    """Negative, zero and null amounts are all rejected."""
    check_positive_amounts(pl.DataFrame({"amount_minor": [1, 250]}), "amount_minor")
    frame = pl.DataFrame({"amount_minor": [100, bad_amount]}, schema={"amount_minor": pl.Int64})
    with pytest.raises(DataQualityError, match="amount_minor has 1 null, zero or negative"):
        check_positive_amounts(frame, "amount_minor")


def test_check_status_mix_raises_outside_expected_range() -> None:
    """A 50% approved share fails a 60% to 90% range; 75% passes."""
    expected = {"approved": (0.60, 0.90)}
    check_status_mix(pl.DataFrame({"status": ["approved"] * 3 + ["declined"]}), "status", expected)
    with pytest.raises(DataQualityError, match=r"share of approved is 50\.00%"):
        check_status_mix(pl.DataFrame({"status": ["approved", "declined"] * 2}), "status", expected)


def test_check_status_mix_raises_on_missing_status_and_empty_frame() -> None:
    """A status that never occurs has a 0% share; an empty frame cannot be checked."""
    with pytest.raises(DataQualityError, match=r"share of declined is 0\.00%"):
        check_status_mix(
            pl.DataFrame({"status": ["approved"]}), "status", {"declined": (0.05, 0.30)}
        )
    with pytest.raises(DataQualityError, match="empty frame"):
        check_status_mix(pl.DataFrame({"status": []}, schema={"status": pl.String}), "status")


def test_check_row_count_reconciliation() -> None:
    """12 deliveries minus 2 duplicates are 10 events in 5 transactions."""
    counts = {
        "raw_rows": 12,
        "duplicate_deliveries": 2,
        "unique_events": 10,
        "events_in_staging": 10,
        "raw_transactions": 5,
        "staging_rows": 5,
    }
    check_row_count_reconciliation(**counts)
    with pytest.raises(DataQualityError, match="event counts do not reconcile"):
        check_row_count_reconciliation(**{**counts, "events_in_staging": 9})
    with pytest.raises(DataQualityError, match="transaction counts do not reconcile"):
        check_row_count_reconciliation(**{**counts, "staging_rows": 4})
