"""Tests for the staging to marts transformation.

Purpose: check the grain and the additive measures of both marts against hand computed values.
Inputs: a tiny in memory staging frame of six transactions and a two merchant dimension.
Outputs: pytest assertions.
"""

from datetime import date, datetime

import polars as pl
import pytest

from pipeline.transform import build_agg_daily, build_fct_transactions

# (transaction_id, merchant_id, psp, payment_method, final_status, amount_usd, created_at_local)
ROWS = [
    ("txn_1", "mrc_001", "PSP_A", "card", "approved", 10.0, datetime(2026, 9, 1, 9, 0)),
    ("txn_2", "mrc_001", "PSP_A", "card", "declined", 20.0, datetime(2026, 9, 1, 23, 59)),
    ("txn_3", "mrc_001", "PSP_A", "card", "refunded", 5.0, datetime(2026, 9, 1, 12, 0)),
    ("txn_4", "mrc_002", "PSP_A", "card", "approved", 7.5, datetime(2026, 9, 1, 13, 0)),
    ("txn_5", "mrc_002", "PSP_B", "card", "failed", 3.0, datetime(2026, 9, 1, 14, 0)),
    ("txn_6", "mrc_002", "PSP_A", "card", "pending", 8.0, datetime(2026, 9, 2, 0, 0)),
]


def _staging() -> pl.LazyFrame:
    """Build the staging fixture (all transactions in Mexico)."""
    return pl.DataFrame(
        {
            "transaction_id": [row[0] for row in ROWS],
            "merchant_id": [row[1] for row in ROWS],
            "country": ["MX"] * len(ROWS),
            "psp": [row[2] for row in ROWS],
            "payment_method": [row[3] for row in ROWS],
            "final_status": [row[4] for row in ROWS],
            "amount_usd": [row[5] for row in ROWS],
            "created_at_local": [row[6] for row in ROWS],
        }
    ).lazy()


def _merchants() -> pl.LazyFrame:
    """Build the merchant dimension fixture."""
    return pl.DataFrame(
        {
            "merchant_id": ["mrc_001", "mrc_002"],
            "country": ["MX", "MX"],
            "category": ["fashion", "travel"],
            "size_tier": ["enterprise", "small"],
        }
    ).lazy()


def test_fct_keeps_one_row_per_transaction_and_adds_attributes() -> None:
    """The fact table keeps the staging grain and gains the local date and merchant columns."""
    fct = build_fct_transactions(_staging(), _merchants()).collect()

    assert fct.get_column("transaction_id").to_list() == [row[0] for row in ROWS]
    assert fct.get_column("local_date").to_list() == [date(2026, 9, 1)] * 5 + [date(2026, 9, 2)]
    assert fct.get_column("merchant_category").to_list() == ["fashion"] * 3 + ["travel"] * 3
    assert fct.get_column("merchant_size_tier").to_list() == ["enterprise"] * 3 + ["small"] * 3


def test_fct_rejects_a_duplicated_merchant() -> None:
    """A merchant dimension with a repeated key fails instead of fanning out transactions."""
    merchants = pl.concat([_merchants(), _merchants()])
    with pytest.raises(pl.exceptions.ComputeError):
        build_fct_transactions(_staging(), merchants).collect()


def test_agg_daily_has_the_documented_grain_and_additive_measures() -> None:
    """Six transactions fall into three date x country x psp x method groups."""
    agg = build_agg_daily(build_fct_transactions(_staging(), _merchants())).collect()

    assert agg.select("date", "country", "psp", "payment_method").rows() == [
        (date(2026, 9, 1), "MX", "PSP_A", "card"),
        (date(2026, 9, 1), "MX", "PSP_B", "card"),
        (date(2026, 9, 2), "MX", "PSP_A", "card"),
    ]
    first = agg.row(0, named=True)
    assert first["n_transactions"] == 4  # txn_1 to txn_4
    assert (first["n_approved"], first["n_declined"], first["n_refunded"]) == (2, 1, 1)
    assert (first["n_failed"], first["n_expired"], first["n_pending"]) == (0, 0, 0)
    assert first["approved_amount_usd"] == pytest.approx(17.5)  # 10.0 + 7.5
    assert first["refunded_amount_usd"] == pytest.approx(5.0)
    assert agg.row(1, named=True)["n_failed"] == 1
    assert agg.row(2, named=True)["n_pending"] == 1
    assert agg.get_column("n_transactions").sum() == len(ROWS)
