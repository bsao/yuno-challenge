"""Tests for the PSP cost analysis.

Purpose: pin the cost per successful transaction and the traffic shift simulation to hand
    computed values.
Inputs: a tiny in memory aggregate with two PSPs in one segment, plus a fee schedule.
Outputs: pytest assertions.

Fixture (one segment, Mexico cards, one day each):
    PSP_X: 100 attempts, 80 approved, 4,000 USD, fees 3% + 0.10 -> 120 + 10 = 130 USD, 1.625 each
    PSP_Y: 100 attempts, 90 approved, 4,500 USD, fees 2% + 0.05 -> 90 + 5 = 95 USD, 1.0556 each
"""

from datetime import date
from typing import Any

import polars as pl
import pytest

from analytics.cost import cost_per_successful_transaction, like_for_like, simulate_traffic_shift


def _row(psp: str, day: int, approved: int, declined: int, usd: float) -> dict[str, Any]:
    """Build one ``agg_daily`` row for Mexico cards."""
    return {
        "date": date(2026, 9, day),
        "country": "MX",
        "psp": psp,
        "payment_method": "card",
        "n_approved": approved,
        "n_refunded": 0,
        "n_declined": declined,
        "n_failed": 0,
        "n_expired": 0,
        "approved_amount_usd": usd,
        "refunded_amount_usd": 0.0,
    }


AGG = pl.DataFrame([_row("PSP_X", 1, 80, 20, 4000.0), _row("PSP_Y", 1, 90, 10, 4500.0)]).lazy()
FEES = pl.DataFrame(
    {
        "psp": ["PSP_X", "PSP_Y"],
        "country": ["MX", "MX"],
        "payment_method": ["card", "card"],
        "pct_fee": [3.0, 2.0],
        "fixed_fee_usd": [0.10, 0.05],
    }
).lazy()


def test_cost_per_success_charges_the_fixed_fee_on_every_attempt() -> None:
    """PSP_X costs 130 / 80 = 1.625 USD per success; PSP_Y costs 95 / 90 = 1.0556 USD."""
    costs = cost_per_successful_transaction(AGG, FEES).collect()
    x, y = costs.row(0, named=True), costs.row(1, named=True)

    assert (x["psp"], y["psp"]) == ("PSP_X", "PSP_Y")
    assert x["total_fees_usd"] == pytest.approx(130.0)  # 0.03 * 4000 + 0.10 * 100
    assert x["cost_per_success_usd"] == pytest.approx(1.625)
    assert x["avg_ticket_usd"] == pytest.approx(50.0)
    assert x["fee_share_of_gmv"] == pytest.approx(130 / 4000)
    assert y["total_fees_usd"] == pytest.approx(95.0)  # 0.02 * 4500 + 0.05 * 100
    assert y["cost_per_success_usd"] == pytest.approx(95 / 90)


def test_cost_is_null_without_a_success() -> None:
    """A PSP with attempts but no approval has fees and no cost per success."""
    agg = pl.DataFrame([_row("PSP_X", 1, 0, 10, 0.0)]).lazy()

    row = cost_per_successful_transaction(agg, FEES).collect().row(0, named=True)

    assert row["total_fees_usd"] == pytest.approx(1.0)  # 10 attempts * 0.10
    assert row["cost_per_success_usd"] is None


def test_shift_moves_a_fifth_of_the_worst_psp_to_the_best() -> None:
    """One observed day scaled to 30: 600 attempts move from PSP_X to PSP_Y and save 210 USD."""
    result = simulate_traffic_shift(AGG, FEES).collect().row(0, named=True)

    assert (result["worst_psp"], result["best_psp"]) == ("PSP_X", "PSP_Y")
    assert result["days"] == 1
    assert result["moved_attempts_monthly"] == pytest.approx(600.0)  # 0.20 * 100 * 30 / 1
    # Before: 600 * 0.10 + 0.03 * (600 * 0.8) * 50 = 60 + 720. After: 600 * 0.05 + 0.02 * 540 * 50.
    assert result["fees_before_usd"] == pytest.approx(780.0)
    assert result["fees_after_usd"] == pytest.approx(570.0)
    assert result["monthly_savings_usd"] == pytest.approx(210.0)
    assert result["monthly_approved_delta"] == pytest.approx(60.0)  # 540 - 480
    assert result["monthly_gmv_delta_usd"] == pytest.approx(3000.0)  # 60 * 50


def test_cheapest_psp_can_lose_sales() -> None:
    """Ranking by cost picks the cheap, weak PSP: fees fall and approved GMV falls too."""
    agg = pl.DataFrame([_row("PSP_X", 1, 90, 10, 4500.0), _row("PSP_Y", 1, 60, 40, 3000.0)]).lazy()
    # PSP_X: 0.03 * 4500 + 10 = 145 -> 1.611 each. PSP_Y: 0.02 * 3000 + 5 = 65 -> 1.083 each.

    by_cost = simulate_traffic_shift(agg, FEES).collect().row(0, named=True)
    by_rate = simulate_traffic_shift(agg, FEES, rank_by="auth_rate").collect().row(0, named=True)

    assert (by_cost["worst_psp"], by_cost["best_psp"]) == ("PSP_X", "PSP_Y")
    assert by_cost["monthly_savings_usd"] > 0
    assert by_cost["monthly_gmv_delta_usd"] == pytest.approx(-9000.0)  # 600 * (0.6 - 0.9) * 50
    assert (by_rate["worst_psp"], by_rate["best_psp"]) == ("PSP_Y", "PSP_X")
    assert by_rate["monthly_gmv_delta_usd"] == pytest.approx(9000.0)
    assert by_rate["monthly_savings_usd"] < 0


def test_like_for_like_drops_days_before_every_psp_was_live() -> None:
    """PSP_Y starts on day 2, so day 1 of PSP_X is excluded from the comparison."""
    agg = pl.DataFrame(
        [
            _row("PSP_X", 1, 10, 90, 500.0),
            _row("PSP_X", 2, 80, 20, 4000.0),
            _row("PSP_Y", 2, 90, 10, 4500.0),
        ]
    ).lazy()

    kept = like_for_like(agg).collect()
    result = simulate_traffic_shift(agg, FEES).collect().row(0, named=True)

    assert kept.select("psp", "date").rows() == [
        ("PSP_X", date(2026, 9, 2)),
        ("PSP_Y", date(2026, 9, 2)),
    ]
    assert result["days"] == 1
    assert result["worst_auth_rate"] == pytest.approx(0.8)  # day 1 would have made it 0.45


def test_segments_with_one_psp_are_not_simulated_and_share_is_validated() -> None:
    """A single PSP has nowhere to move traffic; a share above 1 is rejected."""
    single = pl.DataFrame([_row("PSP_X", 1, 80, 20, 4000.0)]).lazy()
    assert simulate_traffic_shift(single, FEES).collect().height == 0

    with pytest.raises(ValueError, match="share must be between 0 and 1"):
        simulate_traffic_shift(AGG, FEES, share=1.5)
