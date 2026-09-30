"""Tests for the merchant health score.

Purpose: pin the component scores, the weighted score, the label and the main driver to hand
    computed values.
Inputs: tiny in memory frames.
Outputs: pytest assertions.

Weights: authorization 0.40, failure 0.20, refund 0.15, volume 0.25.
"""

from datetime import date, timedelta
from typing import Any

import polars as pl
import pytest

from analytics.health import COMPONENTS, merchant_health, score_components


def _score(**inputs: float | None) -> dict[str, Any]:
    """Score one merchant from its four component inputs."""
    frame = pl.DataFrame(
        [inputs],
        schema={
            "auth_gap": pl.Float64,
            "failure_rate": pl.Float64,
            "refund_rate": pl.Float64,
            "volume_trend": pl.Float64,
        },
    )
    return score_components(frame.lazy()).collect().row(0, named=True)


def test_weights_sum_to_one() -> None:
    """The documented weights add up to 1, so the score stays within 0 to 100."""
    assert sum(weight for weight, _, _, _ in COMPONENTS.values()) == pytest.approx(1.0)


def test_every_input_at_its_bad_anchor_scores_zero() -> None:
    """All four components at their bad anchors give 0, at risk, driven by authorization."""
    row = _score(auth_gap=-0.10, failure_rate=0.10, refund_rate=0.10, volume_trend=-0.50)

    assert row["health_score"] == pytest.approx(0.0)
    assert row["label"] == "at_risk"
    assert row["main_driver"] == "authorization"  # loses 40 points, the largest weight


def test_every_input_at_its_good_anchor_scores_one_hundred() -> None:
    """All four components at their good anchors give 100; values beyond them are clipped."""
    row = _score(auth_gap=0.05, failure_rate=0.0, refund_rate=0.0, volume_trend=0.25)
    assert row["health_score"] == pytest.approx(100.0)
    assert row["label"] == "healthy"

    beyond = _score(auth_gap=0.30, failure_rate=0.0, refund_rate=0.0, volume_trend=2.0)
    assert beyond["health_score"] == pytest.approx(100.0)


def test_typical_merchant_matches_the_hand_computed_score() -> None:
    """Gap 0, 3% failures, 2% refunds and flat volume give 69.33."""
    row = _score(auth_gap=0.0, failure_rate=0.03, refund_rate=0.02, volume_trend=0.0)

    assert row["score_authorization"] == pytest.approx(66.6667, abs=1e-3)  # 0.10 / 0.15
    assert row["score_failure"] == pytest.approx(70.0)  # 1 - 0.03 / 0.10
    assert row["score_refund"] == pytest.approx(80.0)  # 1 - 0.02 / 0.10
    assert row["score_volume"] == pytest.approx(66.6667, abs=1e-3)  # 0.50 / 0.75
    # 0.40 * 66.667 + 0.20 * 70 + 0.15 * 80 + 0.25 * 66.667 = 26.667 + 14 + 12 + 16.667
    assert row["health_score"] == pytest.approx(69.3333, abs=1e-3)
    assert row["label"] == "healthy"
    # Points lost: authorization 13.33, failure 6.0, refund 3.0, volume 8.33.
    assert row["main_driver"] == "authorization"


def test_main_driver_is_the_component_that_costs_the_most_points() -> None:
    """A halved volume costs 25 points, more than the 13.3 lost on authorization."""
    row = _score(auth_gap=0.0, failure_rate=0.0, refund_rate=0.0, volume_trend=-0.50)

    # 0.40 * 66.667 + 0.20 * 100 + 0.15 * 100 + 0.25 * 0 = 61.667
    assert row["health_score"] == pytest.approx(61.6667, abs=1e-3)
    assert row["main_driver"] == "volume"


def test_below_fifty_is_at_risk() -> None:
    """Gap -10 points and 10% failures with perfect refunds and volume give 40."""
    row = _score(auth_gap=-0.10, failure_rate=0.10, refund_rate=0.0, volume_trend=0.25)

    # 0.40 * 0 + 0.20 * 0 + 0.15 * 100 + 0.25 * 100 = 40
    assert row["health_score"] == pytest.approx(40.0)
    assert row["label"] == "at_risk"


def test_undefined_inputs_score_a_neutral_fifty() -> None:
    """A merchant with no history on any component scores 50 on each, so 50 overall."""
    row = _score(auth_gap=None, failure_rate=None, refund_rate=None, volume_trend=None)

    assert row["health_score"] == pytest.approx(50.0)
    assert row["label"] == "healthy"  # at risk is strictly below 50


def test_merchant_health_derives_the_inputs_from_transactions() -> None:
    """Rates and the 30 day volume trend are derived per merchant and ranked worst first."""
    last_day = date(2026, 9, 30)

    def rows(merchant: str, status: str, days_ago: int, n: int) -> list[dict[str, Any]]:
        return [
            {
                "merchant_id": merchant,
                "country": "MX",
                "merchant_category": "fashion",
                "payment_method": "card",
                "final_status": status,
                "amount_usd": 10.0,
                "local_date": last_day - timedelta(days=days_ago),
            }
        ] * n

    # mrc_a: 40 recent (30 approved, 10 declined) and 80 prior approved: volume halves.
    # mrc_b: 60 recent and 60 prior, 54 approved + 6 refunded in each period: flat volume.
    fct = pl.DataFrame(
        rows("mrc_a", "approved", 5, 30)
        + rows("mrc_a", "declined", 5, 10)
        + rows("mrc_a", "approved", 45, 80)
        + rows("mrc_b", "approved", 0, 54)
        + rows("mrc_b", "refunded", 29, 6)
        + rows("mrc_b", "approved", 30, 54)
        + rows("mrc_b", "refunded", 59, 6)
    ).lazy()

    health = merchant_health(fct).collect()
    by_merchant = {row["merchant_id"]: row for row in health.iter_rows(named=True)}
    a, b = by_merchant["mrc_a"], by_merchant["mrc_b"]

    assert health.get_column("merchant_id").to_list() == ["mrc_a", "mrc_b"]  # worst first
    assert (a["transactions_last_30d"], a["transactions_prior_30d"]) == (40, 80)
    assert a["volume_trend"] == pytest.approx(-0.5)
    assert a["auth_rate"] == pytest.approx(110 / 120)
    assert a["peer_rate"] == pytest.approx(1.0)  # mrc_b approves everything
    assert a["auth_gap"] == pytest.approx(110 / 120 - 1.0)
    assert a["refund_rate"] == pytest.approx(0.0)
    assert (b["transactions_last_30d"], b["transactions_prior_30d"]) == (60, 60)
    assert b["volume_trend"] == pytest.approx(0.0)
    assert b["refund_rate"] == pytest.approx(12 / 120)
    assert b["score_refund"] == pytest.approx(0.0)
    # mrc_a: authorization (-0.0833 + 0.10) / 0.15 = 11.11, volume 0, failure and refund 100:
    # 0.40 * 11.11 + 0.20 * 100 + 0.15 * 100 + 0.25 * 0 = 39.44. Authorization costs 35.6 points.
    assert a["health_score"] == pytest.approx(39.4444, abs=1e-3)
    assert a["label"] == "at_risk"
    assert a["main_driver"] == "authorization"
    # mrc_b: 0.40 * 100 + 0.20 * 100 + 0.15 * 0 + 0.25 * 66.667 = 76.67.
    assert b["health_score"] == pytest.approx(76.6667, abs=1e-3)
    assert b["main_driver"] == "refund"
