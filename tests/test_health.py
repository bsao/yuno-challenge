"""Tests for the merchant health score.

Purpose: pin the component scores, the weighted score, the label, the main driver and the
    diagnosis to hand computed values.
Inputs: tiny in memory frames.
Outputs: pytest assertions.

Weights: authorization 0.35, abandonment 0.20, volume 0.20, failure 0.15, refund 0.10.
Anchors (bad -> good): authorization gap -0.15 -> +0.05; abandonment gap +0.15 -> -0.03;
volume trend -0.50 -> +0.10; failure rate 0.10 -> 0.01; refund rate 0.08 -> 0.
"""

from datetime import date, timedelta
from typing import Any

import polars as pl
import pytest

from analytics.health import COMPONENTS, DIAGNOSIS, merchant_health, score_components

GOOD = {
    "auth_gap": 0.05,
    "abandonment_gap": -0.03,
    "volume_trend": 0.10,
    "failure_rate": 0.01,
    "refund_rate": 0.0,
}
BAD = {
    "auth_gap": -0.15,
    "abandonment_gap": 0.15,
    "volume_trend": -0.50,
    "failure_rate": 0.10,
    "refund_rate": 0.08,
}


def _score(attempts: int = 1000, **inputs: float | None) -> dict[str, Any]:
    """Score one merchant from its component inputs."""
    frame = pl.DataFrame(
        [{"attempts": attempts, **inputs}],
        schema={"attempts": pl.Int64, **dict.fromkeys(GOOD, pl.Float64)},
    )
    return score_components(frame.lazy()).collect().row(0, named=True)


def test_weights_sum_to_one_and_every_component_has_a_diagnosis() -> None:
    """The weights add up to 1, so the score stays within 0 to 100."""
    assert sum(weight for weight, _, _, _ in COMPONENTS.values()) == pytest.approx(1.0)
    assert set(DIAGNOSIS) == set(COMPONENTS)


def test_every_input_at_its_bad_anchor_scores_zero() -> None:
    """All components at their bad anchors give 0, at risk, driven by authorization."""
    row = _score(**BAD)

    assert row["health_score"] == pytest.approx(0.0)
    assert row["label"] == "at_risk"
    assert row["main_driver"] == "authorization"  # loses 35 points, the largest weight
    assert row["diagnosis"] == "processing"


def test_every_input_at_its_good_anchor_scores_one_hundred() -> None:
    """All components at their good anchors give 100; values beyond them are clipped."""
    assert _score(**GOOD)["health_score"] == pytest.approx(100.0)
    beyond = _score(**{**GOOD, "auth_gap": 0.30, "volume_trend": 2.0})
    assert beyond["health_score"] == pytest.approx(100.0)
    assert beyond["label"] == "healthy"


def test_typical_merchant_matches_the_hand_computed_score() -> None:
    """At its peers, flat volume, 3% failures and 2% refunds give 78.75."""
    row = _score(
        auth_gap=0.0, abandonment_gap=0.0, volume_trend=0.0, failure_rate=0.03, refund_rate=0.02
    )

    assert row["score_authorization"] == pytest.approx(75.0)  # 0.15 / 0.20
    assert row["score_abandonment"] == pytest.approx(83.3333, abs=1e-3)  # 0.15 / 0.18
    assert row["score_volume"] == pytest.approx(83.3333, abs=1e-3)  # 0.50 / 0.60
    assert row["score_failure"] == pytest.approx(77.7778, abs=1e-3)  # 0.07 / 0.09
    assert row["score_refund"] == pytest.approx(75.0)  # 0.06 / 0.08
    # 0.35 * 75 + 0.20 * 83.333 + 0.20 * 83.333 + 0.15 * 77.778 + 0.10 * 75
    assert row["health_score"] == pytest.approx(78.75, abs=1e-3)
    assert row["label"] == "healthy"
    # Points lost: authorization 8.75, abandonment 3.33, volume 3.33, failure 3.33, refund 2.5.
    assert row["main_driver"] == "authorization"


def test_volume_loss_is_diagnosed_as_ux() -> None:
    """A halved volume with everything else good scores 80, driven by volume."""
    row = _score(**{**GOOD, "volume_trend": -0.50})

    assert row["health_score"] == pytest.approx(80.0)  # 100 - 0.20 * 100
    assert row["main_driver"] == "volume"
    assert row["diagnosis"] == "ux"


def test_abandonment_above_peers_is_diagnosed_as_ux() -> None:
    """Abandonment 15 points above peers costs 20 points and points to the checkout."""
    row = _score(**{**GOOD, "abandonment_gap": 0.15})

    assert row["health_score"] == pytest.approx(80.0)
    assert row["main_driver"] == "abandonment"
    assert row["diagnosis"] == "ux"


def test_below_fifty_is_at_risk_and_processing_when_authorization_drives() -> None:
    """Authorization, failure and refund at their bad anchors leave 40 points."""
    row = _score(**{**GOOD, "auth_gap": -0.15, "failure_rate": 0.10, "refund_rate": 0.08})

    assert row["health_score"] == pytest.approx(40.0)  # 100 - 35 - 15 - 10
    assert row["label"] == "at_risk"
    assert row["diagnosis"] == "processing"


def test_exactly_fifty_is_healthy() -> None:
    """At risk is strictly below 50: authorization and failure at bad anchors give exactly 50."""
    row = _score(**{**GOOD, "auth_gap": -0.15, "failure_rate": 0.10})

    assert row["health_score"] == pytest.approx(50.0)
    assert row["label"] == "healthy"


def test_few_attempts_are_labelled_insufficient_data() -> None:
    """A merchant with 49 attempts is not labelled at risk, whatever its score."""
    assert _score(attempts=49, **BAD)["label"] == "insufficient_data"
    assert _score(attempts=50, **BAD)["label"] == "at_risk"


def test_undefined_inputs_score_a_neutral_fifty() -> None:
    """Null inputs score 50 on each component, so 50 overall."""
    row = _score(**dict.fromkeys(GOOD, None))

    assert row["health_score"] == pytest.approx(50.0)
    assert row["label"] == "healthy"


def test_merchant_health_uses_the_last_30_days_and_ranks_worst_first() -> None:
    """Rates cover the last 30 days only; the volume trend compares them with the 30 before."""
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

    # mrc_a: 80 recent (60 approved, 20 declined) and 160 prior approved: volume halves.
    # mrc_b: 60 recent (54 approved, 6 refunded) and 60 prior, of which 30 declined.
    fct = pl.DataFrame(
        rows("mrc_a", "approved", 5, 60)
        + rows("mrc_a", "declined", 5, 20)
        + rows("mrc_a", "approved", 45, 160)
        + rows("mrc_b", "approved", 0, 54)
        + rows("mrc_b", "refunded", 29, 6)
        + rows("mrc_b", "approved", 30, 30)
        + rows("mrc_b", "declined", 59, 30)
    ).lazy()

    health = merchant_health(fct).collect()
    by_merchant = {row["merchant_id"]: row for row in health.iter_rows(named=True)}
    a, b = by_merchant["mrc_a"], by_merchant["mrc_b"]

    assert health.get_column("merchant_id").to_list() == ["mrc_a", "mrc_b"]  # worst first
    assert (a["transactions_last_30d"], a["transactions_prior_30d"]) == (80, 160)
    assert a["volume_trend"] == pytest.approx(-0.5)
    assert a["attempts"] == 80  # the 160 prior transactions are outside the rate window
    assert a["auth_rate"] == pytest.approx(0.75)
    assert a["auth_gap"] == pytest.approx(-0.25)  # its only peer approves 60 of 60
    # authorization 0, volume 0, abandonment 83.33, failure 100, refund 100:
    # 0.20 * 83.333 + 0.15 * 100 + 0.10 * 100 = 41.67. Authorization costs 35 points.
    assert a["health_score"] == pytest.approx(41.6667, abs=1e-3)
    assert (a["label"], a["main_driver"], a["diagnosis"]) == (
        "at_risk",
        "authorization",
        "processing",
    )

    assert b["volume_trend"] == pytest.approx(0.0)
    assert b["auth_rate"] == pytest.approx(1.0)  # the 30 prior declines are not counted
    assert b["refund_rate"] == pytest.approx(0.10)
    # authorization 100, abandonment 83.33, volume 83.33, failure 100, refund 0:
    # 35 + 16.667 + 16.667 + 15 + 0 = 83.33. The refund component costs 10 points.
    assert b["health_score"] == pytest.approx(83.3333, abs=1e-3)
    assert (b["label"], b["main_driver"], b["diagnosis"]) == ("healthy", "refund", "refunds")
