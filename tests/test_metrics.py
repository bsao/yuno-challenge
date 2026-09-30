"""Tests for the metric definitions.

Purpose: pin every formula in ``analytics.metrics`` to hand computed expected values.
Inputs: tiny in memory frames of additive measures.
Outputs: pytest assertions.

Hand computation of the Wilson interval (z = 1.96, z**2 = 3.8416):
    8 of 10:   denominator = 1.38416; center = (0.8 + 0.19208) / 1.38416 = 0.716738;
               half = 1.96 * sqrt(0.016 + 0.009604) / 1.38416 = 0.226578 -> (0.490160, 0.943316)
    0 of 10:   center = half = 0.19208 / 1.38416 = 0.138770 -> (0, 0.277540)
    10 of 10:  mirror of 0 of 10 -> (0.722460, 1)
    50 of 100: denominator = 1.038416; center = 0.5;
               half = 1.96 * sqrt(0.0025 + 0.00009604) / 1.038416 = 0.096170 -> (0.403830, 0.596170)
    75 of 100: center = (0.75 + 0.019208) / 1.038416 = 0.740752;
               half = 1.96 * sqrt(0.001875 + 0.00009604) / 1.038416 = 0.083798 -> (0.65695, 0.82455)
"""

import polars as pl
import pytest

from analytics.metrics import (
    outcome_rates,
    performance,
    wilson_interval,
    wilson_interval_expr,
)

TOLERANCE = 1e-5
WILSON_CASES = [
    (8, 10, 0.490160, 0.943316),
    (0, 10, 0.0, 0.277540),
    (10, 10, 0.722460, 1.0),
    (50, 100, 0.403830, 0.596170),
    (75, 100, 0.656954, 0.824550),
]


def _measures(rows: list[dict[str, object]]) -> pl.LazyFrame:
    """Build an additive measures frame, filling unspecified measures with zero."""
    defaults: dict[str, object] = {
        "n_approved": 0,
        "n_refunded": 0,
        "n_declined": 0,
        "n_failed": 0,
        "n_expired": 0,
        "n_pending": 0,
        "approved_amount_usd": 0.0,
        "refunded_amount_usd": 0.0,
    }
    return pl.DataFrame([{**defaults, **row} for row in rows]).lazy()


@pytest.mark.parametrize(("successes", "n", "low", "high"), WILSON_CASES)
def test_wilson_interval_matches_hand_computed_values(
    successes: int, n: int, low: float, high: float
) -> None:
    """The scalar Wilson interval reproduces the hand computed bounds."""
    assert wilson_interval(successes, n) == pytest.approx((low, high), abs=TOLERANCE)


def test_wilson_interval_expr_matches_hand_computed_values() -> None:
    """The Polars Wilson expressions reproduce the same hand computed bounds."""
    frame = pl.DataFrame(
        {
            "successes": [case[0] for case in WILSON_CASES],
            "n": [case[1] for case in WILSON_CASES],
        }
    )
    low, high = wilson_interval_expr(pl.col("successes"), pl.col("n"))

    result = frame.select(low.alias("low"), high.alias("high"))

    assert result.get_column("low").to_list() == pytest.approx(
        [case[2] for case in WILSON_CASES], abs=TOLERANCE
    )
    assert result.get_column("high").to_list() == pytest.approx(
        [case[3] for case in WILSON_CASES], abs=TOLERANCE
    )


def test_wilson_interval_is_null_without_trials_and_rejects_invalid_input() -> None:
    """Zero trials give null bounds in Polars and an error in the scalar function."""
    low, high = wilson_interval_expr(pl.col("successes"), pl.col("n"))
    result = pl.DataFrame({"successes": [0], "n": [0]}).select(
        low.alias("low"), high.alias("high")
    )
    assert result.row(0) == (None, None)

    with pytest.raises(ValueError, match="invalid proportion"):
        wilson_interval(0, 0)
    with pytest.raises(ValueError, match="invalid proportion"):
        wilson_interval(11, 10)


def test_auth_rate_counts_refunded_as_approved_and_excludes_pending_and_expired() -> (
    None
):
    """70 approved + 5 refunded over 100 attempts is 75%; 7 pending and 9 expired are ignored."""
    lf = _measures(
        [
            {
                "payment_method": "card",
                "n_approved": 70,
                "n_refunded": 5,
                "n_declined": 20,
                "n_failed": 5,
                "n_pending": 7,
                "n_expired": 9,
            }
        ]
    )

    row = performance(lf, ["payment_method"]).collect().row(0, named=True)

    assert row["attempts"] == 100  # 70 + 5 + 20 + 5
    assert row["approved"] == 75  # 70 + 5
    assert row["auth_rate"] == pytest.approx(0.75)
    assert row["wilson_low"] == pytest.approx(0.656954, abs=TOLERANCE)
    assert row["wilson_high"] == pytest.approx(0.824550, abs=TOLERANCE)


def test_auth_rate_is_recomputed_from_counts_when_rolling_up() -> None:
    """Rolling up sums the counts: (8 + 42) / (10 + 90) = 50%, not the 63.3% mean of rates."""
    lf = _measures(
        [
            {
                "country": "MX",
                "payment_method": "card",
                "n_approved": 8,
                "n_declined": 2,
            },
            {
                "country": "MX",
                "payment_method": "spei",
                "n_approved": 42,
                "n_declined": 48,
            },
            {"country": "CL", "payment_method": "card", "n_approved": 10},
        ]
    )

    result = performance(lf, ["country"]).collect()

    assert result.get_column("country").to_list() == ["CL", "MX"]
    assert result.get_column("attempts").to_list() == [10, 100]
    assert result.get_column("auth_rate").to_list() == pytest.approx([1.0, 0.5])
    mx = result.row(1, named=True)
    assert (mx["wilson_low"], mx["wilson_high"]) == pytest.approx(
        (0.403830, 0.596170), abs=TOLERANCE
    )


def test_auth_rate_is_null_without_attempts() -> None:
    """A segment holding only pending transactions has no rate and no interval."""
    lf = _measures([{"payment_method": "card", "n_pending": 3}])

    row = performance(lf, ["payment_method"]).collect().row(0, named=True)

    assert row["attempts"] == 0
    assert row["auth_rate"] is None
    assert row["wilson_low"] is None
    assert row["wilson_high"] is None


def test_gmv_is_gross_and_net_gmv_subtracts_refunds() -> None:
    """Gross GMV is 1,000 + 50 = 1,050 USD; net GMV is 1,050 - 50 = 1,000 USD."""
    lf = _measures(
        [
            {
                "payment_method": "card",
                "approved_amount_usd": 600.0,
                "refunded_amount_usd": 50.0,
            },
            {"payment_method": "spei", "approved_amount_usd": 400.0},
        ]
    )

    row = performance(lf, []).collect().row(0, named=True)

    assert row["gmv_usd"] == pytest.approx(1050.0)
    assert row["net_gmv_usd"] == pytest.approx(1000.0)


def test_completion_rate_applies_to_voucher_methods_only() -> None:
    """OXXO: (60 + 2) paid of 62 + 38 resolved is 62%; the card row has no completion rate."""
    lf = _measures(
        [
            {
                "payment_method": "oxxo",
                "n_approved": 60,
                "n_refunded": 2,
                "n_expired": 38,
                "n_failed": 1,
                "n_pending": 4,
            },
            {
                "payment_method": "card",
                "n_approved": 80,
                "n_declined": 20,
                "n_expired": 5,
            },
        ]
    )

    by_method = performance(lf, ["payment_method"]).collect()
    card, oxxo = by_method.row(0, named=True), by_method.row(1, named=True)

    assert oxxo["voucher_attempts"] == 100  # 62 paid + 38 expired
    assert oxxo["completion_rate"] == pytest.approx(0.62)
    assert oxxo["auth_rate"] == pytest.approx(
        62 / 63
    )  # expired is not an authorization attempt
    assert card["voucher_attempts"] == 0
    assert card["completion_rate"] is None
    assert card["completion_wilson_low"] is None

    # Rolled up across methods, completion still uses the voucher rows only.
    total = performance(lf, []).collect().row(0, named=True)
    assert total["voucher_attempts"] == 100
    assert total["completion_rate"] == pytest.approx(0.62)
    assert total["attempts"] == 163  # 63 oxxo + 100 card


def test_outcome_rates_split_declines_failures_and_voucher_expiration() -> None:
    """Of 100 card attempts 15 decline and 5 fail; of 40 resolved vouchers 10 expire."""
    lf = _measures(
        [
            {
                "payment_method": "card",
                "n_approved": 78,
                "n_refunded": 2,
                "n_declined": 15,
                "n_failed": 5,
                "n_expired": 9,
            },
            {
                "payment_method": "oxxo",
                "n_approved": 29,
                "n_refunded": 1,
                "n_expired": 10,
            },
        ]
    )

    by_method = outcome_rates(lf, ["payment_method"]).collect()
    card, oxxo = by_method.row(0, named=True), by_method.row(1, named=True)

    assert card["attempts"] == 100  # 78 + 2 + 15 + 5; the 9 expired are not attempts
    assert card["decline_rate"] == pytest.approx(0.15)
    assert card["failure_rate"] == pytest.approx(0.05)
    assert card["voucher_attempts"] == 0
    assert card["expiration_rate"] is None
    assert oxxo["voucher_attempts"] == 40  # 30 paid + 10 expired
    assert oxxo["expiration_rate"] == pytest.approx(0.25)
    assert oxxo["decline_rate"] == pytest.approx(0.0)

    total = outcome_rates(lf, []).collect().row(0, named=True)
    assert total["attempts"] == 130
    assert total["decline_rate"] == pytest.approx(15 / 130)
    assert total["expiration_rate"] == pytest.approx(0.25)  # voucher rows only
    # Wilson bounds of 10 of 40 by the scalar reference.
    assert (
        total["expiration_rate_wilson_low"],
        total["expiration_rate_wilson_high"],
    ) == (pytest.approx(wilson_interval(10, 40)))
