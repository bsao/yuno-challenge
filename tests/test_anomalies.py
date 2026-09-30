"""Tests for the failure analysis and the anomaly detectors.

Purpose: pin the detection rules to hand computed cases, and prove that every anomaly planted by
    the generator is detected on the real pipeline output.
Inputs: tiny in memory frames, plus one full scale pipeline run in a temporary directory (the
    planted night time anomaly only holds enough transactions at full scale).
Outputs: pytest assertions.
"""

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from analytics import anomalies
from analytics.health import merchant_health
from pipeline.run import run_pipeline

START = date(2026, 9, 1)


def _transactions(groups: list[dict[str, Any]]) -> pl.LazyFrame:
    """Build a transaction frame from groups, repeating each group ``n`` times."""
    defaults: dict[str, Any] = {
        "merchant_id": "mrc_001",
        "country": "MX",
        "merchant_category": "fashion",
        "payment_method": "card",
        "final_status": "approved",
        "decline_reason": None,
        "amount_usd": 20.0,
        "n": 1,
    }
    rows = [{**defaults, **group} for group in groups for _ in range(group.get("n", 1))]
    return pl.DataFrame(rows, schema_overrides={"decline_reason": pl.String}).drop("n").lazy()


def _agg_day(day: int, attempts: int, declined: int, psp: str = "PSP_A") -> dict[str, Any]:
    """Build one ``agg_daily`` row with the given attempts and declines."""
    return {
        "date": START + timedelta(days=day),
        "psp": psp,
        "country": "MX",
        "payment_method": "card",
        "n_approved": attempts - declined,
        "n_refunded": 0,
        "n_declined": declined,
        "n_failed": 0,
        "n_expired": 0,
    }


def test_daily_decline_rate_is_scored_against_the_trailing_14_days() -> None:
    """Baseline 20%: 40 of 100 gives z = 5.0 (flagged); 26 of 100 gives z = 1.5 (not flagged)."""
    rows = [_agg_day(day, 100, 20) for day in range(14)]
    rows.append(_agg_day(14, 100, 40))  # z = (0.40 - 0.20) / sqrt(0.2 * 0.8 / 100) = 5.0
    rows.append(_agg_day(15, 100, 26))  # baseline 300 / 1400; not an anomaly

    scored = anomalies.score_daily_decline_rate(pl.DataFrame(rows).lazy()).collect()

    assert scored.get_column("z").head(14).null_count() == 14  # no full baseline yet
    spike = scored.row(14, named=True)
    assert spike["baseline_attempts"] == 1400
    assert spike["baseline_rate"] == pytest.approx(0.20)
    assert spike["z"] == pytest.approx(5.0)
    assert spike["is_anomaly"] is True
    after = scored.row(15, named=True)
    assert after["baseline_rate"] == pytest.approx(300 / 1400)  # the spike day is in the window
    assert after["z"] == pytest.approx(
        (0.26 - 300 / 1400) / (300 / 1400 * 1100 / 1400 / 100) ** 0.5
    )
    assert after["is_anomaly"] is False
    assert scored.get_column("is_anomaly").sum() == 1


def test_daily_decline_rate_needs_at_least_50_attempts() -> None:
    """16 of 40 against a 20% baseline is z = 3.16, but 40 attempts is below the minimum."""
    rows = [_agg_day(day, 100, 20) for day in range(14)]
    rows.append(_agg_day(14, 40, 16))  # z = 0.20 / sqrt(0.16 / 40) = 3.162

    last = (
        anomalies.score_daily_decline_rate(pl.DataFrame(rows).lazy()).collect().row(-1, named=True)
    )

    assert last["z"] == pytest.approx(3.1623, abs=1e-4)
    assert last["is_anomaly"] is False


def test_daily_baselines_do_not_mix_segments() -> None:
    """A second PSP with a 50% decline rate does not move the first PSP's 20% baseline."""
    rows = [_agg_day(day, 100, 20) for day in range(15)]
    rows += [_agg_day(day, 100, 50, psp="PSP_B") for day in range(15)]

    scored = anomalies.score_daily_decline_rate(pl.DataFrame(rows).lazy()).collect()

    last_day = scored.filter(pl.col("date") == START + timedelta(days=14)).sort("psp")
    assert last_day.get_column("baseline_rate").to_list() == pytest.approx([0.20, 0.50])
    assert last_day.get_column("z").to_list() == pytest.approx([0.0, 0.0])


def test_merchant_far_below_peers_is_flagged() -> None:
    """50% against peers at 90%: the Wilson upper bound 59.6% is below 90% - 10 points."""
    fct = _transactions(
        [
            {"merchant_id": "mrc_low", "final_status": "approved", "n": 50},
            {"merchant_id": "mrc_low", "final_status": "declined", "n": 50},
            {"merchant_id": "mrc_b", "final_status": "approved", "n": 90},
            {"merchant_id": "mrc_b", "final_status": "declined", "n": 10},
            {"merchant_id": "mrc_c", "final_status": "approved", "n": 90},
            {"merchant_id": "mrc_c", "final_status": "declined", "n": 10},
        ]
    )

    scored = anomalies.score_merchants_against_peers(fct).collect()
    rows = {row["merchant_id"]: row for row in scored.iter_rows(named=True)}

    assert scored.get_column("metric").unique().to_list() == ["auth_rate"]  # no vouchers here
    assert rows["mrc_low"]["peer_rate"] == pytest.approx(180 / 200)  # the merchant is left out
    assert rows["mrc_low"]["wilson_high"] == pytest.approx(0.596170, abs=1e-5)
    assert rows["mrc_low"]["gap"] == pytest.approx(0.40)
    assert rows["mrc_low"]["is_anomaly"] is True
    assert rows["mrc_b"]["peer_rate"] == pytest.approx(140 / 200)
    assert rows["mrc_b"]["is_anomaly"] is False
    assert rows["mrc_low"]["peer_scope"] == "country_category"


def test_merchant_without_category_peers_falls_back_to_country_peers() -> None:
    """The only travel merchant in Mexico is compared with the other Mexican merchants."""
    fct = _transactions(
        [
            {"merchant_id": "mrc_a", "merchant_category": "travel", "n": 60},
            {"merchant_id": "mrc_b", "final_status": "approved", "n": 30},
            {"merchant_id": "mrc_b", "final_status": "declined", "n": 30},
        ]
    )

    scored = anomalies.score_merchants_against_peers(fct).collect()
    travel = scored.filter(pl.col("merchant_id") == "mrc_a").row(0, named=True)

    assert travel["peer_scope"] == "country"
    assert travel["peer_attempts"] == 60
    assert travel["peer_rate"] == pytest.approx(0.5)


def test_small_merchants_are_not_flagged() -> None:
    """0 of 10 is far below peers, but 10 attempts is below the minimum of 50."""
    fct = _transactions(
        [
            {"merchant_id": "mrc_tiny", "final_status": "declined", "n": 10},
            {"merchant_id": "mrc_b", "final_status": "approved", "n": 100},
        ]
    )

    scored = anomalies.score_merchants_against_peers(fct).collect()

    assert scored.get_column("is_anomaly").sum() == 0


def test_amount_buckets_are_left_closed() -> None:
    """9.99 USD is in 0-10, 10 USD in 10-20, 24.99 USD in 20-50 and exactly 250 USD in 250+."""
    fct = _transactions(
        [
            {"amount_usd": 9.99, "final_status": "declined"},
            {"amount_usd": 10.0, "final_status": "approved"},
            {"amount_usd": 24.99, "final_status": "declined"},
            {"amount_usd": 250.0, "final_status": "failed"},
        ]
    )

    result = anomalies.decline_rate_by_amount_bucket(fct).collect()

    assert result.get_column("amount_bucket").to_list() == ["0-10", "10-20", "20-50", "250+"]
    assert result.get_column("attempts").to_list() == [1, 1, 1, 1]
    assert result.get_column("decline_rate").to_list() == pytest.approx([1.0, 0.0, 1.0, 0.0])
    assert result.get_column("failure_rate").to_list() == pytest.approx([0.0, 0.0, 0.0, 1.0])


def test_reason_breakdown_shares_sum_to_one_per_country_and_method() -> None:
    """3 insufficient_funds and 1 network_timeout are 75% and 25% of the card failures."""
    fct = _transactions(
        [
            {
                "final_status": "declined",
                "decline_reason": "insufficient_funds",
                "n": 3,
            },
            {"final_status": "failed", "decline_reason": "network_timeout", "n": 1},
            {"final_status": "approved", "n": 6},
            {"final_status": "expired", "decline_reason": "expired", "n": 2},
        ]
    )

    result = anomalies.reason_breakdown(fct).collect()

    assert result.get_column("decline_reason").to_list() == [
        "insufficient_funds",
        "network_timeout",
    ]
    assert result.get_column("transactions").to_list() == [3, 1]
    assert result.get_column("share").to_list() == pytest.approx([0.75, 0.25])


def test_heatmap_has_one_row_per_weekday_and_hour() -> None:
    """Two slots: Monday 02:00 with 1 decline of 2 attempts, Friday 20:00 with none of 1."""
    fct = _transactions(
        [
            {"local_weekday": 1, "local_hour": 2, "final_status": "declined"},
            {"local_weekday": 1, "local_hour": 2, "final_status": "approved"},
            {"local_weekday": 5, "local_hour": 20, "final_status": "approved"},
        ]
    )

    result = anomalies.decline_heatmap(fct).collect()

    assert result.select("local_weekday", "local_hour").rows() == [(1, 2), (5, 20)]
    assert result.get_column("attempts").to_list() == [2, 1]
    assert result.get_column("decline_rate").to_list() == pytest.approx([0.5, 0.0])
    assert result.get_column("overall_decline_rate").to_list() == pytest.approx([1 / 3, 1 / 3])
    assert result.get_column("decline_rate_is_high").to_list() == [False, False]  # tiny samples


def test_heatmap_marks_only_cells_clearly_above_the_overall_rate() -> None:
    """Overall 300 of 3,000 is 10%: a cell at 40% of 500 is marked, one at 12% of 500 is not."""
    fct = _transactions(
        [
            {"local_weekday": 6, "local_hour": 3, "final_status": "declined", "n": 200},
            {"local_weekday": 6, "local_hour": 3, "final_status": "approved", "n": 300},
            {"local_weekday": 2, "local_hour": 9, "final_status": "declined", "n": 60},
            {"local_weekday": 2, "local_hour": 9, "final_status": "approved", "n": 440},
            {"local_weekday": 3, "local_hour": 15, "final_status": "declined", "n": 40},
            {"local_weekday": 3, "local_hour": 15, "final_status": "approved", "n": 1960},
        ]
    )

    result = anomalies.decline_heatmap(fct).collect().sort("local_weekday")

    assert result.get_column("overall_decline_rate").to_list() == pytest.approx([0.1] * 3)
    assert result.get_column("decline_rate").to_list() == pytest.approx([0.12, 0.02, 0.40])
    # 60 of 500 at z = 3.43: lower bound 0.079, below 10%. 200 of 500: lower bound 0.328.
    assert result.get_column("decline_rate_is_high").to_list() == [False, False, True]


def test_voucher_expiration_by_merchant_uses_resolved_vouchers_only() -> None:
    """3 expired and 1 paid is 75%; the pending voucher and the card payment are ignored."""
    fct = _transactions(
        [
            {
                "payment_method": "oxxo",
                "final_status": "expired",
                "decline_reason": "expired",
                "n": 3,
            },
            {"payment_method": "oxxo", "final_status": "approved", "n": 1},
            {"payment_method": "oxxo", "final_status": "pending", "n": 1},
            {"payment_method": "card", "final_status": "approved", "n": 5},
            {
                "merchant_id": "mrc_002",
                "payment_method": "card",
                "final_status": "approved",
            },
        ]
    )

    result = anomalies.voucher_expiration_by_merchant(fct).collect()

    assert result.get_column("merchant_id").to_list() == ["mrc_001"]
    assert result.get_column("voucher_attempts").to_list() == [4]
    assert result.get_column("expiration_rate").to_list() == pytest.approx([0.75])
    by_bucket = anomalies.voucher_expiration_by_amount_bucket(fct).collect()
    assert by_bucket.get_column("amount_bucket").to_list() == ["20-50"]
    assert by_bucket.get_column("expiration_rate").to_list() == pytest.approx([0.75])


def test_share_of_expired_vouchers_by_ticket_size() -> None:
    """Of 10 expired OXXO vouchers, 5 are under 20 USD: the cumulative share there is 50%."""
    expired = {"payment_method": "oxxo", "final_status": "expired", "decline_reason": "expired"}
    fct = _transactions(
        [
            {**expired, "amount_usd": 5.0, "n": 2},
            {**expired, "amount_usd": 15.0, "n": 3},
            {**expired, "amount_usd": 30.0, "n": 5},
            {"payment_method": "oxxo", "final_status": "approved", "amount_usd": 5.0, "n": 2},
            {"payment_method": "oxxo", "final_status": "approved", "amount_usd": 30.0, "n": 15},
        ]
    )

    result = anomalies.voucher_expiration_by_amount_bucket(fct).collect()

    assert result.get_column("amount_bucket").to_list() == ["0-10", "10-20", "20-50"]
    assert result.get_column("expiration_rate").to_list() == pytest.approx([0.5, 1.0, 0.25])
    assert result.get_column("share_of_expired").to_list() == pytest.approx([0.2, 0.3, 0.5])
    assert result.get_column("cumulative_share_of_expired").to_list() == pytest.approx(
        [0.2, 0.5, 1.0]
    )


def test_decline_rate_by_card_brand_ignores_non_card_methods() -> None:
    """Visa declines 1 of 4 and Mastercard 2 of 4; the PSE decline has no brand."""
    fct = _transactions(
        [
            {"card_brand": "visa", "final_status": "approved", "n": 3},
            {"card_brand": "visa", "final_status": "declined", "n": 1},
            {"card_brand": "mastercard", "final_status": "approved", "n": 2},
            {"card_brand": "mastercard", "final_status": "declined", "n": 2},
            {"card_brand": None, "payment_method": "pse", "final_status": "declined", "n": 5},
        ]
    )

    result = anomalies.decline_rate_by_card_brand(fct).collect()

    assert result.get_column("card_brand").to_list() == ["mastercard", "visa"]
    assert result.get_column("attempts").to_list() == [4, 4]
    assert result.get_column("decline_rate").to_list() == pytest.approx([0.5, 0.25])


def test_decline_rate_by_merchant_segment() -> None:
    """Small merchants decline 3 of 4 and enterprise merchants 1 of 4."""
    fct = _transactions(
        [
            {"merchant_size_tier": "small", "final_status": "declined", "n": 3},
            {"merchant_size_tier": "small", "final_status": "approved", "n": 1},
            {"merchant_size_tier": "enterprise", "final_status": "declined", "n": 1},
            {"merchant_size_tier": "enterprise", "final_status": "approved", "n": 3},
        ]
    )

    result = anomalies.decline_rate_by_merchant_segment(fct, "merchant_size_tier").collect()

    assert result.get_column("merchant_size_tier").to_list() == ["enterprise", "small"]
    assert result.get_column("decline_rate").to_list() == pytest.approx([0.25, 0.75])


def test_hourly_reason_rate_is_scored_against_the_trailing_14_days() -> None:
    """Baseline 2%: 15 timeouts in 30 attempts is flagged; 9 in 100 has too few events."""
    groups: list[dict[str, Any]] = []
    for day in range(14):  # 14 baseline days: 100 attempts and 2 timeouts each, at 10:00
        slot = {
            "psp": "PSP_A",
            "local_date": START + timedelta(days=day),
            "local_hour": 10,
        }
        groups.append({**slot, "final_status": "approved", "n": 98})
        groups.append(
            {
                **slot,
                "final_status": "failed",
                "decline_reason": "network_timeout",
                "n": 2,
            }
        )
    today = {"psp": "PSP_A", "local_date": START + timedelta(days=14)}
    timeout = {"final_status": "failed", "decline_reason": "network_timeout"}
    groups += [
        {**today, "local_hour": 3, "final_status": "approved", "n": 15},
        {**today, "local_hour": 3, **timeout, "n": 15},
        {**today, "local_hour": 10, "final_status": "approved", "n": 91},
        {**today, "local_hour": 10, **timeout, "n": 9},
    ]

    scored = anomalies.score_hourly_reason_rate(_transactions(groups)).collect()
    last_day = scored.filter(pl.col("local_date") == today["local_date"]).sort("local_hour")
    night, morning = last_day.row(0, named=True), last_day.row(1, named=True)

    assert night["baseline_rate"] == pytest.approx(28 / 1400)  # 0.02
    # z = (0.50 - 0.02) / sqrt(0.02 * 0.98 / 30) = 18.78
    assert night["z"] == pytest.approx(18.78, abs=0.01)
    assert night["is_anomaly"] is True
    # z = (0.09 - 0.02) / sqrt(0.02 * 0.98 / 100) = 5.0, but only 9 events.
    assert morning["z"] == pytest.approx(5.0)
    assert morning["is_anomaly"] is False
    assert scored.get_column("is_anomaly").sum() == 1


@pytest.fixture(scope="module")
def full_scale_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Run the whole pipeline once with the default, full scale configuration."""
    data_dir = tmp_path_factory.mktemp("data")
    run_pipeline(data_dir)
    return data_dir


def test_every_planted_anomaly_is_detected(full_scale_data: Path) -> None:
    """Every planted anomaly is detected by the rule designed for it."""
    planted = json.loads((full_scale_data / "raw" / "planted_anomalies.json").read_text())
    by_id = {anomaly["id"]: anomaly for anomaly in planted["anomalies"]}
    assert set(by_id) == {"a", "b", "c", "d", "e"}
    fct = pl.scan_parquet(full_scale_data / "marts" / "fct_transactions.parquet")
    agg_daily = pl.scan_parquet(full_scale_data / "marts" / "agg_daily.parquet")

    # a) Decline rate spike: every planted day of the planted segment is flagged.
    spike = by_id["a"]
    first, last = date.fromisoformat(spike["start_date"]), date.fromisoformat(spike["end_date"])
    expected_days = {
        (
            first + timedelta(days=offset),
            spike["psp"],
            spike["country"],
            spike["payment_method"],
        )
        for offset in range((last - first).days + 1)
    }
    daily_flags = (
        anomalies.score_daily_decline_rate(agg_daily).filter(pl.col("is_anomaly")).collect()
    )
    flagged_days = set(daily_flags.select("date", "psp", "country", "payment_method").rows())
    # The daily rule also flags the segments hit by the planted merchant problem (anomaly d)
    # and a few chance days, so the planted days must be among the flags, not the only ones.
    assert expected_days <= flagged_days

    # b) and d) Merchants against peers: the voucher merchant on completion, the processing
    # merchant on authorization, and nobody else.
    voucher, processing = by_id["b"], by_id["d"]
    merchant_flags = (
        anomalies.score_merchants_against_peers(fct).filter(pl.col("is_anomaly")).collect()
    )
    assert set(merchant_flags.select("merchant_id", "metric").rows()) == {
        (voucher["merchant_id"], "completion_rate"),
        (processing["merchant_id"], "auth_rate"),
    }
    worst = anomalies.voucher_expiration_by_merchant(fct).collect().row(0, named=True)
    assert worst["merchant_id"] == voucher["merchant_id"]
    assert worst["expiration_rate"] == pytest.approx(voucher["expected_expiration_rate"], abs=0.03)

    # c) Timeout spike: every flag is a planted slot and every planted night is detected. A slot
    # with fewer than 20 attempts or 10 events is below the rule's minimum sample.
    timeout = by_id["c"]
    planted_nights = {date.fromisoformat(day) for day in timeout["dates"]}
    hourly_flags = anomalies.score_hourly_reason_rate(fct).filter(pl.col("is_anomaly")).collect()
    for psp, country, night, hour, reason in hourly_flags.select(
        "psp", "country", "local_date", "local_hour", "decline_reason"
    ).rows():
        assert (psp, country, reason) == (timeout["psp"], timeout["country"], "network_timeout")
        assert night in planted_nights
        assert hour in timeout["local_hours"]
    assert set(hourly_flags.get_column("local_date").to_list()) == planted_nights

    # d) and e) Merchant health: exactly the two planted merchants are at risk, each with the
    # planted diagnosis.
    ux = by_id["e"]
    at_risk = merchant_health(fct).filter(pl.col("label") == "at_risk").collect()
    assert dict(at_risk.select("merchant_id", "diagnosis").rows()) == {
        processing["merchant_id"]: processing["expected_diagnosis"],
        ux["merchant_id"]: ux["expected_diagnosis"],
    }
