"""Failure analysis and anomaly detection.

Purpose: describe how payments fail (reasons, time of day, ticket size, voucher expiration) and
    flag segments, time slots and merchants that behave abnormally.
Inputs: ``fct_transactions`` (one row per transaction) and ``agg_daily`` (one row per date x
    country x psp x payment_method) as lazy frames.
Outputs: lazy frames. Descriptive functions return rates with their sample size and Wilson 95%
    interval. Detection functions return every scored row with an ``is_anomaly`` flag, so callers
    can plot the full series and filter the flags.

Every rate comes from ``analytics.metrics``; this module only chooses the grain and the rule.

Detection rules (thresholds are module constants, documented in ``docs/DECISIONS.md``):
    * Daily decline rate per psp x country x payment_method against the pooled rate of the
      trailing 14 days: flagged when ``z >= 3`` and ``attempts >= 50``.
    * Hourly rate of each decline or failure reason per psp x country against the pooled rate of
      the trailing 14 days: flagged when ``z >= 5``, ``attempts >= 20`` and ``events >= 10``.
    * Merchant authorization and completion rate against country and category peers: flagged
      when the merchant's Wilson upper bound is more than 10 points below the peer rate and the
      merchant has at least 50 attempts.

Assumptions:
    * ``z = (rate - baseline_rate) / sqrt(baseline_rate * (1 - baseline_rate) / attempts)``, the
      one sided binomial z score of the observed rate under the baseline rate.
    * Every segment has transactions on every day it is live, so a window of 14 rows is a window
      of 14 days. A segment is scored only once it has 14 days of history.
    * Baselines include earlier anomalous days. That makes detection slightly conservative
      during a multi day incident, which is the safe direction.
"""

from collections.abc import Sequence

import polars as pl

from analytics.metrics import (
    ATTEMPT_STATUSES,
    NOT_APPROVED_STATUSES,
    outcome_rates,
    performance,
)
from pipeline.transform import aggregate_additive_measures

BASELINE_DAYS = 14
Z_THRESHOLD = 3.0
# The hourly detector scores about 50,000 slot and reason combinations, so z >= 3 alone would
# flag dozens of them by chance. A stricter threshold keeps the expected false positives below one.
HOURLY_Z_THRESHOLD = 5.0
MIN_DAILY_ATTEMPTS = 50
MIN_HOURLY_ATTEMPTS = 20
MIN_HOURLY_EVENTS = 10
MIN_MERCHANT_ATTEMPTS = 50
PEER_MARGIN = 0.10

DAILY_SEGMENT: tuple[str, ...] = ("psp", "country", "payment_method")
HOURLY_SEGMENT: tuple[str, ...] = ("psp", "country")
PEER_GROUP: tuple[str, ...] = ("country", "merchant_category")

# Upper edges (exclusive) of the USD ticket size buckets; the last bucket is open ended.
AMOUNT_BUCKET_EDGES_USD: tuple[float, ...] = (10.0, 25.0, 50.0, 100.0, 250.0)
AMOUNT_BUCKET_LABELS: tuple[str, ...] = (
    "0-10",
    "10-25",
    "25-50",
    "50-100",
    "100-250",
    "250+",
)


def _z_score(successes: pl.Expr, n: pl.Expr, baseline_rate: pl.Expr) -> pl.Expr:
    """Build the binomial z score of ``successes / n`` under ``baseline_rate``.

    Null when the baseline is missing or degenerate (0 or 1) or when ``n`` is zero.
    """
    defined = (n > 0) & (baseline_rate > 0) & (baseline_rate < 1)
    standard_error = (baseline_rate * (1 - baseline_rate) / n).sqrt()
    return (
        pl.when(defined)
        .then((successes / n - baseline_rate) / standard_error)
        .otherwise(None)
    )


def _with_amount_bucket(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Add ``amount_bucket`` (label) and ``amount_bucket_order`` (0 for the smallest tickets)."""
    order = pl.sum_horizontal(
        (pl.col("amount_usd") >= edge).cast(pl.Int8) for edge in AMOUNT_BUCKET_EDGES_USD
    )
    return fct.with_columns(
        order.alias("amount_bucket_order"),
        order.replace_strict(
            dict(enumerate(AMOUNT_BUCKET_LABELS)), return_dtype=pl.String
        ).alias("amount_bucket"),
    )


def _outcome_rates_by(fct: pl.LazyFrame, dims: Sequence[str]) -> pl.LazyFrame:
    """Aggregate transactions to ``dims`` and compute the decline, failure and expiration rates."""
    measures = aggregate_additive_measures(fct, [*dims, "payment_method"])
    return outcome_rates(measures, dims)


def reason_breakdown(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Rank decline and failure reasons by volume within each country and payment method.

    Grain: one row per ``country`` x ``payment_method`` x ``final_status`` x ``decline_reason``,
    restricted to transactions whose final status is declined or failed.

    Formula: ``share = transactions / all declined or failed transactions of the same country
    and payment method``, so the shares of one country and method sum to 1.

    Args:
        fct: One row per transaction.

    Returns:
        ``country``, ``payment_method``, ``final_status``, ``decline_reason``, ``transactions``
        and ``share``, sorted by country, method and descending volume.
    """
    group = ["country", "payment_method"]
    return (
        fct.filter(pl.col("final_status").is_in(NOT_APPROVED_STATUSES))
        .group_by(*group, "final_status", "decline_reason")
        .agg(pl.len().alias("transactions"))
        .with_columns(
            (pl.col("transactions") / pl.col("transactions").sum().over(group)).alias(
                "share"
            )
        )
        .sort(
            [*group, "transactions", "decline_reason"],
            descending=[False, False, True, False],
        )
    )


def decline_heatmap(fct: pl.LazyFrame, dims: Sequence[str] = ()) -> pl.LazyFrame:
    """Compute decline and failure rates per local weekday and hour.

    Grain: one row per ``dims`` x ``local_weekday`` (ISO, 1 is Monday) x ``local_hour``.

    Args:
        fct: One row per transaction.
        dims: Extra columns to split the heatmap by, for example ``["country"]``.

    Returns:
        The columns of ``analytics.metrics.outcome_rates`` at that grain.
    """
    return _outcome_rates_by(fct, [*dims, "local_weekday", "local_hour"])


def decline_rate_by_amount_bucket(
    fct: pl.LazyFrame, dims: Sequence[str] = ()
) -> pl.LazyFrame:
    """Compute decline and failure rates per USD ticket size bucket.

    Grain: one row per ``dims`` x ``amount_bucket``. Buckets are left closed:
    0-10, 10-25, 25-50, 50-100, 100-250 and 250+ USD.

    Args:
        fct: One row per transaction, with ``amount_usd``.
        dims: Extra columns to split by, for example ``["country"]``.

    Returns:
        ``dims``, ``amount_bucket_order``, ``amount_bucket`` and the columns of
        ``analytics.metrics.outcome_rates``, sorted by ``dims`` and bucket order.
    """
    return _outcome_rates_by(
        _with_amount_bucket(fct), [*dims, "amount_bucket_order", "amount_bucket"]
    )


def oxxo_expiration_by_amount_bucket(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the voucher expiration rate per USD ticket size bucket.

    Grain: one row per ``amount_bucket``, over voucher transactions only.

    Args:
        fct: One row per transaction, with ``amount_usd``.

    Returns:
        ``amount_bucket_order``, ``amount_bucket``, ``voucher_attempts``, ``expired``,
        ``expiration_rate`` and its Wilson bounds, for buckets with resolved vouchers.
    """
    return (
        _outcome_rates_by(
            _with_amount_bucket(fct), ["amount_bucket_order", "amount_bucket"]
        )
        .filter(pl.col("voucher_attempts") > 0)
        .select(
            "amount_bucket_order",
            "amount_bucket",
            "voucher_attempts",
            pl.col("^expir.*$"),
        )
    )


def oxxo_expiration_by_merchant(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the voucher expiration rate per merchant.

    Grain: one row per ``merchant_id``, over voucher transactions only.

    Args:
        fct: One row per transaction.

    Returns:
        ``merchant_id``, ``voucher_attempts``, ``expired``, ``expiration_rate`` and its Wilson
        bounds, for merchants with resolved vouchers, sorted by descending expiration rate.
    """
    return (
        _outcome_rates_by(fct, ["merchant_id"])
        .filter(pl.col("voucher_attempts") > 0)
        .select("merchant_id", "voucher_attempts", pl.col("^expir.*$"))
        .sort("expiration_rate", "merchant_id", descending=[True, False])
    )


def score_daily_decline_rate(agg_daily: pl.LazyFrame) -> pl.LazyFrame:
    """Score each day's decline rate against the trailing 14 day baseline of its segment.

    Grain: one row per ``date`` x ``psp`` x ``country`` x ``payment_method``.

    Formulas:
        ``baseline_rate = declined / attempts`` pooled over the previous 14 days of the segment
        ``z`` as defined in the module docstring
        ``is_anomaly = z >= 3 and attempts >= 50``

    Args:
        agg_daily: The daily aggregate mart.

    Returns:
        ``date``, the segment columns, ``attempts``, ``declined``, ``decline_rate``,
        ``baseline_attempts``, ``baseline_rate``, ``z`` and ``is_anomaly``. ``z`` is null, and the
        row is not flagged, until the segment has 14 days of history.
    """
    segment = list(DAILY_SEGMENT)
    daily = outcome_rates(agg_daily, ["date", *segment]).sort("date")

    def trailing(column: str) -> pl.Expr:
        return pl.col(column).rolling_sum(BASELINE_DAYS).shift(1).over(segment)

    return (
        daily.with_columns(
            trailing("attempts").alias("baseline_attempts"),
            (trailing("declined") / trailing("attempts")).alias("baseline_rate"),
        )
        .with_columns(
            _z_score(
                pl.col("declined"), pl.col("attempts"), pl.col("baseline_rate")
            ).alias("z")
        )
        .with_columns(
            ((pl.col("z") >= Z_THRESHOLD) & (pl.col("attempts") >= MIN_DAILY_ATTEMPTS))
            .fill_null(False)
            .alias("is_anomaly")
        )
        .select(
            "date",
            *segment,
            "attempts",
            "declined",
            "decline_rate",
            "baseline_attempts",
            "baseline_rate",
            "z",
            "is_anomaly",
        )
        .sort("date", *segment)
    )


def score_hourly_reason_rate(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Score each hour's rate of every decline or failure reason against a 14 day baseline.

    Grain: one row per ``psp`` x ``country`` x ``local_date`` x ``local_hour`` x
    ``decline_reason``, for slots where the reason occurred at least once.

    Formulas:
        ``rate = events / attempts`` within the hour, where ``events`` counts attempts that
            ended with that reason
        ``baseline_rate = events / attempts`` pooled over the previous 14 days (all hours) of
            the same psp, country and reason
        ``z`` as defined in the module docstring
        ``is_anomaly = z >= 5 and attempts >= 20 and events >= 10``

    Args:
        fct: One row per transaction.

    Returns:
        ``psp``, ``country``, ``local_date``, ``local_hour``, ``decline_reason``, ``attempts``,
        ``events``, ``rate``, ``baseline_rate``, ``z`` and ``is_anomaly``.
    """
    segment = list(HOURLY_SEGMENT)
    slot = [*segment, "local_date", "local_hour"]
    attempts = fct.filter(pl.col("final_status").is_in(ATTEMPT_STATUSES))
    with_reason = attempts.filter(pl.col("decline_reason").is_not_null())

    # Dense day x reason grid per segment, so a rolling window of 14 rows spans 14 days even
    # when a reason did not occur on some of them.
    daily_attempts = attempts.group_by(*segment, "local_date").agg(
        pl.len().alias("day_attempts")
    )
    daily_events = with_reason.group_by(*segment, "decline_reason", "local_date").agg(
        pl.len().alias("day_events")
    )
    reasons = with_reason.select("decline_reason").unique()
    series = [*segment, "decline_reason"]

    def trailing(column: str) -> pl.Expr:
        return pl.col(column).rolling_sum(BASELINE_DAYS).shift(1).over(series)

    baseline = (
        daily_attempts.join(reasons, how="cross")
        .join(daily_events, on=[*series, "local_date"], how="left")
        .with_columns(pl.col("day_events").fill_null(0))
        .sort("local_date")
        .select(
            *series,
            "local_date",
            (trailing("day_events") / trailing("day_attempts")).alias("baseline_rate"),
        )
    )

    slot_attempts = attempts.group_by(slot).agg(pl.len().alias("attempts"))
    return (
        with_reason.group_by(*slot, "decline_reason")
        .agg(pl.len().alias("events"))
        .join(slot_attempts, on=slot, how="inner")
        .join(baseline, on=[*series, "local_date"], how="left")
        .with_columns(
            (pl.col("events") / pl.col("attempts")).alias("rate"),
            _z_score(
                pl.col("events"), pl.col("attempts"), pl.col("baseline_rate")
            ).alias("z"),
        )
        .with_columns(
            (
                (pl.col("z") >= HOURLY_Z_THRESHOLD)
                & (pl.col("attempts") >= MIN_HOURLY_ATTEMPTS)
                & (pl.col("events") >= MIN_HOURLY_EVENTS)
            )
            .fill_null(False)
            .alias("is_anomaly")
        )
        .select(
            *slot,
            "decline_reason",
            "attempts",
            "events",
            "rate",
            "baseline_rate",
            "z",
            "is_anomaly",
        )
        .sort(*slot, "decline_reason")
    )


def score_merchants_against_peers(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Compare each merchant's authorization and completion rate with its peers.

    Grain: one row per ``merchant_id`` x ``metric``, where ``metric`` is ``auth_rate`` or
    ``completion_rate``, for merchants with at least one attempt on that metric.

    Formulas:
        ``peer_rate = successes / attempts`` pooled over the other merchants of the same country
            and category (the merchant itself is left out). When the merchant has no such peer,
            the other merchants of its country are used and ``peer_scope`` says so.
        ``gap = peer_rate - rate``
        ``is_anomaly = attempts >= 50 and wilson_high < peer_rate - 0.10``: even the optimistic
            end of the merchant's interval is more than 10 points below its peers.

    Limitation: the authorization rate is not adjusted for payment method mix, so a merchant
    selling mostly through a weak method can look worse than peers for that reason alone.

    Args:
        fct: One row per transaction, with ``merchant_category``.

    Returns:
        ``merchant_id``, ``country``, ``merchant_category``, ``metric``, ``attempts``,
        ``successes``, ``rate``, ``wilson_low``, ``wilson_high``, ``peer_scope``,
        ``peer_attempts``, ``peer_rate``, ``gap`` and ``is_anomaly``.
    """
    keys = ["merchant_id", *PEER_GROUP]
    per_merchant = performance(
        aggregate_additive_measures(fct, [*keys, "payment_method"]), keys
    )
    long = pl.concat(
        [
            per_merchant.select(
                *keys,
                pl.lit("auth_rate").alias("metric"),
                "attempts",
                pl.col("approved").alias("successes"),
                pl.col("auth_rate").alias("rate"),
                "wilson_low",
                "wilson_high",
            ),
            per_merchant.select(
                *keys,
                pl.lit("completion_rate").alias("metric"),
                pl.col("voucher_attempts").alias("attempts"),
                pl.col("voucher_paid").alias("successes"),
                pl.col("completion_rate").alias("rate"),
                pl.col("completion_wilson_low").alias("wilson_low"),
                pl.col("completion_wilson_high").alias("wilson_high"),
            ),
        ]
    ).filter(pl.col("attempts") > 0)

    def others(column: str, group: Sequence[str]) -> pl.Expr:
        return pl.col(column).sum().over([*group, "metric"]) - pl.col(column)

    category_attempts = others("attempts", PEER_GROUP)
    use_category = category_attempts > 0
    peer_attempts = (
        pl.when(use_category)
        .then(category_attempts)
        .otherwise(others("attempts", ["country"]))
    )
    peer_successes = (
        pl.when(use_category)
        .then(others("successes", PEER_GROUP))
        .otherwise(others("successes", ["country"]))
    )
    return (
        long.with_columns(
            pl.when(use_category)
            .then(pl.lit("country_category"))
            .otherwise(pl.lit("country"))
            .alias("peer_scope"),
            peer_attempts.alias("peer_attempts"),
            pl.when(peer_attempts > 0)
            .then(peer_successes / peer_attempts)
            .otherwise(None)
            .alias("peer_rate"),
        )
        .with_columns(
            (pl.col("peer_rate") - pl.col("rate")).alias("gap"),
            (
                (pl.col("attempts") >= MIN_MERCHANT_ATTEMPTS)
                & (pl.col("wilson_high") < pl.col("peer_rate") - PEER_MARGIN)
            )
            .fill_null(False)
            .alias("is_anomaly"),
        )
        .sort("metric", "merchant_id")
    )
