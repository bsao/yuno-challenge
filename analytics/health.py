"""Merchant health score.

Purpose: rank merchants from 0 (worst) to 100 (best) so an account team knows whom to call first
    and why.
Inputs: ``fct_transactions`` as a lazy frame (one row per transaction).
Outputs: one row per merchant with four component scores, the weighted ``health_score``, a
    ``label`` (``at_risk`` below 50, otherwise ``healthy``) and the ``main_driver`` of the score.

Score = weighted sum of four component scores. Each component maps its input linearly from a
"bad" anchor (score 0) to a "good" anchor (score 100), clipped to [0, 100]:

    Component      Weight  Input                                          Bad     Good
    authorization  0.40    auth_rate minus peer auth_rate                 -0.10   +0.05
    failure        0.20    failure_rate (technical failures / attempts)    0.10    0.00
    refund         0.15    refund_rate (refunded / approved)               0.10    0.00
    volume         0.25    transactions last 30 days / previous 30 - 1    -0.50   +0.25

Assumptions:
    * Rates come from ``analytics.metrics``; peers are the other merchants of the same country
      and category, as in ``analytics.anomalies.score_merchants_against_peers``.
    * Rates cover the whole input window; the volume trend compares the 30 local days ending on
      the last date in the input with the 30 days before them.
    * A component whose input is undefined (no attempts, no approved transaction, no volume in
      the previous 30 days) scores a neutral 50.
    * ``main_driver`` is the component that costs the most points: the largest
      ``weight * (100 - component score)``.
    * The authorization component is not adjusted for payment method mix, and small merchants
      have noisy components; ``attempts`` is returned so readers can judge the sample.
"""

from datetime import timedelta

import polars as pl

from analytics.anomalies import score_merchants_against_peers
from analytics.metrics import outcome_rates
from pipeline.transform import aggregate_additive_measures

AT_RISK_THRESHOLD = 50.0
NEUTRAL_SCORE = 50.0
TREND_DAYS = 30

# Component name -> (weight, input column, bad anchor, good anchor). Weights sum to 1.
COMPONENTS: dict[str, tuple[float, str, float, float]] = {
    "authorization": (0.40, "auth_gap", -0.10, 0.05),
    "failure": (0.20, "failure_rate", 0.10, 0.0),
    "refund": (0.15, "refund_rate", 0.10, 0.0),
    "volume": (0.25, "volume_trend", -0.50, 0.25),
}


def score_components(components: pl.LazyFrame) -> pl.LazyFrame:
    """Turn the four component inputs into component scores, the health score and its driver.

    Formulas:
        ``score_<c> = clip((input - bad) / (good - bad) * 100, 0, 100)``, or 50 when the input
            is null
        ``health_score = sum(weight_c * score_c)``
        ``label = "at_risk" if health_score < 50 else "healthy"``
        ``main_driver = argmax_c(weight_c * (100 - score_c))``

    Args:
        components: One row per merchant with ``auth_gap``, ``failure_rate``, ``refund_rate``
            and ``volume_trend``.

    Returns:
        The input columns plus ``score_authorization``, ``score_failure``, ``score_refund``,
        ``score_volume``, ``health_score``, ``label`` and ``main_driver``.
    """
    names = list(COMPONENTS)
    scores = [
        ((pl.col(column) - bad) / (good - bad) * 100)
        .clip(0.0, 100.0)
        .fill_null(NEUTRAL_SCORE)
        .alias(f"score_{name}")
        for name, (_, column, bad, good) in COMPONENTS.items()
    ]
    health = pl.sum_horizontal(
        pl.col(f"score_{name}") * weight for name, (weight, _, _, _) in COMPONENTS.items()
    )
    points_lost = pl.concat_list(
        [(100 - pl.col(f"score_{name}")) * weight for name, (weight, _, _, _) in COMPONENTS.items()]
    )
    return (
        components.with_columns(scores)
        .with_columns(health.alias("health_score"))
        .with_columns(
            pl.when(pl.col("health_score") < AT_RISK_THRESHOLD)
            .then(pl.lit("at_risk"))
            .otherwise(pl.lit("healthy"))
            .alias("label"),
            points_lost.list.arg_max()
            .replace_strict(dict(enumerate(names)), return_dtype=pl.String)
            .alias("main_driver"),
        )
    )


def merchant_health(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Score every merchant and rank them, worst first.

    Grain: one row per ``merchant_id``.

    Inputs of the components:
        ``auth_gap = auth_rate - peer_rate`` (peers leave the merchant out)
        ``failure_rate`` and ``refund_rate`` from ``analytics.metrics.outcome_rates``
        ``volume_trend = transactions_last_30d / transactions_prior_30d - 1``

    Args:
        fct: One row per transaction, with ``merchant_category`` and ``local_date``.

    Returns:
        ``merchant_id``, ``country``, ``merchant_category``, ``attempts``, ``auth_rate``,
        ``peer_rate``, ``auth_gap``, ``failure_rate``, ``refund_rate``,
        ``transactions_last_30d``, ``transactions_prior_30d``, ``volume_trend`` and the columns
        added by ``score_components``, sorted by ascending ``health_score``.
    """
    keys = ["merchant_id", "country", "merchant_category"]
    last_day = pl.col("local_date").max()
    recent_start = last_day - timedelta(days=TREND_DAYS)
    prior_start = last_day - timedelta(days=2 * TREND_DAYS)
    volume = (
        fct.with_columns(
            (pl.col("local_date") > recent_start).alias("_recent"),
            ((pl.col("local_date") > prior_start) & (pl.col("local_date") <= recent_start)).alias(
                "_prior"
            ),
        )
        .group_by(keys)
        .agg(
            pl.col("_recent").sum().alias("transactions_last_30d"),
            pl.col("_prior").sum().alias("transactions_prior_30d"),
        )
        .with_columns(
            pl.when(pl.col("transactions_prior_30d") > 0)
            .then(pl.col("transactions_last_30d") / pl.col("transactions_prior_30d") - 1)
            .otherwise(None)
            .alias("volume_trend")
        )
    )
    authorization = (
        score_merchants_against_peers(fct)
        .filter(pl.col("metric") == "auth_rate")
        .select(
            "merchant_id",
            "attempts",
            pl.col("rate").alias("auth_rate"),
            "peer_rate",
            (pl.col("rate") - pl.col("peer_rate")).alias("auth_gap"),
        )
    )
    rates = outcome_rates(
        aggregate_additive_measures(fct, ["merchant_id", "payment_method"]), ["merchant_id"]
    ).select("merchant_id", "failure_rate", "refund_rate")

    components = (
        volume.join(authorization, on="merchant_id", how="left")
        .join(rates, on="merchant_id", how="left")
        .with_columns(pl.col("attempts").fill_null(0))
        .select(
            *keys,
            "attempts",
            "auth_rate",
            "peer_rate",
            "auth_gap",
            "failure_rate",
            "refund_rate",
            "transactions_last_30d",
            "transactions_prior_30d",
            "volume_trend",
        )
    )
    return score_components(components).sort("health_score", "merchant_id")
