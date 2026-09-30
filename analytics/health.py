"""Merchant health score.

Purpose: rank merchants from 0 (worst) to 100 (best) on their current state, and say whether a
    weak score points to a processing problem or a checkout experience problem.
Inputs: ``fct_transactions`` as a lazy frame (one row per transaction).
Outputs: one row per merchant with five component scores, the weighted ``health_score``, a
    ``label``, the ``main_driver`` of the score and a ``diagnosis``.

Score = weighted sum of five component scores. Each component maps its input linearly from a
"bad" anchor (score 0) to a "good" anchor (score 100), clipped to [0, 100]:

    Component      Weight  Input                                             Bad     Good
    authorization  0.35    auth_rate minus peer auth_rate                    -0.15   +0.05
    abandonment    0.20    abandonment_rate minus peer abandonment_rate      +0.15   -0.03
    volume         0.20    transactions last 30 days / previous 30 days - 1  -0.50   +0.10
    failure        0.15    failure_rate (technical failures / attempts)       0.10    0.01
    refund         0.10    refund_rate (refunded / approved)                  0.08    0.00

Labels: ``insufficient_data`` when the merchant has fewer than 50 attempts in the rate window,
``at_risk`` when the score is below 50, otherwise ``healthy``.

Diagnosis, from the main driver: ``processing`` (authorization or failure: the PSP or issuer
refuses or errors), ``ux`` (abandonment or volume: customers do not finish or do not come) or
``refunds``.

Assumptions:
    * Health is a current state, so every rate covers only the last 30 local days in the input;
      the volume trend compares those days with the 30 before them. A problem that started a
      month ago is therefore not diluted by two good months.
    * Rates come from ``analytics.metrics``. Peers are the other merchants of the same country
      and category over the same 30 days, leaving the merchant out; a merchant alone in its
      group is compared with its country.
    * A component whose input is undefined scores a neutral 50.
    * ``main_driver`` is the component that costs the most points: the largest
      ``weight * (100 - component score)``.
    * Peer gaps are not adjusted for payment method mix.
"""

from datetime import timedelta

import polars as pl

from analytics.metrics import outcome_rates
from pipeline.transform import aggregate_additive_measures

AT_RISK_THRESHOLD = 50.0
NEUTRAL_SCORE = 50.0
WINDOW_DAYS = 30
MIN_ATTEMPTS = 50
PEER_GROUP: tuple[str, ...] = ("country", "merchant_category")

# Component name -> (weight, input column, bad anchor, good anchor). Weights sum to 1.
COMPONENTS: dict[str, tuple[float, str, float, float]] = {
    "authorization": (0.35, "auth_gap", -0.15, 0.05),
    "abandonment": (0.20, "abandonment_gap", 0.15, -0.03),
    "volume": (0.20, "volume_trend", -0.50, 0.10),
    "failure": (0.15, "failure_rate", 0.10, 0.01),
    "refund": (0.10, "refund_rate", 0.08, 0.0),
}
DIAGNOSIS: dict[str, str] = {
    "authorization": "processing",
    "failure": "processing",
    "abandonment": "ux",
    "volume": "ux",
    "refund": "refunds",
}


def score_components(components: pl.LazyFrame) -> pl.LazyFrame:
    """Turn the component inputs into component scores, the health score, label and diagnosis.

    Formulas:
        ``score_<c> = clip((input - bad) / (good - bad) * 100, 0, 100)``, or 50 when the input
            is null
        ``health_score = sum(weight_c * score_c)``
        ``label = "insufficient_data" if attempts < 50, "at_risk" if health_score < 50, else
            "healthy"``
        ``main_driver = argmax_c(weight_c * (100 - score_c))``
        ``diagnosis`` = the ``DIAGNOSIS`` of ``main_driver``

    Args:
        components: One row per merchant with ``attempts`` and the input column of every
            component (``auth_gap``, ``abandonment_gap``, ``volume_trend``, ``failure_rate``,
            ``refund_rate``).

    Returns:
        The input columns plus one ``score_<component>`` per component, ``health_score``,
        ``label``, ``main_driver`` and ``diagnosis``.
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
        .with_columns(
            health.alias("health_score"),
            points_lost.list.arg_max()
            .replace_strict(dict(enumerate(names)), return_dtype=pl.String)
            .alias("main_driver"),
        )
        .with_columns(
            pl.when(pl.col("attempts") < MIN_ATTEMPTS)
            .then(pl.lit("insufficient_data"))
            .when(pl.col("health_score") < AT_RISK_THRESHOLD)
            .then(pl.lit("at_risk"))
            .otherwise(pl.lit("healthy"))
            .alias("label"),
            pl.col("main_driver")
            .replace_strict(DIAGNOSIS, return_dtype=pl.String)
            .alias("diagnosis"),
        )
    )


def _peer_gap(numerator: str, denominator: str) -> pl.Expr:
    """Build ``own rate - peer rate``, where peers are the other merchants of the peer group.

    Peers are pooled (sum of numerators / sum of denominators) over the country and category,
    leaving the merchant out. Without such a peer the country is used. Null when the merchant or
    its peers have no denominator.
    """

    def others(column: str, group: tuple[str, ...]) -> pl.Expr:
        return pl.col(column).sum().over(list(group)) - pl.col(column)

    use_category = others(denominator, PEER_GROUP) > 0
    peer_n = (
        pl.when(use_category)
        .then(others(denominator, PEER_GROUP))
        .otherwise(others(denominator, ("country",)))
    )
    peer_successes = (
        pl.when(use_category)
        .then(others(numerator, PEER_GROUP))
        .otherwise(others(numerator, ("country",)))
    )
    defined = (pl.col(denominator) > 0) & (peer_n > 0)
    own_rate = pl.col(numerator) / pl.col(denominator)
    return pl.when(defined).then(own_rate - peer_successes / peer_n).otherwise(None)


def merchant_health(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Score every merchant on its last 30 days and rank them, worst first.

    Grain: one row per ``merchant_id``.

    Inputs of the components, all over the 30 local days ending on the last date in ``fct``:
        ``auth_gap = auth_rate - peer auth_rate``
        ``abandonment_gap = abandonment_rate - peer abandonment_rate``
        ``failure_rate`` and ``refund_rate`` from ``analytics.metrics.outcome_rates``
        ``volume_trend = transactions_last_30d / transactions_prior_30d - 1``

    Args:
        fct: One row per transaction, with ``merchant_category`` and ``local_date``.

    Returns:
        ``merchant_id``, ``country``, ``merchant_category``, ``attempts``, ``auth_rate``,
        ``auth_gap``, ``abandonment_rate``, ``abandonment_gap``, ``failure_rate``,
        ``refund_rate``, ``transactions_last_30d``, ``transactions_prior_30d``,
        ``volume_trend`` and the columns added by ``score_components``, sorted by ascending
        ``health_score``.
    """
    keys = ["merchant_id", *PEER_GROUP]
    last_day = pl.col("local_date").max()
    recent_start = last_day - timedelta(days=WINDOW_DAYS)
    prior_start = last_day - timedelta(days=2 * WINDOW_DAYS)
    dated = fct.with_columns(
        (pl.col("local_date") > recent_start).alias("_recent"),
        ((pl.col("local_date") > prior_start) & (pl.col("local_date") <= recent_start)).alias(
            "_prior"
        ),
    )
    volume = (
        dated.group_by(keys)
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
    recent = dated.filter(pl.col("_recent"))
    rates = (
        outcome_rates(aggregate_additive_measures(recent, [*keys, "payment_method"]), keys)
        .with_columns(
            pl.when(pl.col("attempts") > 0)
            .then(pl.col("approved") / pl.col("attempts"))
            .otherwise(None)
            .alias("auth_rate"),
            _peer_gap("approved", "attempts").alias("auth_gap"),
            _peer_gap("abandoned", "resolved").alias("abandonment_gap"),
        )
        .select(
            "merchant_id",
            "attempts",
            "auth_rate",
            "auth_gap",
            "abandonment_rate",
            "abandonment_gap",
            "failure_rate",
            "refund_rate",
        )
    )
    components = (
        volume.join(rates, on="merchant_id", how="left")
        .with_columns(pl.col("attempts").fill_null(0))
        .select(
            *keys,
            "attempts",
            "auth_rate",
            "auth_gap",
            "abandonment_rate",
            "abandonment_gap",
            "failure_rate",
            "refund_rate",
            "transactions_last_30d",
            "transactions_prior_30d",
            "volume_trend",
        )
    )
    return score_components(components).sort("health_score", "merchant_id")
