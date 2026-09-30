"""PSP cost analysis.

Purpose: price each PSP per successful transaction and estimate what moving traffic between PSPs
    would save.
Inputs: ``agg_daily`` (one row per date x country x psp x payment_method) and the fee schedule
    ``psp_fees.csv`` (one row per psp x country x payment_method with ``pct_fee`` in percent of
    the amount and ``fixed_fee_usd``), both as lazy frames.
Outputs: lazy frames, one row per psp x country x payment_method for the cost view and one row
    per country x payment_method for the simulation.

Fee model (an assumption, documented in ``docs/DECISIONS.md``):
    * The percentage fee is charged on the amount of successful transactions (approved or later
      refunded; the fee is not returned on a refund).
    * The fixed fee is charged on every authorization attempt, successful or not.
    * So ``total_fees_usd = pct_fee / 100 * gmv_usd + fixed_fee_usd * attempts`` and
      ``cost_per_success_usd = total_fees_usd / approved``. A PSP that fails more pays its fixed
      fee more often per success.

Counts, rates and GMV come from ``analytics.metrics.performance``.

Assumptions of the simulation:
    * PSPs are compared like for like: only the days on which every PSP of the segment was live.
    * Authorization rates are held at their observed values, and moved traffic keeps the average
      ticket of the PSP it leaves.
    * Results are scaled to a 30 day month from the days observed.
"""

from typing import Literal

import polars as pl

from analytics.metrics import performance

SEGMENT: tuple[str, ...] = ("country", "payment_method")
SHIFT_SHARE = 0.20
DAYS_PER_MONTH = 30
RankBy = Literal["cost_per_success_usd", "auth_rate"]


def cost_per_successful_transaction(agg_daily: pl.LazyFrame, fees: pl.LazyFrame) -> pl.LazyFrame:
    """Compute the fees paid per successful transaction for each PSP, country and method.

    Grain: one row per ``psp`` x ``country`` x ``payment_method``.

    Formulas:
        ``total_fees_usd = pct_fee / 100 * gmv_usd + fixed_fee_usd * attempts``
        ``cost_per_success_usd = total_fees_usd / approved``
        ``avg_ticket_usd = gmv_usd / approved``
        ``fee_share_of_gmv = total_fees_usd / gmv_usd``

    Args:
        agg_daily: The daily aggregate mart (or a filtered part of it).
        fees: The fee schedule, one row per ``psp`` x ``country`` x ``payment_method``.

    Returns:
        ``psp``, ``country``, ``payment_method``, ``attempts``, ``approved``, ``auth_rate``,
        ``wilson_low``, ``wilson_high``, ``gmv_usd``, ``avg_ticket_usd``, ``pct_fee``,
        ``fixed_fee_usd``, ``total_fees_usd``, ``cost_per_success_usd`` and
        ``fee_share_of_gmv``. Rows without a fee or without a success have null costs.
    """
    keys = ["psp", *SEGMENT]
    total_fees = pl.col("pct_fee") / 100 * pl.col("gmv_usd") + pl.col("fixed_fee_usd") * pl.col(
        "attempts"
    )
    has_success = pl.col("approved") > 0
    return (
        performance(agg_daily, keys)
        .join(fees.select(*keys, "pct_fee", "fixed_fee_usd"), on=keys, how="left", validate="1:1")
        .with_columns(
            total_fees.alias("total_fees_usd"),
            pl.when(has_success)
            .then(pl.col("gmv_usd") / pl.col("approved"))
            .otherwise(None)
            .alias("avg_ticket_usd"),
        )
        .with_columns(
            pl.when(has_success)
            .then(pl.col("total_fees_usd") / pl.col("approved"))
            .otherwise(None)
            .alias("cost_per_success_usd"),
            pl.when(pl.col("gmv_usd") > 0)
            .then(pl.col("total_fees_usd") / pl.col("gmv_usd"))
            .otherwise(None)
            .alias("fee_share_of_gmv"),
        )
        .select(
            *keys,
            "attempts",
            "approved",
            "auth_rate",
            "wilson_low",
            "wilson_high",
            "gmv_usd",
            "avg_ticket_usd",
            "pct_fee",
            "fixed_fee_usd",
            "total_fees_usd",
            "cost_per_success_usd",
            "fee_share_of_gmv",
        )
        .sort(*SEGMENT, "psp")
    )


def like_for_like(agg_daily: pl.LazyFrame) -> pl.LazyFrame:
    """Keep only the days on which every PSP of a country and method segment was live.

    Args:
        agg_daily: The daily aggregate mart (or a filtered part of it).

    Returns:
        The rows dated on or after the latest first day of any PSP in their segment.
    """
    segment = list(SEGMENT)
    first_day = pl.col("date").min().over([*segment, "psp"])
    return agg_daily.filter(pl.col("date") >= first_day.max().over(segment))


def simulate_traffic_shift(
    agg_daily: pl.LazyFrame,
    fees: pl.LazyFrame,
    share: float = SHIFT_SHARE,
    rank_by: RankBy = "cost_per_success_usd",
) -> pl.LazyFrame:
    """Simulate moving a share of the worst PSP's traffic to the best PSP of each segment.

    Grain: one row per ``country`` x ``payment_method`` with at least two PSPs.

    "Worst" and "best" are the highest and lowest ``cost_per_success_usd`` (default), or the
    lowest and highest ``auth_rate``. With ``m`` the moved attempts per month and ``ticket`` the
    average ticket of the worst PSP:

        ``m = share * attempts_worst * 30 / days``
        ``approved_before = m * auth_worst``; ``approved_after = m * auth_best``
        ``fees_before = m * fixed_worst + pct_worst / 100 * approved_before * ticket``
        ``fees_after = m * fixed_best + pct_best / 100 * approved_after * ticket``
        ``monthly_savings_usd = fees_before - fees_after``
        ``monthly_approved_delta = approved_after - approved_before``
        ``monthly_gmv_delta_usd = monthly_approved_delta * ticket``

    Savings are fee savings only. A cheaper PSP with a lower authorization rate saves fees and
    loses sales, which ``monthly_gmv_delta_usd`` shows as a negative number.

    Args:
        agg_daily: The daily aggregate mart (or a filtered part of it).
        fees: The fee schedule.
        share: Share of the worst PSP's attempts to move, between 0 and 1.
        rank_by: Criterion that defines the worst and the best PSP.

    Returns:
        ``country``, ``payment_method``, ``days``, ``worst_psp``, ``best_psp``, their
        ``auth_rate`` and ``cost_per_success_usd``, ``moved_attempts_monthly``,
        ``monthly_approved_delta``, ``monthly_gmv_delta_usd``, ``fees_before_usd``,
        ``fees_after_usd`` and ``monthly_savings_usd``.

    Raises:
        ValueError: If ``share`` is not between 0 and 1.
    """
    if not 0 <= share <= 1:
        raise ValueError(f"share must be between 0 and 1, got {share}")
    segment = list(SEGMENT)
    comparable = like_for_like(agg_daily)
    days = comparable.group_by(segment).agg(pl.col("date").n_unique().alias("days"))
    costs = cost_per_successful_transaction(comparable, fees).filter(
        pl.col("cost_per_success_usd").is_not_null()
    )
    # Sorting so that the best PSP comes first and the worst last within each segment.
    best_first = costs.sort(rank_by, descending=rank_by == "auth_rate")
    fields = [
        "psp",
        "auth_rate",
        "cost_per_success_usd",
        "attempts",
        "avg_ticket_usd",
        "pct_fee",
        "fixed_fee_usd",
    ]
    pairs = (
        best_first.group_by(segment, maintain_order=True)
        .agg(
            *[pl.col(field).first().alias(f"best_{field}") for field in fields],
            *[pl.col(field).last().alias(f"worst_{field}") for field in fields],
            pl.len().alias("_psps"),
        )
        .filter(pl.col("_psps") >= 2)
        .join(days, on=segment, how="inner")
    )
    moved = share * pl.col("worst_attempts") * DAYS_PER_MONTH / pl.col("days")
    ticket = pl.col("worst_avg_ticket_usd")
    return (
        pairs.with_columns(moved.alias("moved_attempts_monthly"))
        .with_columns(
            (pl.col("moved_attempts_monthly") * pl.col("worst_auth_rate")).alias("_before"),
            (pl.col("moved_attempts_monthly") * pl.col("best_auth_rate")).alias("_after"),
        )
        .with_columns(
            (
                pl.col("moved_attempts_monthly") * pl.col("worst_fixed_fee_usd")
                + pl.col("worst_pct_fee") / 100 * pl.col("_before") * ticket
            ).alias("fees_before_usd"),
            (
                pl.col("moved_attempts_monthly") * pl.col("best_fixed_fee_usd")
                + pl.col("best_pct_fee") / 100 * pl.col("_after") * ticket
            ).alias("fees_after_usd"),
            (pl.col("_after") - pl.col("_before")).alias("monthly_approved_delta"),
        )
        .with_columns(
            (pl.col("monthly_approved_delta") * ticket).alias("monthly_gmv_delta_usd"),
            (pl.col("fees_before_usd") - pl.col("fees_after_usd")).alias("monthly_savings_usd"),
        )
        .select(
            *segment,
            "days",
            "worst_psp",
            "best_psp",
            "worst_auth_rate",
            "best_auth_rate",
            "worst_cost_per_success_usd",
            "best_cost_per_success_usd",
            "moved_attempts_monthly",
            "monthly_approved_delta",
            "monthly_gmv_delta_usd",
            "fees_before_usd",
            "fees_after_usd",
            "monthly_savings_usd",
        )
        .sort(segment)
    )
