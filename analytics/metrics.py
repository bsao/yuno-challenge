"""Metric definitions.

Purpose: the single home of every rate and GMV formula, so the dashboard, the analyses and the
    logs can never disagree on a definition.
Inputs: lazy frames holding the additive measures produced by
    ``pipeline.transform.aggregate_additive_measures`` (``n_approved``, ``n_refunded``,
    ``n_declined``, ``n_failed``, ``n_expired``, ``approved_amount_usd``, ``refunded_amount_usd``)
    plus ``payment_method`` and any dimension columns, for example ``agg_daily.parquet``.
Outputs: lazy frames with one row per combination of the requested dimensions, where every rate
    comes with its sample size and a Wilson 95% interval.

Definitions (documented in ``docs/DECISIONS.md``):
    * ``approved = n_approved + n_refunded``: a refunded transaction was authorized first.
    * ``attempts = approved + n_declined + n_failed``: pending and expired are excluded.
    * ``auth_rate = approved / attempts``.
    * ``gmv_usd = approved_amount_usd + refunded_amount_usd`` (gross).
    * ``net_gmv_usd = gmv_usd - refunded_amount_usd``.
    * ``completion_rate = paid / (paid + expired)`` for voucher methods only, where
      ``paid = n_approved + n_refunded``.
    * ``decline_rate = n_declined / attempts`` and ``failure_rate = n_failed / attempts``: the
      two ways an attempt is not approved (issuer or risk refusal versus technical error).
    * ``expiration_rate = expired / (paid + expired)`` for voucher methods only, the complement
      of ``completion_rate``.
    * ``refund_rate = n_refunded / approved``: the share of authorized transactions later refunded.
    * A rate with a zero denominator is null, never 0 or NaN.
"""

import math
from collections.abc import Sequence

import polars as pl

# Two sided 95% confidence.
WILSON_Z = 1.96
VOUCHER_METHODS: tuple[str, ...] = ("oxxo", "boleto")
# Final statuses that count as an authorization attempt, split by outcome.
APPROVED_STATUSES: tuple[str, ...] = ("approved", "refunded")
NOT_APPROVED_STATUSES: tuple[str, ...] = ("declined", "failed")
ATTEMPT_STATUSES: tuple[str, ...] = APPROVED_STATUSES + NOT_APPROVED_STATUSES


def wilson_interval(successes: int, n: int, z: float = WILSON_Z) -> tuple[float, float]:
    """Compute the Wilson score interval for a proportion (scalar reference implementation).

    Formula, with ``p = successes / n``:
        ``center = (p + z**2 / (2 * n)) / (1 + z**2 / n)``
        ``half = z * sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / (1 + z**2 / n)``
        ``low = max(0, center - half)``, ``high = min(1, center + half)``

    Unlike the normal approximation it stays inside [0, 1] and remains informative for small
    samples and for rates of exactly 0 or 1.

    Args:
        successes: Number of successes.
        n: Number of trials.
        z: Standard normal quantile; 1.96 gives a 95% interval.

    Returns:
        The ``(low, high)`` bounds.

    Raises:
        ValueError: If ``n`` is not positive or ``successes`` is outside ``[0, n]``.
    """
    if n <= 0 or not 0 <= successes <= n:
        raise ValueError(f"invalid proportion: successes={successes} n={n}")
    p = successes / n
    denominator = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denominator
    half = z * math.sqrt(p * (1 - p) / n + z**2 / (4 * n**2)) / denominator
    return max(0.0, center - half), min(1.0, center + half)


def rate_expr(successes: pl.Expr, n: pl.Expr) -> pl.Expr:
    """Build the expression ``successes / n``, null when ``n`` is zero.

    Args:
        successes: Expression holding the number of successes.
        n: Expression holding the number of trials.

    Returns:
        A Float64 expression.
    """
    return pl.when(n > 0).then(successes / n).otherwise(None)


def wilson_interval_expr(
    successes: pl.Expr, n: pl.Expr, z: float = WILSON_Z
) -> tuple[pl.Expr, pl.Expr]:
    """Build the Wilson score interval as Polars expressions.

    Same formula as ``wilson_interval``. Both bounds are null when ``n`` is zero.

    Args:
        successes: Expression holding the number of successes.
        n: Expression holding the number of trials.
        z: Standard normal quantile; 1.96 gives a 95% interval.

    Returns:
        The ``(low, high)`` expressions.
    """
    p = successes / n
    denominator = 1 + z**2 / n
    center = (p + z**2 / (2 * n)) / denominator
    half = z * (p * (1 - p) / n + z**2 / (4 * n**2)).sqrt() / denominator
    has_trials = n > 0
    low = pl.when(has_trials).then((center - half).clip(0.0, 1.0)).otherwise(None)
    high = pl.when(has_trials).then((center + half).clip(0.0, 1.0)).otherwise(None)
    return low, high


def performance(lf: pl.LazyFrame, dims: Sequence[str]) -> pl.LazyFrame:
    """Compute payment performance per combination of ``dims``.

    Grain: one row per combination of ``dims`` (a single row when ``dims`` is empty).

    Formulas:
        ``approved = sum(n_approved) + sum(n_refunded)``
        ``attempts = approved + sum(n_declined) + sum(n_failed)``
        ``auth_rate = approved / attempts``
        ``wilson_low, wilson_high`` = Wilson 95% interval of ``approved`` out of ``attempts``
        ``gmv_usd = sum(approved_amount_usd) + sum(refunded_amount_usd)``
        ``net_gmv_usd = gmv_usd - sum(refunded_amount_usd)``
        ``voucher_attempts = paid + expired`` over voucher method rows only, where
            ``paid = n_approved + n_refunded``
        ``completion_rate = paid / voucher_attempts``, with its own Wilson 95% interval in
            ``completion_wilson_low`` and ``completion_wilson_high``; null when the group holds
            no resolved voucher transaction.

    Args:
        lf: Frame with the additive measures described in the module docstring, a
            ``payment_method`` column and the ``dims`` columns.
        dims: Columns to group by; may be empty for a grand total.

    Returns:
        ``dims`` followed by ``attempts``, ``approved``, ``auth_rate``, ``wilson_low``,
        ``wilson_high``, ``gmv_usd``, ``net_gmv_usd``, ``voucher_attempts``, ``voucher_paid``,
        ``completion_rate``,
        ``completion_wilson_low`` and ``completion_wilson_high``, sorted by ``dims``.
    """
    is_voucher = pl.col("payment_method").is_in(VOUCHER_METHODS)
    paid = pl.col("n_approved") + pl.col("n_refunded")
    sums = [
        paid.sum().alias("approved"),
        (pl.col("n_declined") + pl.col("n_failed")).sum().alias("_not_approved"),
        pl.col("approved_amount_usd").sum().alias("net_gmv_usd"),
        pl.col("refunded_amount_usd").sum().alias("_refunded_usd"),
        paid.filter(is_voucher).sum().alias("_voucher_paid"),
        pl.col("n_expired").filter(is_voucher).sum().alias("_voucher_expired"),
    ]
    aggregated = lf.group_by(dims).agg(sums).sort(dims) if dims else lf.select(sums)

    approved = pl.col("approved")
    attempts = approved + pl.col("_not_approved")
    wilson_low, wilson_high = wilson_interval_expr(approved, attempts)
    voucher_paid = pl.col("_voucher_paid")
    voucher_attempts = voucher_paid + pl.col("_voucher_expired")
    completion_low, completion_high = wilson_interval_expr(voucher_paid, voucher_attempts)
    return aggregated.select(
        *dims,
        attempts.alias("attempts"),
        approved,
        rate_expr(approved, attempts).alias("auth_rate"),
        wilson_low.alias("wilson_low"),
        wilson_high.alias("wilson_high"),
        (pl.col("net_gmv_usd") + pl.col("_refunded_usd")).alias("gmv_usd"),
        pl.col("net_gmv_usd"),
        voucher_attempts.alias("voucher_attempts"),
        voucher_paid.alias("voucher_paid"),
        rate_expr(voucher_paid, voucher_attempts).alias("completion_rate"),
        completion_low.alias("completion_wilson_low"),
        completion_high.alias("completion_wilson_high"),
    )


def outcome_rates(lf: pl.LazyFrame, dims: Sequence[str]) -> pl.LazyFrame:
    """Compute how attempts fail, per combination of ``dims``.

    Grain: one row per combination of ``dims`` (a single row when ``dims`` is empty).

    Formulas:
        ``attempts = sum(n_approved) + sum(n_refunded) + sum(n_declined) + sum(n_failed)``
        ``decline_rate = sum(n_declined) / attempts``
        ``failure_rate = sum(n_failed) / attempts``
        ``voucher_attempts = paid + expired`` over voucher method rows only, where
            ``paid = n_approved + n_refunded``
        ``expiration_rate = expired / voucher_attempts``
        ``refund_rate = sum(n_refunded) / approved``, where
            ``approved = sum(n_approved) + sum(n_refunded)``
        Each rate has a Wilson 95% interval in ``<rate>_wilson_low`` and ``<rate>_wilson_high``.

    Args:
        lf: Frame with the additive measures described in the module docstring, a
            ``payment_method`` column and the ``dims`` columns.
        dims: Columns to group by; may be empty for a grand total.

    Returns:
        ``dims`` followed by ``attempts``, ``declined``, ``decline_rate``, ``failed``,
        ``failure_rate``, ``voucher_attempts``, ``expired``, ``expiration_rate``, ``approved``,
        ``refunded``, ``refund_rate`` and the Wilson bounds of the four rates, sorted by ``dims``.
    """
    is_voucher = pl.col("payment_method").is_in(VOUCHER_METHODS)
    paid = pl.col("n_approved") + pl.col("n_refunded")
    sums = [
        (paid + pl.col("n_declined") + pl.col("n_failed")).sum().alias("attempts"),
        pl.col("n_declined").sum().alias("declined"),
        pl.col("n_failed").sum().alias("failed"),
        (paid + pl.col("n_expired")).filter(is_voucher).sum().alias("voucher_attempts"),
        pl.col("n_expired").filter(is_voucher).sum().alias("expired"),
        paid.sum().alias("approved"),
        pl.col("n_refunded").sum().alias("refunded"),
    ]
    aggregated = lf.group_by(dims).agg(sums).sort(dims) if dims else lf.select(sums)

    columns: list[pl.Expr] = []
    for successes, n, name in (
        ("declined", "attempts", "decline_rate"),
        ("failed", "attempts", "failure_rate"),
        ("expired", "voucher_attempts", "expiration_rate"),
        ("refunded", "approved", "refund_rate"),
    ):
        low, high = wilson_interval_expr(pl.col(successes), pl.col(n))
        columns += [
            rate_expr(pl.col(successes), pl.col(n)).alias(name),
            low.alias(f"{name}_wilson_low"),
            high.alias(f"{name}_wilson_high"),
        ]
    return aggregated.with_columns(columns).select(
        *dims,
        "attempts",
        "declined",
        pl.col("^decline_rate.*$"),
        "failed",
        pl.col("^failure_rate.*$"),
        "voucher_attempts",
        "expired",
        pl.col("^expiration_rate.*$"),
        "approved",
        "refunded",
        pl.col("^refund_rate.*$"),
    )
