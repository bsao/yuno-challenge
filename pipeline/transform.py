"""Staging to marts transformation.

Purpose: publish the analytical tables the dashboard and the analyses read.
Inputs: ``<staging_dir>/transactions.parquet`` (one row per transaction) and
    ``<raw_dir>/merchants.csv`` (one row per merchant).
Outputs:
    ``<marts_dir>/fct_transactions.parquet``: one row per ``transaction_id``, the staging columns
        plus ``local_date``, ``merchant_category`` and ``merchant_size_tier``.
    ``<marts_dir>/agg_daily.parquet``: one row per ``date`` x ``country`` x ``psp`` x
        ``payment_method``, holding additive measures only.

Assumptions:
    * ``date`` is the local calendar date on which the transaction was created, in the time zone
      of its country. Transactions are never re-dated when their status changes.
    * The aggregate stores only additive measures (status counts and amount sums). Rates are not
      additive, so they are never stored: ``analytics.metrics`` derives them at read time from
      these columns, which keeps every rollup correct and keeps metric definitions out of the
      pipeline.
    * Every transaction has a merchant in ``merchants.csv`` (asserted).
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pipeline import quality

logger = logging.getLogger(__name__)

STAGING_FILE = "transactions.parquet"
MERCHANTS_FILE = "merchants.csv"
FCT_FILE = "fct_transactions.parquet"
AGG_DAILY_FILE = "agg_daily.parquet"

AGG_DAILY_GRAIN: tuple[str, ...] = ("date", "country", "psp", "payment_method")
STATUSES: tuple[str, ...] = ("approved", "declined", "failed", "expired", "pending", "refunded")
# Floating point sums taken in a different order can differ in the last digits.
USD_SUM_TOLERANCE = 0.01


@dataclass(frozen=True)
class TransformStats:
    """Counts describing one transform run.

    Attributes:
        fct_rows: Rows written to ``fct_transactions.parquet``.
        agg_daily_rows: Rows written to ``agg_daily.parquet``.
    """

    fct_rows: int
    agg_daily_rows: int


def build_fct_transactions(staging: pl.LazyFrame, merchants: pl.LazyFrame) -> pl.LazyFrame:
    """Enrich staging transactions with the local date and merchant attributes.

    Grain: one row per ``transaction_id`` (unchanged from staging; the join to merchants is many
    to one).

    Args:
        staging: One row per transaction, as written by ``pipeline.ingest``.
        merchants: One row per merchant with ``merchant_id``, ``category`` and ``size_tier``.

    Returns:
        The staging columns plus ``local_date``, ``merchant_category`` and ``merchant_size_tier``
        (null when the merchant is unknown).
    """
    merchant_attributes = merchants.select(
        "merchant_id",
        pl.col("category").alias("merchant_category"),
        pl.col("size_tier").alias("merchant_size_tier"),
    )
    return staging.with_columns(pl.col("created_at_local").dt.date().alias("local_date")).join(
        merchant_attributes, on="merchant_id", how="left", validate="m:1"
    )


def aggregate_additive_measures(transactions: pl.LazyFrame, dims: Sequence[str]) -> pl.LazyFrame:
    """Aggregate transactions into additive status counts and amount sums.

    Grain: one row per combination of ``dims``.

    Measures:
        ``n_transactions``: all transactions.
        ``n_<status>``: transactions whose ``final_status`` is that status, for each status.
        ``approved_amount_usd``: sum of ``amount_usd`` over ``approved`` transactions.
        ``refunded_amount_usd``: sum of ``amount_usd`` over ``refunded`` transactions.

    Args:
        transactions: One row per transaction with ``final_status``, ``amount_usd`` and ``dims``.
        dims: Columns to group by.

    Returns:
        One row per combination of ``dims`` with the measures above, sorted by ``dims``.
    """
    status = pl.col("final_status")
    return (
        transactions.group_by(dims)
        .agg(
            pl.len().alias("n_transactions"),
            *[(status == name).sum().alias(f"n_{name}") for name in STATUSES],
            pl.col("amount_usd").filter(status == "approved").sum().alias("approved_amount_usd"),
            pl.col("amount_usd").filter(status == "refunded").sum().alias("refunded_amount_usd"),
        )
        .sort(dims)
    )


def build_agg_daily(fct: pl.LazyFrame) -> pl.LazyFrame:
    """Aggregate the fact table to the daily performance grain.

    Grain: one row per ``date`` x ``country`` x ``psp`` x ``payment_method``, where ``date`` is
    the local creation date.

    Args:
        fct: One row per transaction, as returned by ``build_fct_transactions``.

    Returns:
        The additive measures of ``aggregate_additive_measures`` at the daily grain.
    """
    return aggregate_additive_measures(fct.rename({"local_date": "date"}), AGG_DAILY_GRAIN)


def transform(staging_dir: Path, raw_dir: Path, marts_dir: Path) -> TransformStats:
    """Build both marts from staging and validate them.

    Quality assertions: unique ``transaction_id`` in the fact table; fact rows equal staging
    rows; no transaction without merchant attributes; unique grain in the aggregate; and the
    aggregate's transaction count, per status counts and USD sums reconcile with the fact table.

    Args:
        staging_dir: Directory holding ``transactions.parquet``.
        raw_dir: Directory holding ``merchants.csv``.
        marts_dir: Directory that receives the marts (created if missing).

    Returns:
        The row counts of the run.

    Raises:
        FileNotFoundError: If an input file does not exist.
        DataQualityError: If a quality assertion fails; nothing is written in that case.
    """
    for path in (staging_dir / STAGING_FILE, raw_dir / MERCHANTS_FILE):
        if not path.exists():
            raise FileNotFoundError(path)
    staging = pl.scan_parquet(staging_dir / STAGING_FILE)
    merchants = pl.scan_csv(raw_dir / MERCHANTS_FILE)

    fct = build_fct_transactions(staging, merchants).collect()
    agg_daily = build_agg_daily(fct.lazy()).collect()

    quality.check_unique(fct, "transaction_id")
    quality.check_totals_match(
        "fct_rows_vs_staging_rows", staging.select(pl.len()).collect().item(), fct.height
    )
    quality.check_totals_match(
        "transactions_without_merchant", 0, fct.get_column("merchant_category").null_count()
    )
    quality.check_unique(agg_daily, AGG_DAILY_GRAIN)
    quality.check_totals_match(
        "agg_daily_n_transactions", fct.height, int(agg_daily.get_column("n_transactions").sum())
    )
    status_counts = dict(fct.get_column("final_status").value_counts().iter_rows())
    for name in STATUSES:
        quality.check_totals_match(
            f"agg_daily_n_{name}",
            status_counts.get(name, 0),
            int(agg_daily.get_column(f"n_{name}").sum()),
        )
    for name in ("approved", "refunded"):
        quality.check_totals_match(
            f"agg_daily_{name}_amount_usd",
            float(fct.filter(pl.col("final_status") == name).get_column("amount_usd").sum()),
            float(agg_daily.get_column(f"{name}_amount_usd").sum()),
            tolerance=USD_SUM_TOLERANCE,
        )

    marts_dir.mkdir(parents=True, exist_ok=True)
    fct.write_parquet(marts_dir / FCT_FILE)
    agg_daily.write_parquet(marts_dir / AGG_DAILY_FILE)
    logger.info(
        "transform finished fct_rows=%d agg_daily_rows=%d output_dir=%s",
        fct.height,
        agg_daily.height,
        marts_dir,
    )
    return TransformStats(fct_rows=fct.height, agg_daily_rows=agg_daily.height)
