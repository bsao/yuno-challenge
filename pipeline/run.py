"""Pipeline orchestrator and CLI entry point.

Purpose: run every pipeline stage in order: generate raw data when it is missing, ingest raw
    events into staging, transform staging into marts, then log the headline findings.
Inputs: command line arguments and the files under ``<data_dir>/raw``.
Outputs: raw files when they were missing, Parquet files under ``<data_dir>/staging`` and
    ``<data_dir>/marts``, and log lines per stage, per quality check and per highlight.

Quality assertions run inside the ingest and transform stages, before each stage writes its
output, so a failed check stops the pipeline and leaves no partial file behind.
"""

import argparse
import logging
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path
from typing import Any

import polars as pl

from analytics.metrics import performance
from data_gen.generate import RAW_FILES, GeneratorConfig, generate
from pipeline.ingest import ingest
from pipeline.transform import AGG_DAILY_FILE, transform

logger = logging.getLogger(__name__)

STAGES: tuple[str, ...] = ("generate", "ingest", "transform", "quality")
DEFAULT_DATA_DIR = Path("data")
PSP_COMPARISON_COUNTRY = "CO"
PSP_COMPARISON_METHOD = "card"


def _describe(row: dict[str, Any], label: str) -> str:
    """Format one performance row as ``label auth_rate [interval] n=attempts``."""
    text = (
        f"{label} auth_rate={row['auth_rate']:.1%} "
        f"ci95=[{row['wilson_low']:.1%}, {row['wilson_high']:.1%}] n={row['attempts']}"
    )
    if row["completion_rate"] is not None:
        text += f" completion_rate={row['completion_rate']:.1%} n={row['voucher_attempts']}"
    return text


def log_highlights(marts_dir: Path) -> None:
    """Log the best and worst payment method per country and the Colombia card PSP comparison.

    Methods are ranked by authorization rate. The PSP comparison only uses the days on which
    every PSP of the segment was live, so PSPs launched mid window are compared like for like.

    Args:
        marts_dir: Directory holding ``agg_daily.parquet``.
    """
    agg_daily = pl.scan_parquet(marts_dir / AGG_DAILY_FILE)

    by_method = (
        performance(agg_daily, ["country", "payment_method"])
        .filter(pl.col("attempts") > 0)
        .sort("country", "auth_rate")
        .collect()
    )
    for (country,), rows in by_method.partition_by(
        "country", as_dict=True, maintain_order=True
    ).items():
        worst, best = rows.row(0, named=True), rows.row(-1, named=True)
        logger.info(
            "highlight=method_ranking country=%s best: %s | worst: %s",
            country,
            _describe(best, best["payment_method"]),
            _describe(worst, worst["payment_method"]),
        )

    segment = agg_daily.filter(
        (pl.col("country") == PSP_COMPARISON_COUNTRY)
        & (pl.col("payment_method") == PSP_COMPARISON_METHOD)
    )
    common_start = (
        segment.group_by("psp").agg(pl.col("date").min()).select(pl.col("date").max()).collect()
    ).item()
    if common_start is None:
        return
    by_psp = (
        performance(segment.filter(pl.col("date") >= common_start), ["psp"])
        .sort("auth_rate", descending=True)
        .collect()
    )
    for row in by_psp.iter_rows(named=True):
        logger.info(
            "highlight=psp_comparison country=%s payment_method=%s since=%s %s gmv_usd=%.0f",
            PSP_COMPARISON_COUNTRY,
            PSP_COMPARISON_METHOD,
            common_start,
            _describe(row, row["psp"]),
            row["gmv_usd"],
        )


def run_pipeline(data_dir: Path, generator_config: GeneratorConfig | None = None) -> None:
    """Run all pipeline stages in order.

    Raw data is generated only when at least one raw file is missing, so reruns reuse the
    existing dataset.

    Args:
        data_dir: Root data directory holding ``raw``, ``staging`` and ``marts``.
        generator_config: Generator parameters used when raw data is missing. Its output
            directory is always replaced by ``<data_dir>/raw``. Defaults to ``GeneratorConfig()``.

    Raises:
        ValueError: If the generator configuration is invalid.
        DataQualityError: If a data quality assertion fails.
    """
    raw_dir, staging_dir, marts_dir = data_dir / "raw", data_dir / "staging", data_dir / "marts"
    missing = [name for name in RAW_FILES if not (raw_dir / name).exists()]
    if missing:
        logger.info("stage=generate status=started missing_files=%d", len(missing))
        generate(replace(generator_config or GeneratorConfig(), output_dir=raw_dir))
        logger.info("stage=generate status=done")
    else:
        logger.info("stage=generate status=skipped reason=raw_data_present")

    logger.info("stage=ingest status=started")
    ingest(raw_dir, staging_dir)
    logger.info("stage=ingest status=done")

    logger.info("stage=transform status=started")
    transform(staging_dir, raw_dir, marts_dir)
    logger.info("stage=transform status=done")

    logger.info("stage=quality status=done detail=assertions_passed_in_ingest_and_transform")
    log_highlights(marts_dir)
    logger.info("pipeline finished stages=%d", len(STAGES))


def main(argv: Sequence[str] | None = None) -> int:
    """Run the pipeline from the command line.

    Args:
        argv: Command line arguments without the program name. Defaults to ``sys.argv[1:]``.

    Returns:
        The process exit code, 0 on success.

    Raises:
        SystemExit: If the arguments cannot be parsed.
    """
    parser = argparse.ArgumentParser(description="Run the TiendaMax analytics pipeline.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR)
    args = parser.parse_args(argv)
    run_pipeline(args.data_dir)
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
