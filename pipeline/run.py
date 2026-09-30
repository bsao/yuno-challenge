"""Pipeline orchestrator and CLI entry point.

Purpose: run every pipeline stage in order: generate raw data when it is missing, ingest raw
    events into staging, transform staging into marts, then run the data quality checks.
Inputs: command line arguments and the files under ``<data_dir>/raw``.
Outputs: raw files when they were missing, Parquet files under ``<data_dir>/staging`` and
    ``<data_dir>/marts``, and one log line per stage.

The generate and ingest stages are implemented (ingest runs its own quality assertions); the
transform and quality stages log that they are not implemented yet.
"""

import argparse
import logging
from collections.abc import Sequence
from dataclasses import replace
from pathlib import Path

from data_gen.generate import RAW_FILES, GeneratorConfig, generate
from pipeline.ingest import ingest

logger = logging.getLogger(__name__)

STAGES: tuple[str, ...] = ("generate", "ingest", "transform", "quality")
DEFAULT_DATA_DIR = Path("data")


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
    raw_dir = data_dir / "raw"
    missing = [name for name in RAW_FILES if not (raw_dir / name).exists()]
    if missing:
        logger.info("stage=generate status=started missing_files=%d", len(missing))
        generate(replace(generator_config or GeneratorConfig(), output_dir=raw_dir))
        logger.info("stage=generate status=done")
    else:
        logger.info("stage=generate status=skipped reason=raw_data_present")

    logger.info("stage=ingest status=started")
    ingest(raw_dir, data_dir / "staging")
    logger.info("stage=ingest status=done")

    for stage in STAGES[2:]:
        logger.info("stage=%s status=skipped reason=not_implemented", stage)
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
