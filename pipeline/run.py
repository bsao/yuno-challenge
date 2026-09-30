"""Pipeline orchestrator and CLI entry point.

Purpose: run every pipeline stage in order: generate raw data when it is missing, ingest raw
    events into staging, transform staging into marts, then run the data quality checks.
Inputs: command line arguments and the files under ``data/raw``.
Outputs: Parquet files under ``data/staging`` and ``data/marts`` plus one log line per stage.

The stages are stubs in this scaffold: each one only logs that it is not implemented yet.
"""

import argparse
import logging
from collections.abc import Sequence

logger = logging.getLogger(__name__)

STAGES: tuple[str, ...] = ("generate", "ingest", "transform", "quality")


def main(argv: Sequence[str] | None = None) -> int:
    """Run all pipeline stages in order.

    Args:
        argv: Command line arguments without the program name. Defaults to ``sys.argv[1:]``.

    Returns:
        The process exit code, 0 on success.

    Raises:
        SystemExit: If the arguments cannot be parsed.
    """
    parser = argparse.ArgumentParser(description="Run the TiendaMax analytics pipeline.")
    parser.parse_args(argv)

    for stage in STAGES:
        logger.info("stage=%s status=skipped reason=not_implemented", stage)
    logger.info("pipeline finished stages=%d", len(STAGES))
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
