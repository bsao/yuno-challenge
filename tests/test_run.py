"""Tests for the pipeline runner.

Purpose: confirm the runner executes every stage in order and generates raw data only when it is
    missing.
Inputs: a small generated dataset in a temporary directory.
Outputs: pytest assertions.
"""

import logging
from pathlib import Path

import pytest

from data_gen.generate import RAW_FILES, GeneratorConfig
from pipeline.run import STAGES, run_pipeline

# Large enough for the status mix guardrails to hold with a wide margin.
SMALL = GeneratorConfig(n_transactions=5_000)


def _logged_stages(caplog: pytest.LogCaptureFixture) -> list[tuple[str, str]]:
    """Return (stage, status) pairs in log order, keeping the last status of each stage."""
    last: dict[str, str] = {}
    for record in caplog.records:
        message = record.getMessage()
        for stage in STAGES:
            if message.startswith(f"stage={stage} "):
                last[stage] = message.split("status=")[1].split(" ")[0]
    return list(last.items())


def test_generates_raw_data_when_missing(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """The first run writes every raw file and logs the stages in pipeline order."""
    with caplog.at_level(logging.INFO, logger="pipeline.run"):
        run_pipeline(tmp_path, SMALL)

    for name in RAW_FILES:
        assert (tmp_path / "raw" / name).is_file()
    assert (tmp_path / "staging" / "transactions.parquet").is_file()
    assert _logged_stages(caplog) == [
        ("generate", "done"),
        ("ingest", "done"),
        ("transform", "skipped"),
        ("quality", "skipped"),
    ]


def test_skips_generation_when_raw_data_exists(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A second run leaves the existing raw files untouched."""
    run_pipeline(tmp_path, SMALL)
    webhooks = tmp_path / "raw" / "webhooks.jsonl"
    before = (webhooks.stat().st_mtime_ns, webhooks.read_bytes())

    with caplog.at_level(logging.INFO, logger="pipeline.run"):
        run_pipeline(tmp_path, GeneratorConfig(n_transactions=9_000, seed=7))

    assert (webhooks.stat().st_mtime_ns, webhooks.read_bytes()) == before
    assert _logged_stages(caplog)[0] == ("generate", "skipped")


def test_regenerates_when_one_raw_file_is_missing(tmp_path: Path) -> None:
    """Removing any raw file triggers a fresh generation."""
    run_pipeline(tmp_path, SMALL)
    (tmp_path / "raw" / "psp_fees.csv").unlink()

    run_pipeline(tmp_path, SMALL)

    assert (tmp_path / "raw" / "psp_fees.csv").is_file()
