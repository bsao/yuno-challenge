"""Smoke tests for the pipeline runner.

Purpose: confirm the CLI entry point runs every stage in the documented order.
Inputs: none (the stages are stubs and touch no data).
Outputs: pytest assertions.
"""

import logging

import pytest

from pipeline.run import STAGES, main


def test_main_returns_zero_and_logs_stages_in_order(caplog: pytest.LogCaptureFixture) -> None:
    """The runner exits with 0 and logs one line per stage, in pipeline order."""
    with caplog.at_level(logging.INFO, logger="pipeline.run"):
        exit_code = main([])

    assert exit_code == 0
    logged_stages = [
        stage for record in caplog.records for stage in STAGES if f"stage={stage}" in record.message
    ]
    assert logged_stages == ["generate", "ingest", "transform", "quality"]
