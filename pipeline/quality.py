"""Data quality assertions.

Purpose: validate the output of every pipeline stage and fail loudly on violations, so a bad
    batch never reaches the marts or the dashboard.
Inputs: Polars frames produced by the pipeline stages, plus the counts they must reconcile with.
Outputs: nothing on success (one log line per check); ``DataQualityError`` on the first violation.

Assumptions: the checks are generic. Vocabularies and expected ranges are passed in by the stage
    that owns the contract, except ``EXPECTED_STATUS_SHARE``, the guardrail for the status mix.
"""

import logging
from collections.abc import Collection, Mapping, Sequence

import polars as pl

logger = logging.getLogger(__name__)

# Guardrails for the share of each final status, as (minimum, maximum). They are deliberately
# wide: their job is to catch a broken batch (for example every transaction stuck in "pending"),
# not to police normal variation.
EXPECTED_STATUS_SHARE: dict[str, tuple[float, float]] = {
    "approved": (0.55, 0.90),
    "declined": (0.08, 0.30),
    "failed": (0.005, 0.08),
    "expired": (0.0, 0.10),
    "pending": (0.0, 0.05),
    "refunded": (0.0, 0.05),
}


class DataQualityError(Exception):
    """Raised when a data quality assertion fails."""


def check_unique(frame: pl.DataFrame, columns: str | Sequence[str]) -> None:
    """Assert that the key formed by ``columns`` holds no duplicated value.

    Args:
        frame: Frame to validate.
        columns: Name of the key column, or the names forming a composite key (the grain).

    Raises:
        DataQualityError: If at least one key appears more than once.
    """
    key = [columns] if isinstance(columns, str) else list(columns)
    name = ", ".join(key)
    duplicated = frame.height - frame.select(key).n_unique()
    if duplicated:
        raise DataQualityError(f"{name} is not unique: {duplicated} duplicated rows")
    logger.info("check=unique column=%s status=passed rows=%d", name, frame.height)


def check_totals_match(name: str, expected: float, actual: float, tolerance: float = 0.0) -> None:
    """Assert that a total computed at one grain equals the same total at another grain.

    Args:
        name: Label of the total, used in the log line and the error message.
        expected: Total at the source grain.
        actual: Total at the derived grain.
        tolerance: Accepted absolute difference, for floating point sums.

    Raises:
        DataQualityError: If the totals differ by more than ``tolerance``.
    """
    if abs(expected - actual) > tolerance:
        raise DataQualityError(f"{name} does not reconcile: expected={expected} actual={actual}")
    logger.info("check=totals_match name=%s status=passed value=%s", name, actual)


def check_enum_values(
    frame: pl.DataFrame,
    allowed: Mapping[str, Collection[str]],
    nullable: Collection[str] = (),
) -> None:
    """Assert that every column only holds values from its vocabulary.

    Args:
        frame: Frame to validate.
        allowed: Accepted values per column name.
        nullable: Columns where null is also accepted. Null is invalid everywhere else.

    Raises:
        DataQualityError: If a column holds a value outside its vocabulary.
    """
    for column, values in allowed.items():
        series = frame.get_column(column)
        invalid = ~series.is_in(list(values))
        invalid = invalid.fill_null(column not in nullable)
        if invalid.any():
            examples = series.filter(invalid).unique().head(5).to_list()
            raise DataQualityError(
                f"{column} has {invalid.sum()} invalid values, for example {examples}"
            )
    logger.info("check=enum_values status=passed columns=%d", len(allowed))


def check_positive_amounts(frame: pl.DataFrame, column: str) -> None:
    """Assert that every amount is present and strictly positive.

    Args:
        frame: Frame to validate.
        column: Name of the amount column.

    Raises:
        DataQualityError: If an amount is null, zero or negative.
    """
    series = frame.get_column(column)
    invalid = int((series <= 0).fill_null(True).sum())
    if invalid:
        raise DataQualityError(f"{column} has {invalid} null, zero or negative values")
    logger.info("check=positive_amounts column=%s status=passed", column)


def check_status_mix(
    frame: pl.DataFrame,
    column: str,
    expected: Mapping[str, tuple[float, float]] = EXPECTED_STATUS_SHARE,
) -> None:
    """Assert that the share of each status lies within its expected range.

    Formula: ``share(status) = rows with that status / all rows``.

    Args:
        frame: Frame to validate, one row per transaction.
        column: Name of the status column.
        expected: Inclusive ``(minimum, maximum)`` share per status.

    Raises:
        DataQualityError: If the frame is empty or a share is outside its range.
    """
    if frame.height == 0:
        raise DataQualityError("status mix cannot be checked on an empty frame")
    counts = dict(frame.get_column(column).value_counts().iter_rows())
    for status, (minimum, maximum) in expected.items():
        share = counts.get(status, 0) / frame.height
        if not minimum <= share <= maximum:
            raise DataQualityError(
                f"share of {status} is {share:.2%}, outside [{minimum:.2%}, {maximum:.2%}]"
            )
    logger.info("check=status_mix column=%s status=passed", column)


def check_row_count_reconciliation(
    *,
    raw_rows: int,
    duplicate_deliveries: int,
    unique_events: int,
    events_in_staging: int,
    raw_transactions: int,
    staging_rows: int,
) -> None:
    """Assert that no event or transaction was lost or invented between raw and staging.

    Two identities must hold:
        ``raw_rows - duplicate_deliveries == unique_events == events_in_staging``
        ``raw_transactions == staging_rows``

    Args:
        raw_rows: Rows read from the raw file.
        duplicate_deliveries: Rows dropped because their ``event_id`` was already seen.
        unique_events: Distinct ``event_id`` values in the raw file.
        events_in_staging: Sum of the per transaction event counts in staging.
        raw_transactions: Distinct ``transaction_id`` values in the raw file.
        staging_rows: Rows in staging.

    Raises:
        DataQualityError: If either identity does not hold.
    """
    if not raw_rows - duplicate_deliveries == unique_events == events_in_staging:
        raise DataQualityError(
            f"event counts do not reconcile: raw_rows={raw_rows} "
            f"duplicate_deliveries={duplicate_deliveries} unique_events={unique_events} "
            f"events_in_staging={events_in_staging}"
        )
    if raw_transactions != staging_rows:
        raise DataQualityError(
            f"transaction counts do not reconcile: raw={raw_transactions} staging={staging_rows}"
        )
    logger.info(
        "check=row_count_reconciliation status=passed raw_rows=%d unique_events=%d transactions=%d",
        raw_rows,
        unique_events,
        staging_rows,
    )
