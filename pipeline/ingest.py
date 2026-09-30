"""Raw to staging ingestion.

Purpose: turn the raw webhook deliveries into one clean, typed row per transaction, idempotently.
Inputs: ``<raw_dir>/webhooks.jsonl`` (one row per webhook delivery, in arrival order).
Outputs: ``<staging_dir>/transactions.parquet``, grain: one row per ``transaction_id`` holding
    its latest status, plus the ``IngestStats`` returned to the caller.

Assumptions:
    * Deliveries sharing an ``event_id`` are identical retries, so the first arrival is kept.
    * "Latest" means the highest ``event_at`` (event time), never arrival order. Ties break on
      the lifecycle rank of the status (pending < final < refunded), then on ``event_id``.
    * Transaction attributes (amount, merchant, method, PSP) never change between events.
    * The stage is a full refresh: it rebuilds staging from the whole raw file, so reruns and
      replayed or reordered deliveries produce the same output.
    * ``amount_usd`` uses the fixed rates in ``USD_PER_MAJOR_UNIT``. They are illustrative, not
      market rates, and exist only so cross country views can be summed.
    * Local time uses IANA time zones (Chile changes to daylight saving time inside the window),
      and is stored as a naive timestamp because one column cannot hold several zones.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from pipeline import quality

logger = logging.getLogger(__name__)

WEBHOOKS_FILE = "webhooks.jsonl"
STAGING_FILE = "transactions.parquet"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%.3fZ"

RAW_SCHEMA: dict[str, type[pl.DataType]] = {
    "event_id": pl.String,
    "transaction_id": pl.String,
    "merchant_id": pl.String,
    "country": pl.String,
    "currency": pl.String,
    "amount_minor": pl.Int64,
    "payment_method": pl.String,
    "card_brand": pl.String,
    "psp": pl.String,
    "status": pl.String,
    "decline_reason": pl.String,
    "created_at": pl.String,
    "event_at": pl.String,
}

COUNTRIES: tuple[str, ...] = ("MX", "CO", "CL")
CURRENCIES: tuple[str, ...] = ("MXN", "COP", "CLP")
PAYMENT_METHODS: tuple[str, ...] = ("card", "oxxo", "spei", "pse", "webpay")
CARD_BRANDS: tuple[str, ...] = ("visa", "mastercard")
PSPS: tuple[str, ...] = ("PSP_A", "PSP_B", "PSP_C", "PSP_D")
DECLINE_REASONS: tuple[str, ...] = (
    "insufficient_funds",
    "card_declined",
    "fraud_suspected",
    "expired",
    "processor_error",
    "network_timeout",
)
# Lifecycle rank used to break ties between events that share an ``event_at``.
STATUS_RANK: dict[str, int] = {
    "pending": 0,
    "approved": 1,
    "declined": 1,
    "failed": 1,
    "expired": 1,
    "refunded": 2,
}
UNKNOWN_STATUS_RANK = -1

TIME_ZONES: dict[str, str] = {
    "MX": "America/Mexico_City",
    "CO": "America/Bogota",
    "CL": "America/Santiago",
}
# Decimal places of each currency (CLP has none) and fixed, illustrative USD rates.
MINOR_UNIT_EXPONENT: dict[str, int] = {"MXN": 2, "COP": 2, "CLP": 0}
USD_PER_MAJOR_UNIT: dict[str, float] = {"MXN": 0.054, "COP": 0.00025, "CLP": 0.00105}

_LATEST_ORDER: tuple[str, ...] = ("event_at", "status_rank", "event_id")


@dataclass(frozen=True)
class IngestStats:
    """Counts describing one ingestion run.

    Attributes:
        raw_rows: Webhook deliveries read from the raw file.
        unique_events: Distinct events after deduplication by ``event_id``.
        duplicate_deliveries: Deliveries dropped as repeats (``raw_rows - unique_events``).
        out_of_order_events: Events that arrived after a later event of the same transaction.
        transactions: Rows written to staging.
    """

    raw_rows: int
    unique_events: int
    duplicate_deliveries: int
    out_of_order_events: int
    transactions: int


def read_raw_events(path: Path) -> pl.LazyFrame:
    """Scan the raw webhook file and type its columns.

    Grain: one row per webhook delivery, with ``arrival_index`` recording the line order.

    Args:
        path: Location of ``webhooks.jsonl``.

    Returns:
        A lazy frame with the raw columns, ``created_at`` and ``event_at`` parsed as UTC
        timestamps, and ``arrival_index``.

    Raises:
        FileNotFoundError: If ``path`` does not exist.
    """
    if not path.exists():
        raise FileNotFoundError(path)
    return (
        pl.scan_ndjson(path, schema=RAW_SCHEMA)
        .with_row_index("arrival_index")
        .with_columns(
            pl.col("created_at", "event_at").str.to_datetime(
                TIMESTAMP_FORMAT, time_unit="ms", time_zone="UTC"
            )
        )
    )


def deduplicate_events(deliveries: pl.LazyFrame) -> pl.LazyFrame:
    """Drop repeated deliveries of the same event.

    Grain: one row per ``event_id`` (the first arrival is kept).

    Args:
        deliveries: One row per delivery, with ``event_id`` and ``arrival_index``.

    Returns:
        The deliveries without repeats, in arrival order.
    """
    return deliveries.sort("arrival_index").unique(
        subset="event_id", keep="first", maintain_order=True
    )


def count_out_of_order(events: pl.LazyFrame) -> int:
    """Count events that arrived after a later event of the same transaction.

    Formula: an event is out of order when its ``event_at`` is lower than the highest
    ``event_at`` seen earlier, in arrival order, for the same ``transaction_id``.

    Args:
        events: One row per ``event_id``, with ``transaction_id``, ``event_at`` and
            ``arrival_index``.

    Returns:
        The number of out of order events.
    """
    behind = pl.col("event_at") < pl.col("event_at").cum_max().over("transaction_id")
    count: int = events.sort("arrival_index").select(behind.sum()).collect().item()
    return count


def latest_per_transaction(events: pl.LazyFrame) -> pl.LazyFrame:
    """Collapse events into the latest state of each transaction.

    Grain: one row per ``transaction_id``. The winning event is the last one when ordered by
    ``event_at``, then lifecycle rank of the status, then ``event_id``. Arrival order is ignored.

    Args:
        events: One row per ``event_id`` with the typed raw columns.

    Returns:
        The winning event's columns with ``status`` renamed to ``final_status`` and ``event_at``
        renamed to ``updated_at``, plus ``n_events``, sorted by ``transaction_id``.
    """
    attributes = [
        "merchant_id",
        "country",
        "currency",
        "amount_minor",
        "payment_method",
        "card_brand",
        "psp",
        "status",
        "decline_reason",
        "created_at",
        "event_at",
    ]
    return (
        events.with_columns(
            pl.col("status")
            .replace_strict(STATUS_RANK, default=UNKNOWN_STATUS_RANK, return_dtype=pl.Int8)
            .alias("status_rank")
        )
        .group_by("transaction_id")
        .agg(
            pl.col(attributes).sort_by(_LATEST_ORDER).last(),
            pl.len().alias("n_events"),
        )
        .rename({"status": "final_status", "event_at": "updated_at"})
        .sort("transaction_id")
    )


def add_derived_columns(transactions: pl.LazyFrame) -> pl.LazyFrame:
    """Add the USD amount and the local time columns.

    Formulas:
        ``amount_usd = amount_minor / 10 ** exponent(currency) * usd_rate(currency)``
        ``created_at_local = created_at`` converted to the time zone of ``country`` (naive)
        ``local_hour`` is 0 to 23; ``local_weekday`` is ISO, 1 for Monday to 7 for Sunday.

    Args:
        transactions: One row per transaction with ``amount_minor``, ``currency``, ``country``
            and ``created_at`` (UTC).

    Returns:
        The same rows with ``amount_usd``, ``created_at_local``, ``local_hour`` and
        ``local_weekday``. Unknown currencies or countries yield nulls.
    """
    usd_per_minor = {
        currency: USD_PER_MAJOR_UNIT[currency] / 10 ** MINOR_UNIT_EXPONENT[currency]
        for currency in CURRENCIES
    }
    local = pl.coalesce(
        pl.when(pl.col("country") == country).then(
            pl.col("created_at").dt.convert_time_zone(zone).dt.replace_time_zone(None)
        )
        for country, zone in TIME_ZONES.items()
    )
    return transactions.with_columns(
        (
            pl.col("amount_minor")
            * pl.col("currency").replace_strict(
                usd_per_minor, default=None, return_dtype=pl.Float64
            )
        ).alias("amount_usd"),
        local.alias("created_at_local"),
    ).with_columns(
        pl.col("created_at_local").dt.hour().alias("local_hour"),
        pl.col("created_at_local").dt.weekday().alias("local_weekday"),
    )


def ingest(raw_dir: Path, staging_dir: Path) -> IngestStats:
    """Build ``transactions.parquet`` from the raw webhook file and validate it.

    Quality assertions: unique ``transaction_id``, valid enum values, positive amounts, status
    mix within the expected ranges, and row count reconciliation between raw and staging (the
    raw counts come from a separate aggregation of the raw file, independent of the
    deduplication path).

    Args:
        raw_dir: Directory holding ``webhooks.jsonl``.
        staging_dir: Directory that receives ``transactions.parquet`` (created if missing).

    Returns:
        The counts of the run.

    Raises:
        FileNotFoundError: If the raw file does not exist.
        DataQualityError: If a quality assertion fails; nothing is written in that case.
    """
    deliveries = read_raw_events(raw_dir / WEBHOOKS_FILE)
    events = deduplicate_events(deliveries).collect()
    staging = add_derived_columns(latest_per_transaction(events.lazy())).collect()

    raw_rows, raw_events, raw_transactions = (
        deliveries.select(
            pl.len(), pl.col("event_id").n_unique(), pl.col("transaction_id").n_unique()
        )
        .collect()
        .row(0)
    )
    stats = IngestStats(
        raw_rows=raw_rows,
        unique_events=events.height,
        duplicate_deliveries=raw_rows - events.height,
        out_of_order_events=count_out_of_order(events.lazy()),
        transactions=staging.height,
    )

    quality.check_unique(staging, "transaction_id")
    quality.check_enum_values(
        staging,
        {
            "country": COUNTRIES,
            "currency": CURRENCIES,
            "payment_method": PAYMENT_METHODS,
            "psp": PSPS,
            "final_status": tuple(STATUS_RANK),
            "card_brand": CARD_BRANDS,
            "decline_reason": DECLINE_REASONS,
        },
        nullable=("card_brand", "decline_reason"),
    )
    quality.check_positive_amounts(staging, "amount_minor")
    quality.check_status_mix(staging, "final_status")
    quality.check_row_count_reconciliation(
        raw_rows=raw_rows,
        duplicate_deliveries=stats.duplicate_deliveries,
        unique_events=raw_events,
        events_in_staging=int(staging.get_column("n_events").sum()),
        raw_transactions=raw_transactions,
        staging_rows=staging.height,
    )

    staging_dir.mkdir(parents=True, exist_ok=True)
    staging.write_parquet(staging_dir / STAGING_FILE)
    logger.info(
        "ingest finished raw_rows=%d unique_events=%d duplicates_removed=%d "
        "out_of_order_events_handled=%d transactions=%d output=%s",
        stats.raw_rows,
        stats.unique_events,
        stats.duplicate_deliveries,
        stats.out_of_order_events,
        stats.transactions,
        staging_dir / STAGING_FILE,
    )
    return stats
