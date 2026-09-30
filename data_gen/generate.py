"""Synthetic webhook generator and CLI entry point.

Purpose: produce deterministic, realistic Yuno transaction webhook deliveries for TiendaMax,
    including duplicated and late (out of order) deliveries and a set of planted patterns that
    the analytics layer is expected to recover.
Inputs: a ``GeneratorConfig`` (seed, scale, window, output directory), usually from the CLI.
Outputs:
    ``<output_dir>/events/received_date=YYYY-MM-DD/events.jsonl``: one JSON object per webhook
        delivery (grain: one row per delivery, so an ``event_id`` can repeat), sorted by arrival.
    ``<output_dir>/_manifest.json``: ground truth counts and the planted patterns, used to
        reconcile downstream stages.

Assumptions:
    * The dataset is a snapshot taken at the end of the window: deliveries that would arrive after
      it are dropped, so recent transactions can legitimately still be ``pending``.
    * Every event carries the full transaction attributes; they never change between events.
    * Refunds are always full refunds.
    * Local time uses fixed UTC offsets that are valid for the default window (June to August).
    * PSP names are fictional. The generator shares no code with the pipeline on purpose: it
      plays the role of the upstream system and the JSON contract is the only interface.
"""

import argparse
import json
import logging
import shutil
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import numpy.typing as npt
import polars as pl

logger = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]

TRANSACTIONS_PER_MONTH = 400_000
MERCHANT_COUNT = 15_000

COUNTRIES: tuple[str, ...] = ("MX", "CO", "CL")
COUNTRY_MERCHANT_SHARE: tuple[float, ...] = (0.5, 0.3, 0.2)
CURRENCIES: tuple[str, ...] = ("MXN", "COP", "CLP")
UTC_OFFSET_HOURS: tuple[int, ...] = (-6, -5, -4)
MEDIAN_AMOUNT_MAJOR: tuple[float, ...] = (600.0, 120_000.0, 25_000.0)
# Minor units per major unit. COP tickets are whole pesos expressed in centavos; CLP has none.
MINOR_PER_MAJOR: tuple[int, ...] = (100, 100, 1)
AMOUNT_SIGMA = 0.8

METHODS: tuple[str, ...] = ("card", "oxxo", "spei", "pse", "nequi", "webpay", "khipu")
VOUCHER_METHOD = "oxxo"
# Per country: (method, share of that country's transactions).
METHOD_MIX: dict[str, tuple[tuple[str, float], ...]] = {
    "MX": (("card", 0.55), ("oxxo", 0.25), ("spei", 0.20)),
    "CO": (("card", 0.45), ("pse", 0.35), ("nequi", 0.20)),
    "CL": (("card", 0.60), ("webpay", 0.30), ("khipu", 0.10)),
}

PSPS: tuple[str, ...] = (
    "psp_azteca",
    "psp_norte",
    "psp_andes",
    "psp_cafetal",
    "psp_magdalena",
    "psp_pacifico",
    "psp_austral",
)
PSP_MIX: dict[str, tuple[tuple[str, float], ...]] = {
    "MX": (("psp_azteca", 0.6), ("psp_norte", 0.4)),
    "CO": (("psp_andes", 1.0),),
    "CL": (("psp_pacifico", 0.7), ("psp_austral", 0.3)),
}
# Colombia after the two new PSPs go live.
CO_PSP_MIX_AFTER_LAUNCH: tuple[tuple[str, float], ...] = (
    ("psp_andes", 0.6),
    ("psp_cafetal", 0.2),
    ("psp_magdalena", 0.2),
)
CO_NEW_PSP_LAUNCH_DAY = 45

CARD_BRANDS: tuple[str, ...] = ("visa", "mastercard", "amex")
CARD_BRAND_SHARE: tuple[float, ...] = (0.55, 0.35, 0.10)
CHANNELS: tuple[str, ...] = ("web", "mobile_web", "app")
CHANNEL_SHARE: tuple[float, ...] = (0.45, 0.35, 0.20)

STATUSES: tuple[str, ...] = ("approved", "declined", "failed", "expired", "pending", "refunded")
APPROVED, DECLINED, FAILED, EXPIRED, PENDING, REFUNDED = range(6)
# Per method: probability of (approved, declined, failed, expired, pending) before modifiers.
BASE_OUTCOME: dict[str, tuple[float, float, float, float, float]] = {
    "card": (0.80, 0.14, 0.02, 0.03, 0.01),
    "oxxo": (0.64, 0.00, 0.005, 0.335, 0.02),
    "spei": (0.90, 0.03, 0.02, 0.04, 0.01),
    "pse": (0.82, 0.08, 0.04, 0.05, 0.01),
    "nequi": (0.86, 0.07, 0.03, 0.03, 0.01),
    "webpay": (0.88, 0.06, 0.02, 0.03, 0.01),
    "khipu": (0.84, 0.06, 0.04, 0.05, 0.01),
}
REFUND_SHARE_OF_APPROVED = 0.03

DECLINE_CODES: tuple[str, ...] = (
    "insufficient_funds",
    "do_not_honor",
    "fraud_suspected",
    "invalid_data",
    "limit_exceeded",
)
DECLINE_CODE_SHARE: tuple[float, ...] = (0.40, 0.25, 0.15, 0.10, 0.10)
FAILURE_CODES: tuple[str, ...] = ("psp_timeout", "psp_unavailable", "network_error")
FAILURE_CODE_SHARE: tuple[float, ...] = (0.60, 0.25, 0.15)
ERROR_CODES: tuple[str, ...] = DECLINE_CODES + FAILURE_CODES

# Relative transaction volume per local hour of day (index 0 is midnight).
HOUR_WEIGHTS: tuple[float, ...] = (
    1.0, 0.6, 0.4, 0.3, 0.3, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 5.5,
    6.0, 6.0, 5.5, 5.0, 5.0, 5.5, 6.0, 7.0, 7.5, 7.0, 5.0, 2.5,
)  # fmt: skip

# Planted patterns (probability mass moved from "approved" to the named outcome).
CO_CARD_DECLINE_SHIFT = 0.06
CL_CARD_APPROVAL_SHIFT = 0.06
CAFETAL_APPROVAL_SHIFT = 0.04
MAGDALENA_DECLINE_SHIFT = 0.06
MAGDALENA_FAILURE_SHIFT = 0.05
NIGHT_TIMEOUT_PSP = "psp_norte"
NIGHT_TIMEOUT_LOCAL_HOURS: tuple[int, ...] = (2, 3, 4)
NIGHT_TIMEOUT_FAILURE_SHIFT = 0.25
AMEX_DECLINE_SHIFT = 0.12
MERCHANT_ISSUE_START_DAY = 60
UX_ISSUE_EXPIRED_SHIFT_MOBILE_WEB = 0.35
UX_ISSUE_EXPIRED_SHIFT_OTHER = 0.10
PROCESSING_ISSUE_PSP = "psp_azteca"
PROCESSING_ISSUE_DECLINE_SHIFT = 0.30

# Delivery behaviour.
DELIVERY_LATENCY_MEAN_MS = 2_000.0
LATE_DELIVERY_SHARE = 0.05
LATE_DELIVERY_RANGE_MS: tuple[int, int] = (60_000, 48 * 3_600_000)
DUPLICATE_DELIVERY_SHARE = 0.02
DUPLICATE_RETRY_RANGE_MS: tuple[int, int] = (1_000, 6 * 3_600_000)

# Transaction lifecycle timing.
CARD_RESOLUTION_MEAN_MS = 20_000.0
OTHER_RESOLUTION_MEAN_MS = 120_000.0
VOUCHER_PAYMENT_RANGE_MS: tuple[int, int] = (3_600_000, 72 * 3_600_000)
VOUCHER_TTL_MS = 72 * 3_600_000
SESSION_TTL_MS = 30 * 60_000
REFUND_DELAY_RANGE_MS: tuple[int, int] = (86_400_000, 10 * 86_400_000)

MS_PER_HOUR = 3_600_000
MS_PER_DAY = 86_400_000
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%.3fZ"


@dataclass(frozen=True)
class GeneratorConfig:
    """Parameters of one generation run.

    Attributes:
        seed: Seed of the random generator; the same seed always yields the same files.
        scale: Multiplier on the real volume of 400,000 transactions per 30 days.
        start_date: First local day of the window.
        days: Number of local days in the window.
        output_dir: Directory that receives ``events/`` and ``_manifest.json``.
    """

    seed: int = 42
    scale: float = 1.0
    start_date: date = date(2026, 6, 1)
    days: int = 90
    output_dir: Path = Path("data/raw")


@dataclass(frozen=True)
class _Transactions:
    """Column arrays with one element per generated transaction."""

    merchant: IntArray
    country: IntArray
    method: IntArray
    psp: IntArray
    card_brand: IntArray
    channel: IntArray
    amount_minor: IntArray
    outcome: IntArray
    refunded: BoolArray
    error_code: IntArray
    created_ms: IntArray
    ux_merchant: int
    processing_merchant: int


def _choose(rng: np.random.Generator, shares: Sequence[float], size: int) -> IntArray:
    """Sample ``size`` category indexes with the given (normalized here) shares."""
    probabilities = np.asarray(shares, dtype=np.float64)
    return rng.choice(len(shares), size=size, p=probabilities / probabilities.sum()).astype(
        np.int64
    )


def _choose_named(
    rng: np.random.Generator,
    mix: Sequence[tuple[str, float]],
    vocabulary: Sequence[str],
    size: int,
) -> IntArray:
    """Sample names from ``mix`` and return their indexes in ``vocabulary``."""
    codes = np.asarray([vocabulary.index(name) for name, _ in mix], dtype=np.int64)
    picked: IntArray = codes[_choose(rng, [share for _, share in mix], size)]
    return picked


def _shift(probs: FloatArray, mask: BoolArray, source: int, target: int, delta: float) -> None:
    """Move up to ``delta`` probability mass from ``source`` to ``target`` for masked rows."""
    moved = np.minimum(probs[mask, source], delta)
    probs[mask, source] -= moved
    probs[mask, target] += moved


def _uniform_ms(rng: np.random.Generator, bounds: tuple[int, int], size: int) -> IntArray:
    """Sample integer millisecond durations uniformly within ``bounds``."""
    return rng.integers(bounds[0], bounds[1], size=size, dtype=np.int64)


def _sample_transactions(
    rng: np.random.Generator, config: GeneratorConfig, start_ms: int
) -> _Transactions:
    """Sample every transaction attribute and its final outcome.

    Grain: one array element per transaction.
    """
    n = round(TRANSACTIONS_PER_MONTH * config.days / 30 * config.scale)

    merchant_country = _choose(rng, COUNTRY_MERCHANT_SHARE, MERCHANT_COUNT)
    merchant_weight = rng.lognormal(mean=0.0, sigma=1.5, size=MERCHANT_COUNT)
    merchant = _choose(rng, merchant_weight.tolist(), n)
    country = merchant_country[merchant]

    def largest_merchant(country_name: str) -> int:
        in_country = merchant_country == COUNTRIES.index(country_name)
        return int(np.argmax(np.where(in_country, merchant_weight, -1.0)))

    ux_merchant = largest_merchant("CL")
    processing_merchant = largest_merchant("MX")

    day = rng.integers(0, config.days, size=n, dtype=np.int64)
    local_hour = _choose(rng, HOUR_WEIGHTS, n)
    local_ms = day * MS_PER_DAY + local_hour * MS_PER_HOUR + _uniform_ms(rng, (0, MS_PER_HOUR), n)
    offset_ms = np.asarray(UTC_OFFSET_HOURS, dtype=np.int64)[country] * MS_PER_HOUR
    created_ms = start_ms + local_ms - offset_ms

    method = np.zeros(n, dtype=np.int64)
    psp = np.zeros(n, dtype=np.int64)
    for code, name in enumerate(COUNTRIES):
        in_country = country == code
        size = int(in_country.sum())
        method[in_country] = _choose_named(rng, METHOD_MIX[name], METHODS, size)
        psp[in_country] = _choose_named(rng, PSP_MIX[name], PSPS, size)
    co_after_launch = (country == COUNTRIES.index("CO")) & (day >= CO_NEW_PSP_LAUNCH_DAY)
    psp[co_after_launch] = _choose_named(
        rng, CO_PSP_MIX_AFTER_LAUNCH, PSPS, int(co_after_launch.sum())
    )

    is_card = method == METHODS.index("card")
    card_brand = np.where(is_card, _choose(rng, CARD_BRAND_SHARE, n), -1)
    channel = _choose(rng, CHANNEL_SHARE, n)

    major = np.asarray(MEDIAN_AMOUNT_MAJOR)[country] * rng.lognormal(0.0, AMOUNT_SIGMA, size=n)
    whole_major = np.maximum(np.rint(major), 1.0).astype(np.int64)
    cents = np.where(country == COUNTRIES.index("MX"), rng.integers(0, 100, size=n), 0)
    minor_per_major = np.asarray(MINOR_PER_MAJOR, dtype=np.int64)[country]
    amount_minor = whole_major * minor_per_major + cents

    probs = np.asarray([BASE_OUTCOME[name] for name in METHODS], dtype=np.float64)[method]
    late_window = day >= MERCHANT_ISSUE_START_DAY
    _shift(
        probs, is_card & (country == COUNTRIES.index("CO")), APPROVED, DECLINED,
        CO_CARD_DECLINE_SHIFT,
    )  # fmt: skip
    _shift(
        probs, is_card & (country == COUNTRIES.index("CL")), DECLINED, APPROVED,
        CL_CARD_APPROVAL_SHIFT,
    )  # fmt: skip
    _shift(probs, psp == PSPS.index("psp_cafetal"), DECLINED, APPROVED, CAFETAL_APPROVAL_SHIFT)
    in_magdalena = psp == PSPS.index("psp_magdalena")
    _shift(probs, in_magdalena, APPROVED, DECLINED, MAGDALENA_DECLINE_SHIFT)
    _shift(probs, in_magdalena, APPROVED, FAILED, MAGDALENA_FAILURE_SHIFT)
    night_timeout = (psp == PSPS.index(NIGHT_TIMEOUT_PSP)) & np.isin(
        local_hour, NIGHT_TIMEOUT_LOCAL_HOURS
    )
    _shift(probs, night_timeout, APPROVED, FAILED, NIGHT_TIMEOUT_FAILURE_SHIFT)
    _shift(probs, card_brand == CARD_BRANDS.index("amex"), APPROVED, DECLINED, AMEX_DECLINE_SHIFT)
    ux_issue = (merchant == ux_merchant) & late_window
    on_mobile_web = channel == CHANNELS.index("mobile_web")
    _shift(probs, ux_issue & on_mobile_web, APPROVED, EXPIRED, UX_ISSUE_EXPIRED_SHIFT_MOBILE_WEB)
    _shift(probs, ux_issue & ~on_mobile_web, APPROVED, EXPIRED, UX_ISSUE_EXPIRED_SHIFT_OTHER)
    processing_issue = (
        (merchant == processing_merchant)
        & late_window
        & (psp == PSPS.index(PROCESSING_ISSUE_PSP))
    )  # fmt: skip
    _shift(probs, processing_issue, APPROVED, DECLINED, PROCESSING_ISSUE_DECLINE_SHIFT)

    thresholds = probs.cumsum(axis=1)[:, :4]
    outcome = (rng.random(n)[:, None] > thresholds).sum(axis=1).astype(np.int64)
    refunded = (outcome == APPROVED) & (rng.random(n) < REFUND_SHARE_OF_APPROVED)

    decline_code = _choose(rng, DECLINE_CODE_SHARE, n)
    failure_code = _choose(rng, FAILURE_CODE_SHARE, n) + len(DECLINE_CODES)
    failure_code[night_timeout] = ERROR_CODES.index("psp_timeout")
    error_code = np.where(
        outcome == DECLINED, decline_code, np.where(outcome == FAILED, failure_code, -1)
    )

    return _Transactions(
        merchant=merchant,
        country=country,
        method=method,
        psp=psp,
        card_brand=card_brand,
        channel=channel,
        amount_minor=amount_minor,
        outcome=outcome,
        refunded=refunded,
        error_code=error_code,
        created_ms=created_ms,
        ux_merchant=ux_merchant,
        processing_merchant=processing_merchant,
    )


def _delivery_latency(rng: np.random.Generator, size: int) -> tuple[IntArray, BoolArray]:
    """Sample webhook delivery latency and flag the late deliveries."""
    latency = rng.exponential(DELIVERY_LATENCY_MEAN_MS, size=size).astype(np.int64)
    late = rng.random(size) < LATE_DELIVERY_SHARE
    latency = latency + np.where(late, _uniform_ms(rng, LATE_DELIVERY_RANGE_MS, size), 0)
    return latency, late


def _names(column: str, vocabulary: Sequence[str]) -> pl.Expr:
    """Map an integer code column to its name; codes outside the vocabulary become null."""
    return pl.col(column).replace_strict(
        dict(enumerate(vocabulary)), default=None, return_dtype=pl.String
    )


def _prefixed_id(prefix: str, column: str, width: int) -> pl.Expr:
    """Format an integer column as a zero padded identifier such as ``txn_000000042``."""
    return pl.format(f"{prefix}_{{}}", pl.col(column).cast(pl.String).str.zfill(width))


def _timestamp(column: str) -> pl.Expr:
    """Format epoch milliseconds as an ISO 8601 UTC string."""
    return pl.from_epoch(pl.col(column), time_unit="ms").dt.strftime(TIMESTAMP_FORMAT)


def generate(config: GeneratorConfig) -> dict[str, Any]:
    """Generate the raw webhook deliveries and the ground truth manifest.

    Any existing ``events`` directory under ``config.output_dir`` is replaced.

    Args:
        config: Seed, scale, window and output directory of the run.

    Returns:
        The manifest that was written to ``_manifest.json``: run parameters, delivery, event and
        transaction counts, observed status counts and the planted patterns.

    Raises:
        ValueError: If ``config.scale`` or ``config.days`` is not positive.
    """
    if config.scale <= 0 or config.days <= 0:
        raise ValueError("scale and days must be positive")

    rng = np.random.default_rng(config.seed)
    start = datetime(
        config.start_date.year, config.start_date.month, config.start_date.day, tzinfo=UTC
    )
    start_ms = int(start.timestamp() * 1000)
    snapshot_ms = start_ms + config.days * MS_PER_DAY

    txn = _sample_transactions(rng, config, start_ms)
    n = len(txn.outcome)

    # Lifecycle: every transaction emits "pending"; resolved ones emit a final event; some
    # approved ones emit "refunded" later.
    is_card = txn.method == METHODS.index("card")
    is_voucher = txn.method == METHODS.index(VOUCHER_METHOD)
    resolution = np.where(
        is_card,
        rng.exponential(CARD_RESOLUTION_MEAN_MS, size=n),
        rng.exponential(OTHER_RESOLUTION_MEAN_MS, size=n),
    ).astype(np.int64)
    resolution = np.where(
        is_voucher & (txn.outcome == APPROVED),
        _uniform_ms(rng, VOUCHER_PAYMENT_RANGE_MS, n),
        resolution,
    )
    resolution = np.where(
        txn.outcome == EXPIRED, np.where(is_voucher, VOUCHER_TTL_MS, SESSION_TTL_MS), resolution
    )
    final_ms = txn.created_ms + resolution + 1
    refund_ms = final_ms + _uniform_ms(rng, REFUND_DELAY_RANGE_MS, n)

    kinds: tuple[tuple[BoolArray, IntArray, IntArray], ...] = (
        (np.ones(n, dtype=np.bool_), np.full(n, PENDING, dtype=np.int64), txn.created_ms),
        (txn.outcome != PENDING, txn.outcome, final_ms),
        (txn.refunded, np.full(n, REFUNDED, dtype=np.int64), refund_ms),
    )
    kept_by_kind: list[BoolArray] = []
    parts: list[tuple[IntArray, IntArray, IntArray, IntArray, BoolArray]] = []
    for exists, status, occurred_ms in kinds:
        latency, late = _delivery_latency(rng, n)
        received_ms = occurred_ms + latency
        kept = exists & (received_ms < snapshot_ms)
        kept_by_kind.append(kept)
        index = np.flatnonzero(kept)
        parts.append((index, status[index], occurred_ms[index], received_ms[index], late[index]))

    txn_index = np.concatenate([part[0] for part in parts])
    status = np.concatenate([part[1] for part in parts])
    occurred = np.concatenate([part[2] for part in parts])
    received = np.concatenate([part[3] for part in parts])
    late_flags = np.concatenate([part[4] for part in parts])
    n_events = len(txn_index)
    # Shuffled so that identifiers carry no ordering information.
    event_number = rng.permutation(n_events).astype(np.int64)

    duplicate_received = received + _uniform_ms(rng, DUPLICATE_RETRY_RANGE_MS, n_events)
    duplicated = (rng.random(n_events) < DUPLICATE_DELIVERY_SHARE) & (
        duplicate_received < snapshot_ms
    )
    dup = np.flatnonzero(duplicated)

    deliveries = pl.DataFrame(
        {
            "event_number": np.concatenate([event_number, event_number[dup]]),
            "txn_index": np.concatenate([txn_index, txn_index[dup]]),
            "status_code": np.concatenate([status, status[dup]]),
            "occurred_ms": np.concatenate([occurred, occurred[dup]]),
            "received_ms": np.concatenate([received, duplicate_received[dup]]),
        }
    )
    transactions = pl.DataFrame(
        {
            "txn_index": np.arange(n, dtype=np.int64),
            "merchant_code": txn.merchant,
            "country_code": txn.country,
            "method_code": txn.method,
            "psp_code": txn.psp,
            "card_brand_code": txn.card_brand,
            "channel_code": txn.channel,
            "amount_minor": txn.amount_minor,
            "error_code_index": txn.error_code,
        }
    )
    carries_error = pl.col("status_code").is_in([DECLINED, FAILED])
    frame = (
        deliveries.lazy()
        .join(transactions.lazy(), on="txn_index", how="inner")
        .sort("received_ms", "event_number")
        .select(
            pl.from_epoch(pl.col("received_ms"), time_unit="ms").dt.date().alias("received_date"),
            _prefixed_id("evt", "event_number", 10).alias("event_id"),
            _prefixed_id("txn", "txn_index", 9).alias("transaction_id"),
            _prefixed_id("mrc", "merchant_code", 5).alias("merchant_id"),
            _names("country_code", COUNTRIES).alias("country"),
            _names("country_code", CURRENCIES).alias("currency"),
            pl.col("amount_minor"),
            _names("method_code", METHODS).alias("payment_method"),
            _names("psp_code", PSPS).alias("psp"),
            _names("card_brand_code", CARD_BRANDS).alias("card_brand"),
            _names("channel_code", CHANNELS).alias("channel"),
            _names("status_code", STATUSES).alias("status"),
            pl.when(carries_error)
            .then(_names("error_code_index", ERROR_CODES))
            .otherwise(None)
            .alias("error_code"),
            _timestamp("occurred_ms").alias("occurred_at"),
            _timestamp("received_ms").alias("received_at"),
        )
        .collect()
    )

    events_dir = config.output_dir / "events"
    if events_dir.exists():
        shutil.rmtree(events_dir)
    partitions = frame.partition_by(
        "received_date", maintain_order=True, include_key=False, as_dict=True
    )
    for (received_date,), partition in partitions.items():
        partition_dir = events_dir / f"received_date={received_date}"
        partition_dir.mkdir(parents=True)
        partition.write_ndjson(partition_dir / "events.jsonl")

    # Ground truth computed at transaction level, independently of the delivery frame.
    pending_kept, final_kept, refund_kept = kept_by_kind
    observed = np.where(
        refund_kept,
        REFUNDED,
        np.where(final_kept, txn.outcome, np.where(pending_kept, PENDING, -1)),
    )
    manifest: dict[str, Any] = {
        "seed": config.seed,
        "scale": config.scale,
        "start_date": config.start_date.isoformat(),
        "days": config.days,
        "snapshot_at": (start + timedelta(days=config.days)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "n_transactions_generated": n,
        "n_transactions": int((observed >= 0).sum()),
        "n_events_unique": n_events,
        "n_duplicate_deliveries": len(dup),
        "n_deliveries": n_events + len(dup),
        "n_late_events": int(late_flags.sum()),
        "n_partitions": len(partitions),
        "status_counts": {
            name: int((observed == code).sum()) for code, name in enumerate(STATUSES)
        },
        "planted": {
            "co_new_psps": ["psp_cafetal", "psp_magdalena"],
            "co_new_psp_launch_date": (
                config.start_date + timedelta(days=CO_NEW_PSP_LAUNCH_DAY)
            ).isoformat(),
            "merchant_issue_start_date": (
                config.start_date + timedelta(days=MERCHANT_ISSUE_START_DAY)
            ).isoformat(),
            "ux_issue_merchant_id": f"mrc_{txn.ux_merchant:05d}",
            "processing_issue_merchant_id": f"mrc_{txn.processing_merchant:05d}",
            "processing_issue_psp": PROCESSING_ISSUE_PSP,
            "night_timeout_psp": NIGHT_TIMEOUT_PSP,
            "night_timeout_local_hours": list(NIGHT_TIMEOUT_LOCAL_HOURS),
            "high_decline_card_brand": "amex",
        },
    }
    (config.output_dir / "_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    logger.info(
        "generation finished transactions=%d unique_events=%d deliveries=%d duplicates=%d "
        "partitions=%d",
        manifest["n_transactions"],
        manifest["n_events_unique"],
        manifest["n_deliveries"],
        manifest["n_duplicate_deliveries"],
        manifest["n_partitions"],
    )
    return manifest


def main(argv: Sequence[str] | None = None) -> int:
    """Generate the raw webhook dataset from the command line.

    Args:
        argv: Command line arguments without the program name. Defaults to ``sys.argv[1:]``.

    Returns:
        The process exit code, 0 on success.

    Raises:
        SystemExit: If the arguments cannot be parsed.
    """
    defaults = GeneratorConfig()
    parser = argparse.ArgumentParser(description="Generate synthetic Yuno webhook events.")
    parser.add_argument("--seed", type=int, default=defaults.seed)
    parser.add_argument("--scale", type=float, default=defaults.scale)
    parser.add_argument("--start-date", type=date.fromisoformat, default=defaults.start_date)
    parser.add_argument("--days", type=int, default=defaults.days)
    parser.add_argument("--output-dir", type=Path, default=defaults.output_dir)
    args = parser.parse_args(argv)
    generate(
        GeneratorConfig(
            seed=args.seed,
            scale=args.scale,
            start_date=args.start_date,
            days=args.days,
            output_dir=args.output_dir,
        )
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
