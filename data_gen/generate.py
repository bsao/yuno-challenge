"""Synthetic webhook generator and CLI entry point.

Purpose: produce deterministic, realistic Yuno transaction webhooks for TiendaMax, with duplicated
    and out of order events and three planted anomalies the analytics layer must recover.
Inputs: a ``GeneratorConfig`` (seed, transaction count, window, output directory), usually from
    the CLI.
Outputs (all under ``config.output_dir``, by default ``data/raw``):
    ``webhooks.jsonl``: grain is one row per webhook delivery, which is one event per status
        change plus about 1% repeated deliveries of the same ``event_id``. Line order is arrival
        order, so some events appear after a later event of the same transaction.
    ``merchants.csv``: one row per merchant (``merchant_id``, ``country``, ``category``,
        ``size_tier``).
    ``psp_fees.csv``: one row per PSP, country and payment method (``pct_fee`` in percent of the
        amount, ``fixed_fee_usd`` per transaction).
    ``planted_anomalies.json``: ground truth of the planted anomalies.

Assumptions:
    * The window is the 90 local days ending on ``config.end_date``. The default end date is
      fixed (2026-09-30) so that the same seed always yields byte identical files.
    * The files are a snapshot taken at ``end_date + 1 day`` 06:00 UTC (local midnight in the
      westernmost country). Status changes after the snapshot are not emitted, so recent
      transactions can still be ``pending``.
    * Every event carries the full transaction attributes; they never change between events.
    * Refunds are always full refunds. Each merchant operates in one country.
    * Cards are one payment method (``card``) with ``card_brand`` visa or mastercard.
    * The generator shares no code with the pipeline on purpose: it plays the upstream system and
      the file contract is the only interface.
"""

import argparse
import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import numpy as np
import numpy.typing as npt
import polars as pl

logger = logging.getLogger(__name__)

IntArray = npt.NDArray[np.int64]
FloatArray = npt.NDArray[np.float64]
BoolArray = npt.NDArray[np.bool_]

WEBHOOKS_FILE = "webhooks.jsonl"
MERCHANTS_FILE = "merchants.csv"
PSP_FEES_FILE = "psp_fees.csv"
PLANTED_ANOMALIES_FILE = "planted_anomalies.json"
RAW_FILES: tuple[str, ...] = (WEBHOOKS_FILE, MERCHANTS_FILE, PSP_FEES_FILE, PLANTED_ANOMALIES_FILE)

MERCHANT_COUNT = 120
MERCHANT_VOLUME_SIGMA = 1.4
MERCHANT_CATEGORIES: tuple[str, ...] = (
    "fashion",
    "electronics",
    "grocery",
    "home",
    "beauty",
    "travel",
)
# Size tiers by volume rank: the first 10 merchants, the next 30, then everyone else.
SIZE_TIERS: tuple[tuple[str, int], ...] = (
    ("enterprise", 10),
    ("mid", 40),
    ("small", MERCHANT_COUNT),
)

COUNTRIES: tuple[str, ...] = ("MX", "CO", "CL")
COUNTRY_MERCHANT_SHARE: tuple[float, ...] = (0.5, 0.3, 0.2)
CURRENCIES: tuple[str, ...] = ("MXN", "COP", "CLP")
TIME_ZONES: tuple[str, ...] = ("America/Mexico_City", "America/Bogota", "America/Santiago")
MEDIAN_AMOUNT_MAJOR: tuple[float, ...] = (600.0, 120_000.0, 25_000.0)
# Minor units per major unit. COP tickets are whole pesos expressed in centavos; CLP has none.
MINOR_PER_MAJOR: tuple[int, ...] = (100, 100, 1)
AMOUNT_SIGMA = 0.8

METHODS: tuple[str, ...] = ("card", "oxxo", "spei", "pse", "webpay")
CARD_METHOD = "card"
VOUCHER_METHOD = "oxxo"
# Per country: (method, share of that country's transactions).
METHOD_MIX: dict[str, tuple[tuple[str, float], ...]] = {
    "MX": (("card", 0.70), ("oxxo", 0.12), ("spei", 0.18)),
    "CO": (("card", 0.65), ("pse", 0.35)),
    "CL": (("card", 0.70), ("webpay", 0.30)),
}
CARD_BRANDS: tuple[str, ...] = ("visa", "mastercard")
CARD_BRAND_SHARE: tuple[float, ...] = (0.6, 0.4)

PSPS: tuple[str, ...] = ("PSP_A", "PSP_B", "PSP_C", "PSP_D")
PSP_MIX: tuple[tuple[str, float], ...] = (("PSP_A", 0.55), ("PSP_B", 0.45))
# Colombia once PSP_C and PSP_D go live (they serve no other country).
NEW_PSPS: tuple[str, ...] = ("PSP_C", "PSP_D")
NEW_PSP_COUNTRY = "CO"
NEW_PSP_LIVE_DAYS = 45
CO_PSP_MIX_AFTER_LAUNCH: tuple[tuple[str, float], ...] = (
    ("PSP_A", 0.40),
    ("PSP_B", 0.30),
    ("PSP_C", 0.15),
    ("PSP_D", 0.15),
)
# Fee schedule: percent of the amount per PSP plus a per method adjustment, and a fixed USD fee.
PSP_PCT_FEE: dict[str, float] = {"PSP_A": 2.9, "PSP_B": 2.6, "PSP_C": 3.2, "PSP_D": 2.2}
PSP_FIXED_FEE_USD: dict[str, float] = {"PSP_A": 0.10, "PSP_B": 0.12, "PSP_C": 0.08, "PSP_D": 0.05}
METHOD_PCT_FEE_ADJUSTMENT: dict[str, float] = {
    "card": 0.0,
    "oxxo": 0.6,
    "spei": -1.6,
    "pse": -1.2,
    "webpay": -0.8,
}

STATUSES: tuple[str, ...] = ("approved", "declined", "failed", "expired", "pending", "refunded")
APPROVED, DECLINED, FAILED, EXPIRED, PENDING, REFUNDED = range(6)
# Per method: probability of (approved, declined, failed, expired, pending) before modifiers.
BASE_OUTCOME: dict[str, tuple[float, float, float, float, float]] = {
    "card": (0.760, 0.195, 0.030, 0.010, 0.005),
    "oxxo": (0.620, 0.000, 0.010, 0.350, 0.020),
    "spei": (0.880, 0.060, 0.030, 0.020, 0.010),
    "pse": (0.800, 0.130, 0.040, 0.020, 0.010),
    "webpay": (0.840, 0.115, 0.030, 0.010, 0.005),
}
REFUND_SHARE_OF_APPROVED = 0.02
# Segment effects on authorization (probability mass moved between approved and declined).
CO_CARD_DECLINE_SHIFT = 0.04
CL_CARD_APPROVAL_SHIFT = 0.04
MASTERCARD_DECLINE_SHIFT = 0.02
PSP_B_DECLINE_SHIFT = 0.03
PSP_B_CL_APPROVAL_SHIFT = 0.06
PSP_C_APPROVAL_SHIFT = 0.05
PSP_D_DECLINE_SHIFT = 0.04
PSP_D_FAILURE_SHIFT = 0.03

REASONS: tuple[str, ...] = (
    "insufficient_funds",
    "card_declined",
    "fraud_suspected",
    "expired",
    "processor_error",
    "network_timeout",
)
# Reason mixes as (reason, share). Non card methods cannot be "card_declined".
CARD_DECLINE_REASONS: tuple[tuple[str, float], ...] = (
    ("insufficient_funds", 0.45),
    ("card_declined", 0.35),
    ("fraud_suspected", 0.20),
)
OTHER_DECLINE_REASONS: tuple[tuple[str, float], ...] = (
    ("insufficient_funds", 0.70),
    ("fraud_suspected", 0.30),
)
FAILURE_REASONS: tuple[tuple[str, float], ...] = (
    ("processor_error", 0.6),
    ("network_timeout", 0.4),
)

# Relative volume per local hour of day (index 0 is midnight) and per weekday (index 0 is Monday).
HOUR_WEIGHTS: tuple[float, ...] = (
    1.2, 0.9, 0.8, 0.7, 0.6, 0.7, 1.0, 2.0, 3.0, 4.0, 5.0, 5.5,
    6.0, 6.0, 5.5, 5.0, 5.0, 5.5, 6.0, 7.0, 7.5, 7.0, 5.0, 2.5,
)  # fmt: skip
WEEKDAY_WEIGHTS: tuple[float, ...] = (1.0, 1.0, 1.0, 1.05, 1.2, 1.15, 0.9)

# Planted anomalies.
DECLINE_SPIKE_PSP = "PSP_C"
DECLINE_SPIKE_COUNTRY = "CO"
DECLINE_SPIKE_FIRST_DAY = 60
DECLINE_SPIKE_DAYS = 3
DECLINE_SPIKE_FACTOR = 2.0
OXXO_EXPIRY_COUNTRY = "MX"
OXXO_EXPIRY_MERCHANT_RANK = 2  # third largest Mexican merchant
OXXO_EXPIRY_OUTCOME: tuple[float, float, float, float, float] = (0.04, 0.0, 0.0, 0.95, 0.01)
TIMEOUT_SPIKE_PSP = "PSP_B"
TIMEOUT_SPIKE_COUNTRY = "MX"
TIMEOUT_SPIKE_EARLIEST_DAY = 66
TIMEOUT_SPIKE_LOCAL_HOURS: tuple[int, ...] = (2, 3)  # 02:00 to 03:59 local time
TIMEOUT_SPIKE_FAILURE_SHIFT = 0.60

# Delivery behaviour.
DUPLICATE_SHARE = 0.01
DUPLICATE_RETRY_RANGE_MS: tuple[int, int] = (1_000, 6 * 3_600_000)
OUT_OF_ORDER_SHARE = 0.03
OUT_OF_ORDER_DELAY_RANGE_MS: tuple[int, int] = (60_000, 48 * 3_600_000)

# Transaction lifecycle timing.
CARD_RESOLUTION_MEAN_MS = 20_000.0
OTHER_RESOLUTION_MEAN_MS = 120_000.0
VOUCHER_PAYMENT_RANGE_MS: tuple[int, int] = (3_600_000, 72 * 3_600_000)
VOUCHER_TTL_MS = 72 * 3_600_000
SESSION_TTL_MS = 30 * 60_000
REFUND_DELAY_RANGE_MS: tuple[int, int] = (86_400_000, 10 * 86_400_000)

MS_PER_HOUR = 3_600_000
MS_PER_DAY = 86_400_000
SNAPSHOT_HOUR_UTC = 6
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S%.3fZ"


@dataclass(frozen=True)
class GeneratorConfig:
    """Parameters of one generation run.

    Attributes:
        seed: Seed of the random generator; the same seed always yields the same files.
        n_transactions: Number of transactions. The default is TiendaMax's real volume of about
            400,000 per month over 90 days.
        end_date: Last local day of the window.
        days: Number of local days in the window.
        output_dir: Directory that receives the raw files.
    """

    seed: int = 42
    n_transactions: int = 1_200_000
    end_date: date = date(2026, 9, 30)
    days: int = 90
    output_dir: Path = Path("data/raw")

    @property
    def start_date(self) -> date:
        """First local day of the window."""
        return self.end_date - timedelta(days=self.days - 1)

    def day_date(self, day: int) -> date:
        """Calendar date of a zero based day index within the window."""
        return self.start_date + timedelta(days=day)


@dataclass(frozen=True)
class _Merchants:
    """Column arrays with one element per merchant, plus the planted merchant."""

    country: IntArray
    weight: FloatArray
    category: IntArray
    size_tier: list[str]
    oxxo_expiry_merchant: int


@dataclass(frozen=True)
class _Transactions:
    """Column arrays with one element per generated transaction."""

    merchant: IntArray
    country: IntArray
    method: IntArray
    psp: IntArray
    card_brand: IntArray
    amount_minor: IntArray
    outcome: IntArray
    refunded: BoolArray
    reason: IntArray
    created_ms: IntArray


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


def _shift(
    probs: FloatArray, mask: BoolArray, source: int, target: int, delta: float | FloatArray
) -> None:
    """Move up to ``delta`` probability mass from ``source`` to ``target`` for masked rows."""
    moved = np.minimum(probs[mask, source], delta)
    probs[mask, source] -= moved
    probs[mask, target] += moved


def _uniform_ms(rng: np.random.Generator, bounds: tuple[int, int], size: int) -> IntArray:
    """Sample integer millisecond durations uniformly within ``bounds``."""
    return rng.integers(bounds[0], bounds[1], size=size, dtype=np.int64)


def _utc_offset_hours(config: GeneratorConfig) -> IntArray:
    """Return the UTC offset in hours per country (rows) and window day (columns).

    The offset is taken at local noon, which is exact except inside the one hour a daylight
    saving change skips or repeats (Chile changes within the default window).
    """
    offsets = np.zeros((len(COUNTRIES), config.days), dtype=np.int64)
    for country, zone in enumerate(TIME_ZONES):
        for day in range(config.days):
            noon = datetime.combine(config.day_date(day), time(12), tzinfo=ZoneInfo(zone))
            offset = noon.utcoffset()
            assert offset is not None
            offsets[country, day] = int(offset.total_seconds() // 3600)
    return offsets


def _timeout_spike_days(config: GeneratorConfig) -> tuple[int, int]:
    """Return the day indexes of the first Saturday and Sunday of the timeout spike weekend."""
    saturday = next(
        day
        for day in range(min(TIMEOUT_SPIKE_EARLIEST_DAY, config.days - 2), config.days)
        if config.day_date(day).weekday() == 5
    )
    return saturday, saturday + 1


def _sample_merchants(rng: np.random.Generator) -> _Merchants:
    """Sample the merchant dimension with a long tail (lognormal) volume distribution."""
    country = _choose(rng, COUNTRY_MERCHANT_SHARE, MERCHANT_COUNT)
    weight = rng.lognormal(mean=0.0, sigma=MERCHANT_VOLUME_SIGMA, size=MERCHANT_COUNT)
    category = _choose(rng, [1.0] * len(MERCHANT_CATEGORIES), MERCHANT_COUNT)

    rank = np.empty(MERCHANT_COUNT, dtype=np.int64)
    rank[np.argsort(-weight)] = np.arange(MERCHANT_COUNT)
    size_tier = [next(name for name, limit in SIZE_TIERS if r < limit) for r in rank.tolist()]

    in_country = np.flatnonzero(country == COUNTRIES.index(OXXO_EXPIRY_COUNTRY))
    by_volume = in_country[np.argsort(-weight[in_country])]
    oxxo_expiry_merchant = int(by_volume[min(OXXO_EXPIRY_MERCHANT_RANK, len(by_volume) - 1)])
    return _Merchants(country, weight, category, size_tier, oxxo_expiry_merchant)


def _sample_transactions(
    rng: np.random.Generator, config: GeneratorConfig, merchants: _Merchants, start_ms: int
) -> _Transactions:
    """Sample every transaction attribute and its final outcome.

    Grain: one array element per transaction.
    """
    n = config.n_transactions
    merchant = _choose(rng, merchants.weight.tolist(), n)
    country = merchants.country[merchant]

    weekday = np.asarray([config.day_date(day).weekday() for day in range(config.days)])
    day = _choose(rng, np.asarray(WEEKDAY_WEIGHTS)[weekday].tolist(), n)
    local_hour = _choose(rng, HOUR_WEIGHTS, n)
    local_ms = day * MS_PER_DAY + local_hour * MS_PER_HOUR + _uniform_ms(rng, (0, MS_PER_HOUR), n)
    created_ms = start_ms + local_ms - _utc_offset_hours(config)[country, day] * MS_PER_HOUR

    method = np.zeros(n, dtype=np.int64)
    for code, name in enumerate(COUNTRIES):
        in_country = country == code
        method[in_country] = _choose_named(rng, METHOD_MIX[name], METHODS, int(in_country.sum()))
    psp = _choose_named(rng, PSP_MIX, PSPS, n)
    new_psps_live = (country == COUNTRIES.index(NEW_PSP_COUNTRY)) & (
        day >= config.days - NEW_PSP_LIVE_DAYS
    )
    psp[new_psps_live] = _choose_named(rng, CO_PSP_MIX_AFTER_LAUNCH, PSPS, int(new_psps_live.sum()))

    is_card = method == METHODS.index(CARD_METHOD)
    card_brand = np.where(is_card, _choose(rng, CARD_BRAND_SHARE, n), -1)

    major = np.asarray(MEDIAN_AMOUNT_MAJOR)[country] * rng.lognormal(0.0, AMOUNT_SIGMA, size=n)
    whole_major = np.maximum(np.rint(major), 1.0).astype(np.int64)
    cents = np.where(country == COUNTRIES.index("MX"), rng.integers(0, 100, size=n), 0)
    amount_minor = whole_major * np.asarray(MINOR_PER_MAJOR, dtype=np.int64)[country] + cents

    # Segment effects: authorization differs by country, brand and PSP.
    probs = np.asarray([BASE_OUTCOME[name] for name in METHODS], dtype=np.float64)[method]
    in_co = country == COUNTRIES.index("CO")
    in_cl = country == COUNTRIES.index("CL")
    _shift(probs, is_card & in_co, APPROVED, DECLINED, CO_CARD_DECLINE_SHIFT)
    _shift(probs, is_card & in_cl, DECLINED, APPROVED, CL_CARD_APPROVAL_SHIFT)
    _shift(
        probs, card_brand == CARD_BRANDS.index("mastercard"), APPROVED, DECLINED,
        MASTERCARD_DECLINE_SHIFT,
    )  # fmt: skip
    on_psp_b = psp == PSPS.index("PSP_B")
    _shift(probs, on_psp_b & ~in_cl, APPROVED, DECLINED, PSP_B_DECLINE_SHIFT)
    _shift(probs, on_psp_b & in_cl, DECLINED, APPROVED, PSP_B_CL_APPROVAL_SHIFT)
    _shift(probs, psp == PSPS.index("PSP_C"), DECLINED, APPROVED, PSP_C_APPROVAL_SHIFT)
    on_psp_d = psp == PSPS.index("PSP_D")
    _shift(probs, on_psp_d, APPROVED, DECLINED, PSP_D_DECLINE_SHIFT)
    _shift(probs, on_psp_d, APPROVED, FAILED, PSP_D_FAILURE_SHIFT)

    # Planted anomaly a: the decline rate doubles for PSP_C card transactions in Colombia.
    decline_spike = (
        (psp == PSPS.index(DECLINE_SPIKE_PSP))
        & (country == COUNTRIES.index(DECLINE_SPIKE_COUNTRY))
        & is_card
        & (day >= DECLINE_SPIKE_FIRST_DAY)
        & (day < DECLINE_SPIKE_FIRST_DAY + DECLINE_SPIKE_DAYS)
    )
    extra_declines = probs[decline_spike, DECLINED] * (DECLINE_SPIKE_FACTOR - 1.0)
    _shift(probs, decline_spike, APPROVED, DECLINED, extra_declines)
    # Planted anomaly b: one Mexican merchant whose OXXO vouchers almost always expire.
    oxxo_expiry = (merchant == merchants.oxxo_expiry_merchant) & (
        method == METHODS.index(VOUCHER_METHOD)
    )
    probs[oxxo_expiry] = OXXO_EXPIRY_OUTCOME
    # Planted anomaly c: network timeouts for one PSP during the night of one weekend.
    timeout_spike = (
        (psp == PSPS.index(TIMEOUT_SPIKE_PSP))
        & (country == COUNTRIES.index(TIMEOUT_SPIKE_COUNTRY))
        & np.isin(day, _timeout_spike_days(config))
        & np.isin(local_hour, TIMEOUT_SPIKE_LOCAL_HOURS)
    )
    _shift(probs, timeout_spike, APPROVED, FAILED, TIMEOUT_SPIKE_FAILURE_SHIFT)

    thresholds = probs.cumsum(axis=1)[:, :4]
    outcome = (rng.random(n)[:, None] > thresholds).sum(axis=1).astype(np.int64)
    refunded = (outcome == APPROVED) & (rng.random(n) < REFUND_SHARE_OF_APPROVED)

    decline_reason = np.where(
        is_card,
        _choose_named(rng, CARD_DECLINE_REASONS, REASONS, n),
        _choose_named(rng, OTHER_DECLINE_REASONS, REASONS, n),
    )
    failure_reason = _choose_named(rng, FAILURE_REASONS, REASONS, n)
    failure_reason[timeout_spike] = REASONS.index("network_timeout")
    reason = np.select(
        [outcome == DECLINED, outcome == FAILED, outcome == EXPIRED],
        [decline_reason, failure_reason, REASONS.index("expired")],
        default=-1,
    ).astype(np.int64)

    return _Transactions(
        merchant=merchant,
        country=country,
        method=method,
        psp=psp,
        card_brand=card_brand,
        amount_minor=amount_minor,
        outcome=outcome,
        refunded=refunded,
        reason=reason,
        created_ms=created_ms,
    )


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


def _merchant_id(code: int) -> str:
    """Format a merchant index as its public identifier."""
    return f"mrc_{code:03d}"


def _write_reference_files(config: GeneratorConfig, merchants: _Merchants) -> None:
    """Write ``merchants.csv``, ``psp_fees.csv`` and ``planted_anomalies.json``."""
    pl.DataFrame(
        {
            "merchant_id": [_merchant_id(code) for code in range(MERCHANT_COUNT)],
            "country": [COUNTRIES[code] for code in merchants.country.tolist()],
            "category": [MERCHANT_CATEGORIES[code] for code in merchants.category.tolist()],
            "size_tier": merchants.size_tier,
        }
    ).write_csv(config.output_dir / MERCHANTS_FILE)

    fees = [
        {
            "psp": psp,
            "country": country,
            "payment_method": method,
            "pct_fee": round(PSP_PCT_FEE[psp] + METHOD_PCT_FEE_ADJUSTMENT[method], 2),
            "fixed_fee_usd": PSP_FIXED_FEE_USD[psp],
        }
        for psp in PSPS
        for country in COUNTRIES
        if psp not in NEW_PSPS or country == NEW_PSP_COUNTRY
        for method, _ in METHOD_MIX[country]
    ]
    pl.DataFrame(fees).write_csv(config.output_dir / PSP_FEES_FILE)

    spike_first = config.day_date(DECLINE_SPIKE_FIRST_DAY)
    saturday, sunday = (config.day_date(day) for day in _timeout_spike_days(config))
    planted = {
        "window": {
            "start_date": config.start_date.isoformat(),
            "end_date": config.end_date.isoformat(),
            "new_psps": list(NEW_PSPS),
            "new_psp_country": NEW_PSP_COUNTRY,
            "new_psp_first_date": config.day_date(config.days - NEW_PSP_LIVE_DAYS).isoformat(),
        },
        "anomalies": [
            {
                "id": "a",
                "type": "decline_rate_spike",
                "description": "The decline rate doubles for 3 consecutive local days.",
                "psp": DECLINE_SPIKE_PSP,
                "country": DECLINE_SPIKE_COUNTRY,
                "payment_method": CARD_METHOD,
                "start_date": spike_first.isoformat(),
                "end_date": (spike_first + timedelta(days=DECLINE_SPIKE_DAYS - 1)).isoformat(),
                "decline_rate_factor": DECLINE_SPIKE_FACTOR,
            },
            {
                "id": "b",
                "type": "voucher_expiration",
                "description": "One merchant whose OXXO vouchers expire about 95% of the time.",
                "merchant_id": _merchant_id(merchants.oxxo_expiry_merchant),
                "country": OXXO_EXPIRY_COUNTRY,
                "payment_method": VOUCHER_METHOD,
                "expected_expiration_rate": OXXO_EXPIRY_OUTCOME[EXPIRED],
            },
            {
                "id": "c",
                "type": "network_timeout_spike",
                "description": "network_timeout failures between 02:00 and 04:00 local time.",
                "psp": TIMEOUT_SPIKE_PSP,
                "country": TIMEOUT_SPIKE_COUNTRY,
                "dates": [saturday.isoformat(), sunday.isoformat()],
                "local_hours": list(TIMEOUT_SPIKE_LOCAL_HOURS),
                "decline_reason": "network_timeout",
                "added_failure_probability": TIMEOUT_SPIKE_FAILURE_SHIFT,
            },
        ],
    }
    (config.output_dir / PLANTED_ANOMALIES_FILE).write_text(json.dumps(planted, indent=2) + "\n")


def generate(config: GeneratorConfig) -> dict[str, Any]:
    """Generate the raw files described in the module docstring, replacing existing ones.

    Args:
        config: Seed, transaction count, window and output directory of the run.

    Returns:
        A ground truth summary computed per transaction, independently of the written file:
        ``n_transactions``, ``n_events_unique``, ``n_duplicate_deliveries``, ``n_deliveries``,
        ``n_out_of_order_deliveries`` and ``status_counts`` (latest status per transaction).

    Raises:
        ValueError: If ``config.n_transactions`` or ``config.days`` is not positive.
    """
    if config.n_transactions <= 0 or config.days <= 0:
        raise ValueError("n_transactions and days must be positive")

    rng = np.random.default_rng(config.seed)
    start_ms = int(datetime.combine(config.start_date, time(0), tzinfo=UTC).timestamp() * 1000)
    snapshot_ms = start_ms + config.days * MS_PER_DAY + SNAPSHOT_HOUR_UTC * MS_PER_HOUR

    merchants = _sample_merchants(rng)
    txn = _sample_transactions(rng, config, merchants, start_ms)
    n = config.n_transactions

    # Lifecycle: every transaction emits "pending"; resolved ones emit a final event; some
    # approved ones emit "refunded" later. Events after the snapshot are not emitted.
    is_voucher = txn.method == METHODS.index(VOUCHER_METHOD)
    resolution = np.where(
        txn.method == METHODS.index(CARD_METHOD),
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
    final_emitted = (txn.outcome != PENDING) & (final_ms < snapshot_ms)
    refund_emitted = txn.refunded & final_emitted & (refund_ms < snapshot_ms)

    kinds: tuple[tuple[BoolArray, IntArray, IntArray], ...] = (
        (np.ones(n, dtype=np.bool_), np.full(n, PENDING, dtype=np.int64), txn.created_ms),
        (final_emitted, txn.outcome, final_ms),
        (refund_emitted, np.full(n, REFUNDED, dtype=np.int64), refund_ms),
    )
    indexes = [np.flatnonzero(emitted) for emitted, _, _ in kinds]
    txn_index = np.concatenate(indexes)
    status = np.concatenate([codes[i] for (_, codes, _), i in zip(kinds, indexes, strict=True)])
    event_ms = np.concatenate([at[i] for (_, _, at), i in zip(kinds, indexes, strict=True)])
    n_events = len(txn_index)
    # Shuffled so that identifiers carry no ordering information.
    event_number = rng.permutation(n_events).astype(np.int64)

    # Arrival order: a few events are delivered late (out of order) and a few are repeated.
    delayed = rng.random(n_events) < OUT_OF_ORDER_SHARE
    arrival_ms = event_ms + np.where(
        delayed, _uniform_ms(rng, OUT_OF_ORDER_DELAY_RANGE_MS, n_events), 0
    )
    dup = np.flatnonzero(rng.random(n_events) < DUPLICATE_SHARE)
    dup_arrival_ms = arrival_ms[dup] + _uniform_ms(rng, DUPLICATE_RETRY_RANGE_MS, len(dup))

    deliveries = pl.DataFrame(
        {
            "event_number": np.concatenate([event_number, event_number[dup]]),
            "txn_index": np.concatenate([txn_index, txn_index[dup]]),
            "status_code": np.concatenate([status, status[dup]]),
            "event_ms": np.concatenate([event_ms, event_ms[dup]]),
            "arrival_ms": np.concatenate([arrival_ms, dup_arrival_ms]),
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
            "amount_minor": txn.amount_minor,
            "reason_code": txn.reason,
            "created_ms": txn.created_ms,
        }
    )
    carries_reason = pl.col("status_code").is_in([DECLINED, FAILED, EXPIRED])
    config.output_dir.mkdir(parents=True, exist_ok=True)
    (
        deliveries.lazy()
        .join(transactions.lazy(), on="txn_index", how="inner")
        .sort("arrival_ms", "event_number")
        .select(
            _prefixed_id("evt", "event_number", 10).alias("event_id"),
            _prefixed_id("txn", "txn_index", 9).alias("transaction_id"),
            _prefixed_id("mrc", "merchant_code", 3).alias("merchant_id"),
            _names("country_code", COUNTRIES).alias("country"),
            _names("country_code", CURRENCIES).alias("currency"),
            pl.col("amount_minor"),
            _names("method_code", METHODS).alias("payment_method"),
            _names("card_brand_code", CARD_BRANDS).alias("card_brand"),
            _names("psp_code", PSPS).alias("psp"),
            _names("status_code", STATUSES).alias("status"),
            pl.when(carries_reason)
            .then(_names("reason_code", REASONS))
            .otherwise(None)
            .alias("decline_reason"),
            _timestamp("created_ms").alias("created_at"),
            _timestamp("event_ms").alias("event_at"),
        )
        .collect()
        .write_ndjson(config.output_dir / WEBHOOKS_FILE)
    )
    _write_reference_files(config, merchants)

    observed = np.where(refund_emitted, REFUNDED, np.where(final_emitted, txn.outcome, PENDING))
    summary: dict[str, Any] = {
        "n_transactions": n,
        "n_events_unique": n_events,
        "n_duplicate_deliveries": len(dup),
        "n_deliveries": n_events + len(dup),
        "n_out_of_order_deliveries": int(delayed.sum()),
        "status_counts": {
            name: int((observed == code).sum()) for code, name in enumerate(STATUSES)
        },
    }
    shares = " ".join(f"{name}={count / n:.1%}" for name, count in summary["status_counts"].items())
    logger.info(
        "generation finished window=%s..%s transactions=%d unique_events=%d deliveries=%d "
        "duplicates=%d delayed=%d output_dir=%s",
        config.start_date,
        config.end_date,
        n,
        n_events,
        summary["n_deliveries"],
        len(dup),
        summary["n_out_of_order_deliveries"],
        config.output_dir,
    )
    logger.info("final status mix %s", shares)
    return summary


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
    parser.add_argument("--transactions", type=int, default=defaults.n_transactions)
    parser.add_argument("--end-date", type=date.fromisoformat, default=defaults.end_date)
    parser.add_argument("--days", type=int, default=defaults.days)
    parser.add_argument("--output-dir", type=Path, default=defaults.output_dir)
    args = parser.parse_args(argv)
    generate(
        GeneratorConfig(
            seed=args.seed,
            n_transactions=args.transactions,
            end_date=args.end_date,
            days=args.days,
            output_dir=args.output_dir,
        )
    )
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    raise SystemExit(main())
