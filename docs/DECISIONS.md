# Decisions

Design decisions and their trade offs, in the order they were made.

## D1. Stack

Python 3.11, Polars (lazy API preferred) for every transformation, Parquet for the staging and mart
layers, Streamlit and Plotly for the dashboard. NumPy is used only for seeded random generation in
`data_gen`. Top level dependencies are pinned to exact versions in `requirements.txt` and
`requirements-dev.txt` so the image builds reproducibly.

## D2. Static typing scope

`mypy --strict` covers `data_gen`, `pipeline` and `analytics`. The Streamlit app is excluded.

Trade off: Streamlit and Plotly expose loosely typed, heavily overloaded APIs, so strict mode in
`app/` would mostly produce casts and ignores without catching real defects. The app is kept thin
(it only reads marts and calls typed functions in `analytics`), so the logic that matters stays
under strict typing. Ruff, including the docstring rules, still applies to `app/`.

## D3. Container topology

One image, two Compose services sharing the named volume `data` mounted at `/app/data`. The
`pipeline` service runs `python -m pipeline.run` and exits; the `app` service starts only when the
pipeline completed successfully. This separates the batch job from the serving process, as it would
be in production, while keeping the reviewer experience to one command. The container runs as a non
root user, and the healthcheck is written in Python because slim images have no curl.

## D4. Synthetic raw data

`data_gen/generate.py` plays the upstream system and shares no code with the pipeline; the file
contract is the only interface. This replaces the first generator design (see the worklog).

- **`data/raw/webhooks.jsonl`**: grain is one row per webhook delivery, which is one event per
  status change plus about 1% repeated deliveries of the same `event_id`. Fields: `event_id`,
  `transaction_id`, `merchant_id`, `country`, `currency`, `amount_minor`, `payment_method`,
  `card_brand`, `psp`, `status`, `decline_reason`, `created_at`, `event_at` (ISO 8601 UTC).
- **`data/raw/merchants.csv`**: 120 merchants with `country`, `category` and `size_tier`
  (enterprise: top 10 by volume, mid: next 30, small: the rest). Volume is lognormal, so the top
  10 merchants carry more than 40% of the transactions.
- **`data/raw/psp_fees.csv`**: one row per PSP, country and payment method. `pct_fee` is a
  percentage of the amount (2.9 means 2.9%) and `fixed_fee_usd` is charged per transaction.
- **`data/raw/planted_anomalies.json`**: ground truth for the three planted anomalies.
- **Cards**: one payment method `card` with `card_brand` visa or mastercard, so a method level
  view is not split by brand while brand stays available as a dimension.
- **PSPs**: PSP_A and PSP_B serve every country for the whole window; PSP_C and PSP_D serve
  Colombia only, for the last 45 days. Authorization differs by country, brand and PSP.
- **Lifecycle**: every transaction emits `pending` (`event_at` equals `created_at`); resolved ones
  emit `approved`, `declined`, `failed` or `expired`; 2% of approved ones later emit `refunded`
  (full refunds only). `decline_reason` is set on declined, failed and expired events.
- **Window**: the 90 local days ending on a fixed date, 2026-09-30. "The last 90 days" is pinned
  rather than read from the clock so the same seed always yields byte identical files.
- **Snapshot**: status changes after 2026-10-01 06:00 UTC are not emitted, so recent transactions
  can still be `pending` and OXXO vouchers created in the last 72 hours are unresolved.
- **Arrival order**: line order is arrival order. 3% of events are delivered 1 minute to 48 hours
  late, so a `pending` event can appear after the final status of its transaction.
- **Local time**: offsets come from the IANA time zones per day, because Chile changes to daylight
  saving time on 2026-09-06, inside the window.

## D5. Full scale by default

The generator defaults to 1,200,000 transactions (TiendaMax's real 400,000 per month over 90 days),
well above the required minimum of 55,000. `--transactions` shrinks it.

Why: the planted night time anomaly covers two hours of one weekend for one PSP. At 55,000
transactions that window holds about 5 transactions, too few to detect anything; at full scale it
holds 122. Small segments need real volume to be readable.

Trade off: a full scale run takes about 2 seconds but peaks at about 1.5 GB of memory and writes a
780 MB raw file, which can exhaust a Docker VM limited to 2 GB.
