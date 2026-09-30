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

`data_gen/generate.py` plays the upstream system and shares no code with the pipeline; the JSON
contract is the only interface.

- **Grain**: one JSON object per webhook delivery, newline delimited, partitioned by arrival date
  (`data/raw/events/received_date=YYYY-MM-DD/events.jsonl`). An `event_id` can repeat.
- **Fields**: `event_id`, `transaction_id`, `merchant_id`, `country`, `currency`, `amount_minor`,
  `payment_method`, `psp`, `card_brand`, `channel`, `status`, `error_code`, `occurred_at` (event
  time) and `received_at` (arrival time), both ISO 8601 UTC.
- **Lifecycle**: every transaction emits `pending`; resolved ones emit `approved`, `declined`,
  `failed` or `expired`; 3% of approved ones later emit `refunded` (full refunds only).
- **Volume**: 90 local days from 2026-06-01 at 400,000 transactions per 30 days (`--scale` shrinks
  it). Seeded, so the same seed yields byte identical files.
- **Snapshot**: deliveries arriving after the end of the window are dropped, so recent transactions
  can still be `pending` and OXXO vouchers created in the last 72 hours are unresolved.
- **Delivery defects**: 5% of events arrive late (1 minute to 48 hours), which produces out of
  order arrival; 2% of deliveries are repeated with the same `event_id` and a later `received_at`.
- **Planted patterns** (also written to `data/raw/_manifest.json`, so the analysis can be checked
  against ground truth): per country method baselines; two Colombian PSPs launched on 2026-07-16,
  `psp_cafetal` better and `psp_magdalena` worse than the incumbent `psp_andes`; from 2026-07-31 the
  largest Chilean merchant has an abandonment spike (UX) and the largest Mexican merchant a decline
  spike on `psp_azteca` (processing); `psp_norte` times out between 02:00 and 04:59 local time;
  amex cards decline more.
- **Trade off**: local time in the generator uses fixed UTC offsets, valid for June to August.
  The pipeline will use IANA time zones, which keeps the two code paths independent.

## D5. Full scale by default

The generator defaults to the real volume (about 1.2 million transactions over 90 days) so the
numbers a reviewer sees match the brief of 400,000 transactions per month.

Trade off: a full scale run peaks at about 1.7 GB of memory and writes about 805 MB of raw files,
which can exhaust a Docker VM limited to 2 GB. Reviewers on a small VM can pass `--scale` (for
example `--scale 0.25`) to shrink the dataset; every rate keeps its sample size and Wilson interval,
so a smaller run stays honest about its precision.
