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

## D6. Ingestion: deduplication and latest status

`pipeline/ingest.py` writes `data/staging/transactions.parquet`, grain: one row per
`transaction_id`.

- **Deduplication**: deliveries that share an `event_id` are identical retries; the first arrival
  is kept.
- **Latest event wins by event time**: the winner has the highest `event_at`. Arrival order is
  ignored, so a `pending` event delivered after the final status cannot overwrite it. Ties on
  `event_at` break on the lifecycle rank of the status (pending < approved, declined, failed,
  expired < refunded), then on `event_id`, so the result is deterministic.
- **Idempotency by full refresh**: every run rebuilds staging from the whole raw file, so replayed
  or reordered deliveries give the same output (tested). In production this becomes an incremental
  merge keyed on `transaction_id` that applies the same ordering rule; the rule, not the refresh
  strategy, is what guarantees correctness.
- **Columns**: `transaction_id`, `merchant_id`, `country`, `currency`, `amount_minor`,
  `payment_method`, `card_brand`, `psp`, `final_status`, `decline_reason`, `created_at` and
  `updated_at` (UTC), `n_events`, `amount_usd`, `created_at_local`, `local_hour`, `local_weekday`.
- **Money**: `amount_minor` stays an integer in the currency's minor unit. `amount_usd` is
  `amount_minor / 10 ** exponent * rate`, with exponents MXN 2, COP 2, CLP 0 and fixed,
  illustrative rates (1 MXN = 0.054 USD, 1 COP = 0.00025 USD, 1 CLP = 0.00105 USD). These are not
  market rates; `amount_usd` exists only so cross country views can be summed.
- **Local time**: `created_at` is converted with the IANA zone of the country (America/Mexico_City,
  America/Bogota, America/Santiago). `created_at_local` is stored without a zone because one
  column cannot hold several; `local_weekday` is ISO (1 Monday to 7 Sunday). Transactions are
  bucketed by creation time, not by their last update.
- **Out of order count**: an event is out of order when it arrives after a later event (by
  `event_at`) of the same transaction. This is lower than the number of delayed deliveries the
  generator reports, because a delayed event that is not overtaken stays in order.
- **Trade off**: the stage collects the deduplicated events in memory. At full scale the pipeline
  run peaks at about 2.3 GB. A streaming sink or the incremental merge removes that ceiling.

## D7. Quality assertions on staging

`pipeline/quality.py` raises `DataQualityError` and nothing is written when a check fails:
duplicate `transaction_id`; values outside the vocabularies (null allowed only for `card_brand` and
`decline_reason`); null, zero or negative amounts; status mix outside guardrails; and row count
reconciliation between raw and staging.

- **Status mix guardrails** (share of transactions): approved 55% to 90%, declined 8% to 30%,
  failed 0.5% to 8%, expired up to 10%, pending up to 5%, refunded up to 5%. They are wide on
  purpose: they catch a broken batch, not normal variation or a planted anomaly.
- **Reconciliation identities**: `raw rows - duplicates == distinct event_id == sum of n_events in
  staging`, and `distinct transaction_id in raw == staging rows`. The raw counts come from a
  separate aggregation of the raw file, independent of the deduplication path.

## D8. Marts: additive measures only

`pipeline/transform.py` writes two marts.

- **`fct_transactions.parquet`**: one row per `transaction_id`. The staging columns plus
  `local_date`, `merchant_category` and `merchant_size_tier` (many to one join to
  `merchants.csv`, validated, so the grain cannot fan out).
- **`agg_daily.parquet`**: one row per `date` x `country` x `psp` x `payment_method`, where `date`
  is the local creation date. Columns: `n_transactions`, one count per final status
  (`n_approved`, `n_declined`, `n_failed`, `n_expired`, `n_pending`, `n_refunded`),
  `approved_amount_usd` and `refunded_amount_usd`.
- **No rates in the marts.** Rates are not additive: averaging daily authorization rates gives a
  wrong answer at any coarser grain. The aggregate stores only counts and sums, and
  `analytics/metrics.py` derives every rate at read time. This also keeps the dependency one way:
  the pipeline knows statuses, the analytics layer knows what they mean.
- **Quality assertions**: unique `transaction_id`; fact rows equal staging rows; no transaction
  without merchant attributes; unique aggregate grain; the aggregate's transaction count, each
  status count and both USD sums reconcile with the fact table.
- **Amounts in the aggregate are USD only**, because the aggregate is read across countries.
  Per currency analysis uses `amount_minor` in the fact table.

## D9. Metric definitions

All formulas live in `analytics/metrics.py`; `performance(lf, dims)` returns them per combination
of dimensions, computed as Polars expressions over the additive measures.

| Metric | Formula |
| --- | --- |
| `approved` | `n_approved + n_refunded` (a refunded transaction was authorized first) |
| `attempts` | `approved + n_declined + n_failed` (pending and expired are excluded) |
| `auth_rate` | `approved / attempts` |
| `wilson_low`, `wilson_high` | Wilson 95% score interval of `approved` out of `attempts`, z = 1.96 |
| `gmv_usd` | `approved_amount_usd + refunded_amount_usd` (gross) |
| `net_gmv_usd` | `gmv_usd - refunded_amount_usd` |
| `voucher_attempts` | `paid + n_expired` over voucher methods (OXXO) only, `paid = n_approved + n_refunded` |
| `completion_rate` | `paid / voucher_attempts`, with its own Wilson interval |

- **Sample size next to every rate**: `attempts` for the authorization rate, `voucher_attempts`
  for the completion rate. A rate with a zero denominator is null, never 0.
- **Why Wilson**: the normal approximation produces bounds outside [0, 1] and collapses to a zero
  width interval at 0% or 100%, exactly where small segments mislead. Wilson stays inside [0, 1].
- **Why vouchers need a second rate**: an OXXO voucher that is never paid expires; it is not
  declined. Expired is excluded from the authorization rate, so OXXO shows 96.2% authorization
  while only 60.0% of vouchers are paid. Ranking methods by authorization rate alone would call
  OXXO the best method in Mexico; the completion rate is the number that matters for it.
- **Like for like PSP comparison**: PSP_C and PSP_D are live in Colombia for only the last 45 days,
  so the PSP comparison uses only the days on which every PSP of the segment was live.

## D10. Failure analysis and anomaly detection

`analytics/anomalies.py` chooses grains and rules; every rate still comes from
`analytics/metrics.py`, which gained three definitions:

| Metric | Formula |
| --- | --- |
| `decline_rate` | `n_declined / attempts` (issuer or risk refusal) |
| `failure_rate` | `n_failed / attempts` (technical error) |
| `expiration_rate` | `expired / (paid + expired)`, voucher methods only; the complement of `completion_rate` |

**Descriptive views**: reasons by volume and share per country and method (share of the declined
and failed transactions of that country and method); decline and failure rate per local weekday and
hour; per USD ticket bucket (0-10, 10-25, 25-50, 50-100, 100-250, 250+, left closed); OXXO
expiration per ticket bucket and per merchant.

**Detection rules**

| Detector | Grain | Baseline | Flag when |
| --- | --- | --- | --- |
| Daily decline rate | date x psp x country x payment_method | pooled rate of the previous 14 days of the segment | `z >= 3` and `attempts >= 50` |
| Hourly reason rate | psp x country x local date x local hour x reason | pooled rate of the previous 14 days (all hours) | `z >= 5`, `attempts >= 20` and `events >= 10` |
| Merchant against peers | merchant x metric (authorization, completion) | other merchants of the same country and category | `attempts >= 50` and Wilson upper bound more than 10 points below the peer rate |

`z = (rate - baseline_rate) / sqrt(baseline_rate * (1 - baseline_rate) / attempts)`.

- **Why a second, hourly detector**: a two hour outage moves the daily rate of its segment by only
  a few points, so the daily rule cannot see it. The hourly rule looks at one reason in one hour.
- **Why z >= 5 for the hourly rule**: it scores about 53,000 slot and reason combinations. At
  z >= 3 it raised 63 flags, 59 of them chance (all between 3.0 and 4.6). At z >= 5 the expected
  number of chance flags is below one, and only the planted window remains. The daily rule scores
  1,112 rows, where z >= 3 produced no chance flag.
- **Pooled baseline, not a mean of daily rates**: pooling weights each day by its volume, so a low
  volume day cannot distort the baseline. A segment is scored only after 14 days of history.
- **Baselines include earlier anomalous days.** During a multi day incident the z score decays
  (7.97, 6.91, 5.67 over the three planted days). That is conservative; excluding flagged days
  from the baseline is the production refinement.
- **Peers leave the merchant out**, so a large merchant is not compared with itself. A merchant
  alone in its country and category falls back to country peers, and `peer_scope` records which
  was used.
- **Limitation**: the merchant authorization rate is not adjusted for payment method mix.

## D11. Dashboard

`app/streamlit_app.py` holds no metric logic: every number comes from `analytics/metrics.py` or
`analytics/anomalies.py`. The page only filters, caches and draws.

- **Caching**: each `st.cache_data` function scans the Parquet marts lazily and returns a small
  aggregated frame keyed by the sidebar filters. The 1.2 million row fact table is never loaded
  into the session, so a filter change costs one Polars scan and a repeated view costs nothing.
- **Filters**: date range (local dates) and country. The anomaly detectors need 14 days of
  history, so they always score the whole window for the selected countries; the date range then
  selects which flags are shown.
- **One caption per chart** names the business decision the chart supports, so the page reads as a
  set of decisions rather than a set of plots.
- **Sample size everywhere**: rates carry `n` in the cell, the label or the hover, and Wilson
  intervals as a band, error bars or text.
- **Chart choices**: no dual axes (authorization rate and GMV are two charts); a single hue
  sequential scale for heatmaps; PSP colours are fixed per PSP so a filter never repaints them;
  red is reserved for anomalous days. The four series palette was checked for colour vision
  deficiency separation; two of its colours have low contrast on the light surface, so those
  charts also carry direct labels or a table view.
- **Light theme pinned** in `.streamlit/config.toml`, because the palette was validated against
  the light surface only.
- **Merchant ranking** requires at least 200 attempts, so a merchant with a handful of
  transactions cannot top either list.
- **Typing trade off** (see D2): the app is outside mypy strict, which is acceptable because it
  contains presentation code only.
