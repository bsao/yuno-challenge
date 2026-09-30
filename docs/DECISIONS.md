# Decisions

Design decisions and their trade offs. Metric definitions are in D6 and D7.

## D1. Stack and tooling

Python 3.11; Polars (lazy where it helps) for every transformation; Parquet for staging and marts;
Streamlit and Plotly for the dashboard; NumPy only for seeded random generation. Top level
dependencies are pinned to exact versions.

`mypy --strict` covers `data_gen`, `pipeline` and `analytics`. **The Streamlit app is excluded**:
Streamlit and Plotly expose loosely typed APIs, so strict mode there would add casts and ignores
without catching real defects. The app is kept free of metric logic so everything that matters
stays typed. Ruff, including docstring rules, still applies to the app.

## D2. Containers

One image, two Compose services sharing the named volume `data` at `/app/data`. `pipeline` runs
`python -m pipeline.run` and exits; `app` starts only if it completed successfully. Batch and
serving are separated as they would be in production, and the reviewer still runs one command.
The container runs as a non root user; the healthcheck is Python because slim images have no curl.

## D3. Synthetic raw data

`data_gen/generate.py` plays the upstream system and shares no code with the pipeline; the files
are the only contract.

- **`webhooks.jsonl`**: one row per webhook delivery: one event per status change, plus about 1%
  repeated deliveries of the same `event_id`. Line order is arrival order, and 3% of events arrive
  1 minute to 48 hours late, so a `pending` can follow its own final status.
- **`merchants.csv`** (120 merchants, lognormal volume), **`psp_fees.csv`** (`pct_fee` is percent of
  the amount, `fixed_fee_usd` per transaction), **`planted_anomalies.json`** (ground truth).
- **Cards** are one method, `card`, with `card_brand` visa or mastercard.
- **PSPs**: PSP_A and PSP_B serve every country; PSP_C and PSP_D serve Colombia only, for the last
  45 days.
- **Lifecycle**: `pending`, then `approved`, `declined`, `failed` or `expired`; 2% of approved are
  later `refunded` (full refunds only).
- **Window**: 90 local days ending on a fixed date, 2026-09-30, so the same seed always gives byte
  identical files. Status changes after the snapshot (2026-10-01 06:00 UTC) are not emitted, so
  recent transactions can still be `pending`.
- **Volume**: 1,200,000 transactions by default (TiendaMax's real 400,000 per month), above the
  55,000 minimum. The planted night time anomaly covers two hours of one weekend for one PSP: about
  5 transactions at 55,000, 122 at full scale. Small segments need real volume.
  `--transactions` shrinks it.
- **Trade off**: at full scale the pipeline peaks at about 2.4 GB of memory, and so does
  `make check`, because one test runs the pipeline at full scale. A Docker VM limited to 2 GB is
  not enough.

## D4. Ingestion

`pipeline/ingest.py` writes `staging/transactions.parquet`, one row per `transaction_id`.

- **Deduplication**: deliveries sharing an `event_id` are identical retries; the first is kept.
- **Latest event wins by event time** (`event_at`), never by arrival order. Ties break on the
  lifecycle rank of the status (pending < final < refunded), then `event_id`.
- **Idempotent by full refresh**: staging is rebuilt from the whole raw file, so replayed or
  reordered deliveries give the same output (tested; reruns are byte identical). In production
  this becomes an incremental merge on `transaction_id` with the same ordering rule.
- **Money**: `amount_minor` stays an integer. `amount_usd = amount_minor / 10 ** exponent * rate`,
  exponents MXN 2, COP 2, CLP 0, fixed illustrative rates 1 MXN = 0.054, 1 COP = 0.00025,
  1 CLP = 0.00105 USD. They are not market rates; USD exists so countries can be summed.
- **Local time**: `created_at` converted with the IANA zone of the country (Chile changes to
  daylight saving time inside the window). Stored without a zone because one column cannot hold
  three. Weekday is ISO, 1 Monday to 7 Sunday. Transactions are bucketed by creation time.
- **Trade off**: deduplicated events are collected in memory. A streaming sink or the incremental
  merge removes that ceiling.

## D5. Marts and quality assertions

- **`fct_transactions.parquet`**: one row per transaction; staging plus `local_date` and merchant
  category and size tier.
- **`agg_daily.parquet`**: one row per local date x country x psp x payment_method, holding only
  additive measures: a count per final status and the approved and refunded USD sums.
- **No rates in the marts.** Rates are not additive, so averaging daily rates is wrong at any
  coarser grain. The analytics layer derives them from counts at read time. This also keeps the
  dependency one way: the pipeline knows statuses, analytics knows what they mean.
- **Assertions** (`pipeline/quality.py`) run before each stage writes, so a failure leaves no
  partial file. Staging: unique `transaction_id`; valid vocabularies; positive amounts; status mix
  inside wide guardrails (approved 55% to 90%, declined 8% to 30%, failed 0.5% to 8%, expired up to
  10%, pending and refunded up to 5%); `raw rows - duplicates == distinct events == sum of events
  per transaction` and `distinct raw transactions == staging rows`. Marts: unique grains; every
  count and USD sum of the aggregate reconciles with the fact table.
- **Gap**: the generator has no runtime assertion of its own; its output is validated by ingest.

## D6. Metric definitions

All formulas live in `analytics/metrics.py`, computed as Polars expressions over the additive
measures.

| Metric | Formula |
| --- | --- |
| `approved` | `n_approved + n_refunded` (a refund was authorized first) |
| `attempts` | `approved + n_declined + n_failed` (pending and expired excluded) |
| `auth_rate` | `approved / attempts` |
| `decline_rate`, `failure_rate` | `n_declined / attempts`, `n_failed / attempts` |
| `gmv_usd` | `approved_amount_usd + refunded_amount_usd` (gross) |
| `net_gmv_usd` | `gmv_usd - refunded_amount_usd` |
| `completion_rate` | `paid / (paid + expired)`, voucher methods (OXXO) only, `paid = n_approved + n_refunded` |
| `expiration_rate` | `expired / (paid + expired)`, voucher methods only |
| Wilson 95% interval | `center = (p + z²/2n) / (1 + z²/n)`, `half = z·sqrt(p(1-p)/n + z²/4n²) / (1 + z²/n)`, z = 1.96 |

- Every rate is returned with its sample size and Wilson interval; a zero denominator gives null.
- **Why Wilson**: the normal approximation leaves [0, 1] and collapses to zero width at 0% or
  100%, exactly where small segments mislead.
- **Why vouchers need their own rate**: an unpaid voucher expires, it is not declined, so OXXO
  shows 96.2% authorization while only 60.0% of vouchers are paid.

## D7. Anomaly detection

`analytics/anomalies.py`.
`z = (rate - baseline) / sqrt(baseline · (1 - baseline) / attempts)`.

| Detector | Grain | Baseline | Flag when |
| --- | --- | --- | --- |
| Daily decline rate | date x psp x country x method | pooled previous 14 days of the segment | z >= 3 and attempts >= 50 |
| Hourly reason rate | psp x country x local date x hour x reason | pooled previous 14 days, all hours | z >= 5, attempts >= 20, events >= 10 |
| Merchant against peers | merchant x metric (authorization, completion) | other merchants of the same country and category | attempts >= 50 and Wilson upper bound more than 10 points below peers |

- **Why an hourly detector**: a two hour outage barely moves a daily rate.
- **Why z >= 5 for it**: it scores about 53,000 combinations. At z >= 3 it raised 63 flags, 59 by
  chance; at z >= 5 only the planted window remains.
- **Pooled baseline**, not a mean of daily rates, so a low volume day cannot distort it.
- **Baselines include earlier anomalous days**, so z decays during a multi day incident (8.0, 6.9,
  5.7). Conservative; excluding flagged days is the production refinement.
- **Peers leave the merchant out.** A merchant alone in its group falls back to country peers.
- **Limitations**: merchant authorization is not adjusted for method mix; segments are assumed to
  trade every day, so 14 rows are 14 days.

## D8. Dashboard

- **No metric logic in the app**: it filters, caches and draws; numbers come from `analytics`.
- **Caching**: each `st.cache_data` function scans Parquet lazily and returns a small aggregate
  keyed by the filters, so the fact table is never held in the session.
- **Anomalies are scored on the whole window** (detectors need 14 days of history); the date
  filter only selects which flags are shown.
- **Each chart has a caption naming the decision it supports.** No dual axes; one hue for
  heatmaps; a PSP keeps its colour under any filter; red is reserved for anomalies. The palette
  was checked for colour vision deficiency; the light theme is pinned because it was validated
  there.
- **Merchant rankings need 200 attempts**, so tiny merchants cannot top a list.
- **Gaps**: the hour by weekday heatmap and the OXXO by merchant chart show the sample size but no
  interval. USD is shown in every view, including single country ones, because the aggregate
  stores USD only.
