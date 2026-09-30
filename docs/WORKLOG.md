# Worklog

How this prototype was built, step by step. Each entry records what the step delivered, the
decisions taken in it, how it was verified, and the commit that contains it. Design rationale lives
in [DECISIONS.md](DECISIONS.md); this file is the chronological trail.

## Working agreement

- The engineer owns the architecture; the AI assistant (Claude Code) implements one small step at a
  time and stops after each one with a summary, verification instructions and a proposed commit.
- Nothing counts as done until `make check` passes (ruff format check, ruff lint, mypy strict,
  pytest, in that order).
- Every step verifies its key numbers through a second, independent code path and reports both
  values side by side.
- Conventional Commits, one logical change per commit. The engineer reviews and approves every
  commit and push.
- Every step is checked against its specification with a coverage table, so gaps are visible.

## Step 0: scaffold

**Delivered**

- `pyproject.toml` (ruff rules E, F, I, B, UP, N, D, SIM, RUF with the Google docstring convention,
  line length 100; mypy strict for `data_gen`, `pipeline`, `analytics`; pytest), pinned
  `requirements.txt` and `requirements-dev.txt`, and a `Makefile` (`install`, `data`, `pipeline`,
  `test`, `lint`, `typecheck`, `check`, `app`, `up`, `down`).
- `Dockerfile` (python:3.11-slim, non root user, `make` installed) and `docker-compose.yml` with a
  one-shot `pipeline` service and an `app` service that waits for it and has a Python healthcheck.
- A stub `pipeline/run.py` that logs the four stages, a minimal Streamlit page, and docstring only
  modules for the rest of the layout.

**Decisions**: D1 (stack), D2 (mypy strict scope), D3 (container topology).

**Verification**

| Check | Result |
| --- | --- |
| `make check` | format, lint, mypy strict and pytest pass |
| `pipeline` container | exit code 0, four stage log lines |
| App health from the host (`curl /_stcore/health`) | `200 ok` |
| App health from the container (`docker inspect`) | `healthy` |
| Page opened in a browser | title "TiendaMax Payment Intelligence" |
| Container user and volume | runs as `app`, `/app/data` writable |

## Step 1: synthetic webhook generator

Commit: `feat(data-gen): add seeded synthetic webhook generator`

**Delivered**

- `data_gen/generate.py`: a seeded, vectorized generator that writes one JSON line per webhook
  delivery, partitioned by arrival date, plus `data/raw/_manifest.json` with ground truth counts
  and the planted patterns.
- `tests/test_generate.py`: determinism, manifest reconciliation, raw contract, duplicate payloads,
  out of order arrival and input validation.

**Decisions**: D4 (raw contract, lifecycle, snapshot cut-off, delivery defects, planted patterns),
D5 (full scale by default).

**Verification**: `make check` passes (9 tests). Full scale run: 4.7 seconds, 1.7 GB peak memory,
805 MB of raw files.

Double check, the manifest (computed per transaction in NumPy) against a plain `json` re-read of
the files that applies the "latest event by `occurred_at`" rule independently:

| Number | Manifest | JSON re-read |
| --- | --- | --- |
| Deliveries | 2,447,429 | 2,447,429 |
| Unique events | 2,399,487 | 2,399,487 |
| Duplicate deliveries | 47,942 | 47,942 |
| Transactions | 1,195,464 | 1,195,464 |
| Approved | 915,605 | 915,605 |
| Declined | 123,984 | 123,984 |
| Failed | 28,038 | 28,038 |
| Expired | 84,216 | 84,216 |
| Pending | 17,229 | 17,229 |
| Refunded | 26,392 | 26,392 |

**Superseded**: Step 2 replaced this generator with the architect's specification (different raw
contract, 120 merchants, 4 PSPs, three planted anomalies). The numbers above describe the old
design and are kept only as a record of the flow.

## Step 2: generator to specification, wired into the runner

Proposed commit: `feat(data-gen): generate webhooks, merchants, fees and planted anomalies`

**Delivered**

- `data_gen/generate.py` rewritten to the specified contract: `webhooks.jsonl`, `merchants.csv`,
  `psp_fees.csv` and `planted_anomalies.json` under `data/raw`.
- `pipeline/run.py`: the generate stage runs only when a raw file is missing; the generator logs a
  summary (window, counts, final status mix) at the end.
- Tests: 14 for the generator and 3 for the runner.

**Decisions**: D4 (rewritten), D5 (rewritten).

**Coverage against the step specification**

| Requirement | Status | Evidence |
| --- | --- | --- |
| Seeded and deterministic (numpy Generator), written with Polars | Met | byte identical files for the same seed (test) |
| `webhooks.jsonl` with the 13 listed fields, UTC | Met | field list asserted per row (test) |
| `merchants.csv` and `psp_fees.csv` with the listed columns | Met | headers asserted (test) |
| At least 55,000 transactions over the last 90 days | Met | 1,200,000 over 2026-07-03 to 2026-09-30 |
| Mexico, Colombia, Chile in MXN, COP, CLP | Met | currency matches country on every row (test) |
| Visa and Mastercard everywhere, OXXO and SPEI in Mexico, PSE in Colombia, Webpay in Chile | Met | modelled as method `card` plus `card_brand` (D4) |
| 4 PSPs with different authorization rates per segment | Met | country, brand and PSP effects |
| Two PSPs live in Colombia only for the last 45 days | Met | PSP_C and PSP_D, first local day 2026-08-17 (test) |
| Final states roughly approved 75%, declined 18%, failed 3% | Met, roughly | 74.3%, 16.8%, 3.0%; expired 3.7%, refunded 1.4%, pending 0.8% |
| The six listed reasons | Met | reason vocabulary per status asserted (test) |
| 120 merchants, long tail | Met | top 10 merchants above 40% of volume (test) |
| Hourly and weekday seasonality | Met | 20:00 above three times 03:00; Friday above Sunday (test) |
| About 1% duplicated events, some out of order | Met | 23,814 duplicates (0.99%); 72,297 delayed deliveries |
| Anomalies a, b, c planted and written as ground truth | Met | see below |
| Wired into `pipeline/run.py`, generate only if missing | Met | second run logs `status=skipped` (test) |
| Summary logged at the end | Met | two log lines from `data_gen.generate` |

**Verification**: `make check` passes (17 tests). Full scale run: 1.8 seconds, 1.5 GB peak memory,
780 MB raw file.

Double check, the generator's summary (computed per transaction in NumPy) against a plain `json`
re-read of the file that applies the "latest event by `event_at`" rule independently:

| Number | Generator | JSON re-read |
| --- | --- | --- |
| Deliveries | 2,430,825 | 2,430,825 |
| Unique events | 2,407,011 | 2,407,011 |
| Duplicate deliveries | 23,814 | 23,814 |
| Transactions | 1,200,000 | 1,200,000 |
| Approved | 74.3% | 891,334 (74.3%) |
| Declined | 16.8% | 201,287 (16.8%) |
| Failed | 3.0% | 35,795 (3.0%) |
| Expired | 3.7% | 44,383 (3.7%) |
| Pending | 0.8% | 10,095 (0.8%) |
| Refunded | 1.4% | 17,106 (1.4%) |

Planted anomalies, ground truth against what the re-read measures:

| Anomaly | Planted | Measured inside | Measured outside |
| --- | --- | --- | --- |
| a) PSP_C, Colombia, cards, 2026-09-01 to 2026-09-03 | decline rate doubles | 321 of 848 (37.9%) | 2,301 of 11,981 (19.2%) |
| b) Merchant `mrc_037`, OXXO | about 95% expiration | 4,539 of 4,730 (96.0%) | 26,455 of 74,557 (35.5%) |
| c) PSP_B, Mexico, 02:00 to 04:00, 2026-09-12 and 13 | network_timeout spike | 78 of 122 (63.9%) | 65 of 4,938 (1.3%) |

## Step 3: ingestion and quality assertions

**Delivered**

- `pipeline/ingest.py`: reads `webhooks.jsonl` lazily with an explicit schema, removes duplicate
  deliveries, keeps the latest event per transaction, derives `amount_usd` and local time, and
  writes `data/staging/transactions.parquet`.
- `pipeline/quality.py`: five assertions that raise `DataQualityError`.
- `pipeline/run.py`: the ingest stage is wired in after generate.
- Tests: 6 for ingestion (tiny in memory fixture) and 9 for the quality checks.

**Decisions**: D6 (deduplication, latest status rule, money, local time), D7 (quality assertions).

**Coverage against the step specification**

| Requirement | Status | Evidence |
| --- | --- | --- |
| Reads the raw files with Polars | Met for `webhooks.jsonl` | `merchants.csv` and `psp_fees.csv` are not needed for staging and are left for the marts |
| Writes `data/staging/transactions.parquet`, one row per transaction | Met | 1,200,000 rows, unique key asserted |
| Latest event wins, duplicates removed | Met | fixture tests: duplicate, out of order, reversed arrival, timestamp tie |
| `final_status`, `amount_minor`, `amount_usd` | Met | USD values hand computed for MXN, COP and CLP (test) |
| Local timestamp, local hour and weekday | Met | hand computed for the three zones, including Chile's daylight saving change (test) |
| Raises on duplicate `transaction_id` | Met | `check_unique` (test) |
| Raises on invalid enum values | Met | `check_enum_values` (test) |
| Raises on negative amounts | Met | `check_positive_amounts` also rejects zero and null (test) |
| Raises on status mix outside expected ranges | Met | `check_status_mix` with the D7 guardrails (test) |
| Raises on row count reconciliation between raw and staging | Met | `check_row_count_reconciliation` (test) |
| Logs duplicates and out of order events handled | Met | `duplicates_removed=23814 out_of_order_events_handled=33197` |
| Pytest tests for deduplication with a tiny in memory fixture | Met | `tests/test_ingest.py`, 12 deliveries across 5 transactions |

**Verification**: `make check` passes (32 tests). Full scale pipeline run (ingest on existing raw
data): 6.3 seconds, 2.3 GB peak memory.

Double check, staging (Polars, lazy scan of the Parquet file) against a pure Python recomputation
from the raw file that implements the same ordering rule without Polars:

| Number | Staging (Polars) | Raw re-read (Python) |
| --- | --- | --- |
| Deliveries | 2,430,825 (log) | 2,430,825 |
| Unique events | 2,407,011 | 2,407,011 |
| Duplicates removed | 23,814 (log) | 23,814 |
| Out of order events | 33,197 (log) | 33,197 |
| Transactions | 1,200,000 | 1,200,000 |
| Approved | 891,334 | 891,334 |
| Declined | 201,287 | 201,287 |
| Failed | 35,795 | 35,795 |
| Expired | 44,383 | 44,383 |
| Pending | 10,095 | 10,095 |
| Refunded | 17,106 | 17,106 |
| Approved amount in USD | 37,425,323.52 | 37,425,323.52 |

## Step 4: marts and metric definitions

**Delivered**

- `pipeline/transform.py`: `fct_transactions.parquet` and `agg_daily.parquet`, with reconciliation
  assertions between staging, the fact table and the aggregate.
- `analytics/metrics.py`: `performance(lf, dims)`, the Wilson interval as a scalar reference and as
  Polars expressions.
- `pipeline/run.py`: the transform stage is wired in and the run ends with the highlight log lines.
- `pipeline/quality.py`: composite key uniqueness and a totals reconciliation check.
- Tests: 12 for the metrics, 3 for the transform, 2 more for quality (49 in total).

**Decisions**: D8 (marts hold additive measures only), D9 (metric definitions).

**Coverage against the step specification**

| Requirement | Status | Evidence |
| --- | --- | --- |
| `data/marts/fct_transactions.parquet` | Met | 1,200,000 rows, 20 columns |
| `data/marts/agg_daily.parquet`, grain date x country x psp x payment_method | Met | 1,440 rows, grain uniqueness asserted |
| `performance(lf, dims)` returns attempts, approved, auth_rate, wilson_low, wilson_high, gmv_usd, net_gmv_usd | Met | tests with hand computed values |
| `completion_rate` for voucher methods | Met | null for non voucher groups; `voucher_attempts` added as its sample size |
| All as Polars expressions | Met | lazy in, lazy out; nothing is collected inside `performance` |
| Unit tests for the authorization rate with hand computed cases | Met | refunded counted as approved, pending and expired excluded, rollup from counts, zero attempts |
| Unit tests for the Wilson interval with hand computed cases | Met | 8/10, 0/10, 10/10, 50/100, 75/100, worked in the test module docstring |
| Log the best and worst method per country | Met | three `highlight=method_ranking` lines |
| Log a PSP comparison for Colombia cards | Met | four `highlight=psp_comparison` lines, like for like since 2026-08-17 |

**Verification**: `make check` passes (49 tests). Full pipeline on existing raw data: 6.7 seconds,
2.4 GB peak memory.

Highlights logged by the run (authorization rate, Wilson 95% interval, sample size):

| Country | Best method | Worst method |
| --- | --- | --- |
| Chile | webpay 88.2% [88.0%, 88.5%] n=72,697 | card 83.2% [83.0%, 83.4%] n=169,625 |
| Colombia | pse 81.1% [80.9%, 81.4%] n=89,573 | card 71.0% [70.8%, 71.2%] n=168,473 |
| Mexico | oxxo 96.2% [96.0%, 96.3%] n=48,293, but completion 60.0% n=77,433 | card 74.9% [74.8%, 75.0%] n=476,089 |

| Colombia cards since 2026-08-17 | Authorization rate | Wilson 95% | Attempts | GMV USD |
| --- | --- | --- | --- | --- |
| PSP_C | 76.3% | [75.5%, 77.0%] | 12,642 | 399,061 |
| PSP_A | 72.3% | [71.8%, 72.7%] | 33,498 | 1,009,644 |
| PSP_B | 68.9% | [68.4%, 69.5%] | 25,388 | 722,490 |
| PSP_D | 65.1% | [64.2%, 65.9%] | 12,542 | 337,553 |

The intervals do not overlap, so the ordering is not noise: PSP_C beats both incumbents and PSP_D
is below both. PSP_C's figure includes its three planted bad days.

Double check, `performance` over `agg_daily` (lazy) against an eager recomputation from staging
that uses neither the marts nor `performance`:

| Segment | Attempts (agg / staging) | Approved (agg / staging) | Authorization rate | Net GMV USD (agg / staging) |
| --- | --- | --- | --- | --- |
| CL card | 169,625 / 169,625 | 141,127 / 141,127 | 83.1994% both | 5,000,136.88 / 5,000,136.88 |
| CL webpay | 72,697 / 72,697 | 64,142 / 64,142 | 88.2320% both | 2,272,260.49 / 2,272,260.49 |
| CO card | 168,473 / 168,473 | 119,644 / 119,644 | 71.0167% both | 4,865,425.15 / 4,865,425.15 |
| CO pse | 89,573 / 89,573 | 72,668 / 72,668 | 81.1271% both | 2,949,162.29 / 2,949,162.29 |
| MX card | 476,089 / 476,089 | 356,650 / 356,650 | 74.9125% both | 15,587,004.59 / 15,587,004.59 |
| MX oxxo | 48,293 / 48,293 | 46,439 / 46,439 | 96.1609% both | 2,040,642.45 / 2,040,642.45 |
| MX spei | 120,772 / 120,772 | 107,770 / 107,770 | 89.2343% both | 4,710,691.66 / 4,710,691.66 |

The Wilson bounds agree to four decimals on every segment (expression path against the scalar
reference), and the OXXO completion rate is 59.9731% on both paths.

## Step 5: failure analysis and anomaly detection

Proposed commit: `feat(analytics): add failure analysis and anomaly detection`

**Delivered**

- `analytics/anomalies.py`: five descriptive views and three detectors.
- `analytics/metrics.py`: `outcome_rates(lf, dims)` with `decline_rate`, `failure_rate` and
  `expiration_rate`; `performance` also returns `voucher_paid`.
- Tests: 12 for the anomalies module (hand computed z scores and peer comparisons, plus the planted
  anomaly test on a full scale pipeline run) and 1 more for the metrics (62 in total).

**Decisions**: D10.

**Coverage against the step specification**

| Requirement | Status | Evidence |
| --- | --- | --- |
| Decline and failure reasons by volume and share per country and method | Met | `reason_breakdown` |
| Hour x weekday decline heatmap data | Met | `decline_heatmap`, 168 rows (7 x 24), optional split by extra dimensions |
| Decline rate by USD amount bucket | Met | `decline_rate_by_amount_bucket` |
| OXXO expiration by amount bucket and by merchant | Met | `oxxo_expiration_by_amount_bucket`, `oxxo_expiration_by_merchant` |
| Daily decline rate per psp x country x payment_method against a trailing 14 day baseline, flag when z >= 3 and attempts >= 50 | Met | `score_daily_decline_rate` |
| Flag merchants whose authorization or completion rate is far below country and category peers | Met | `score_merchants_against_peers` |
| Test that loads `planted_anomalies.json` and asserts every planted anomaly is detected | Met | `test_every_planted_anomaly_is_detected` |

One addition beyond the listed detectors: `score_hourly_reason_rate`. Planted anomaly c (timeouts
between 02:00 and 04:00 on one weekend) is invisible to the daily rule, so "every planted anomaly
is detected" needs an hourly rule (D10).

**Verification**: `make check` passes (62 tests, 11 seconds). The planted anomaly test runs the
whole pipeline at full scale, so `make check` now needs about 2.4 GB of memory.

Detection results on the full dataset:

| Detector | Rows scored | Flags | Planted | Chance flags |
| --- | --- | --- | --- | --- |
| Daily decline rate | 1,112 | 3 | 3 days of anomaly a | 0 |
| Hourly reason rate | 52,822 | 4 | 4 hour slots of anomaly c | 0 |
| Merchant against peers | 182 | 1 | the merchant of anomaly b | 0 |

Double check, each detector against a recomputation from staging in plain Python (no marts, no
metrics module, no detector code):

| Flag | Detector | Staging recompute |
| --- | --- | --- |
| a) 2026-09-01 | 268 attempts, rate 0.3955, baseline 0.2006, z 7.9681 | 268, 0.3955, 0.2006, 7.9681 |
| a) 2026-09-02 | 285 attempts, rate 0.3825, baseline 0.2145, z 6.9094 | 285, 0.3825, 0.2145, 6.9094 |
| a) 2026-09-03 | 286 attempts, rate 0.3706, baseline 0.2297, z 5.6662 | 286, 0.3706, 0.2297, 5.6662 |
| c) 2026-09-12 02h | 33 attempts, 27 timeouts, baseline 0.01167, z 43.139 | 33, 27, 0.01167, 43.139 |
| c) 2026-09-12 03h | 28 attempts, 20 timeouts, baseline 0.01167, z 34.618 | 28, 20, 0.01167, 34.618 |
| c) 2026-09-13 02h | 23 attempts, 13 timeouts, baseline 0.01260, z 23.765 | 23, 13, 0.01260, 23.765 |
| c) 2026-09-13 03h | 28 attempts, 18 timeouts, baseline 0.01260, z 29.905 | 28, 18, 0.01260, 29.905 |
| b) mrc_037 completion | 4,730 vouchers, rate 0.0404, 7,453 peer vouchers, peer rate 0.6354 | 4,730, 0.0404, 7,453, 0.6354 |

**Findings from the descriptive views**

- Reasons: `insufficient_funds` leads everywhere (39% of Mexican card failures), then
  `card_declined` (31%) and `fraud_suspected` (18%); technical failures are about 12%.
- Ticket size: the decline rate is flat, 17.1% for tickets under 10 USD to 18.2% above 250 USD.
  OXXO expiration is also flat across ticket sizes (39% to 41%). Ticket size is not a driver in
  this dataset, and the generator plants no such effect.
- Heatmap: the decline rate varies only between 15.5% and 19.5% across the 168 weekday and hour
  cells. The highest failure rates are Saturday 03:00 (5.8%) and Saturday and Sunday 02:00 (4.7%),
  the footprint of anomaly c.
- OXXO by merchant: `mrc_037` expires 96.0% of 4,730 vouchers; the next merchant is at 51.6% on
  only 62 vouchers.

## Step 6: dashboard

Proposed commit: `feat(app): add Streamlit dashboard with overview, performance, failures and anomalies`

**Delivered**

- `app/streamlit_app.py`: sidebar filters (date range, country) and six tabs.
- `.streamlit/config.toml` (light theme), `PYTHONPATH` set in the `Dockerfile` and in `make app` so
  the app can import `analytics` and `pipeline`.
- Screenshots of the four implemented tabs in `docs/screenshots/`.

**Decisions**: D11.

**Coverage against the step specification**

| Requirement | Status | Evidence |
| --- | --- | --- |
| Reads the marts with `st.cache_data` | Met | six cached loaders over lazy Parquet scans |
| Sidebar filters: date range and country | Met | both applied to every view |
| Overview: attempts, authorization rate, GMV USD, net GMV, daily trend | Met | four tiles, daily authorization rate with Wilson band, daily net GMV |
| Performance: country x method heatmap with sample size | Met | rate and `n` in each cell, completion rate for OXXO |
| Performance: PSP comparison with Wilson intervals | Met | dot and interval per country and PSP, plus a table view |
| Performance: top and worst merchants | Met | 10 each, minimum 200 attempts |
| Failures: reasons, hour x weekday heatmap, amount buckets, OXXO expirations | Met | five charts and a table |
| Anomalies: flagged table and a chart highlighting the anomalous days | Met | 8 flags; daily decline rate with baseline and marked days |
| Merchant Health and Cost: placeholders | Met | one info box each |
| Every chart has a one line caption stating the business decision | Met | 10 charts and the two merchant tables, each with a "Decision:" caption |
| Rebuild with docker compose and confirm it works | Met | see below |

**Verification**

- `make check` passes (62 tests).
- `docker-compose down -v` then `docker-compose up --build`: the `pipeline` container generated,
  ingested and transformed at full scale and exited 0; the `app` container became healthy;
  `curl http://localhost:8501/_stcore/health` returned 200.
- Every tab opened in a browser against the container: Performance 2 charts and 3 tables, Failures
  5 charts and 1 table, Anomalies 1 chart and 1 table, no exception element on any tab.
- The standalone `docker-compose` binary was used because the local Docker CLI has no compose
  plugin; it is the same Compose engine as `docker compose`.

Double check, the Overview tiles read from the page served by the container against an eager
recomputation from the local staging file (different machine state, different code path):

| Number | Dashboard (container) | Staging recompute (local) |
| --- | --- | --- |
| Attempts | 1,145,522 | 1,145,522 |
| Authorization rate | 79.3% | 79.3% |
| GMV (USD) | $38,143,368 | $38,143,368 |
| Net GMV (USD) | $37,425,324 | $37,425,324 |

The container's pipeline log also matches the local run line for line: 2,430,825 deliveries,
23,814 duplicates removed, 33,197 out of order events, 1,200,000 transactions, 1,440 aggregate rows
and the same four Colombia PSP lines.
