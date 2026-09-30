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

Commit: `chore: scaffold project tooling, layout and container stack`

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
|---|---|
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
|---|---|---|
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
|---|---|---|
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
|---|---|---|
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
|---|---|---|---|
| a) PSP_C, Colombia, cards, 2026-09-01 to 2026-09-03 | decline rate doubles | 321 of 848 (37.9%) | 2,301 of 11,981 (19.2%) |
| b) Merchant `mrc_037`, OXXO | about 95% expiration | 4,539 of 4,730 (96.0%) | 26,455 of 74,557 (35.5%) |
| c) PSP_B, Mexico, 02:00 to 04:00, 2026-09-12 and 13 | network_timeout spike | 78 of 122 (63.9%) | 65 of 4,938 (1.3%) |
