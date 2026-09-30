# Worklog

How the prototype was built, in order. Rationale is in [DECISIONS.md](DECISIONS.md); findings are
in [ANALYSIS.md](ANALYSIS.md).

## Working agreement

- The engineer owns the architecture and writes each step's specification. The AI assistant
  (Claude Code) implements one step, stops, reports, and waits.
- A step is done when `make check` passes (ruff format, ruff lint, mypy strict, pytest) and every
  requirement of its specification is checked off.
- Key numbers are verified through a second, independent code path and both values are reported.
- The engineer reviews and approves every commit.

## Steps

| # | Step | Delivered | Decisions |
| --- | --- | --- | --- |
| 1 | Scaffold | Tooling, layout, Dockerfile, Compose stack, stub runner and page | ADR 4, 7 |
| 2 | Generator | `webhooks.jsonl`, `merchants.csv`, `psp_fees.csv`, `planted_anomalies.json`; wired into the runner | ADR 6 |
| 3 | Ingestion | `staging/transactions.parquet`, deduplication, latest status, quality assertions | ADR 2 |
| 4 | Marts and metrics | `fct_transactions`, `agg_daily`, `performance()` with Wilson intervals, highlight logs | ADR 2, 3 |
| 5 | Failure analysis | Five descriptive views, three anomaly detectors, planted anomaly test | ADR 5 |
| 6 | Dashboard | Filters and tabs, rebuilt and verified in Docker | ADR 4 |
| 7 | Health and cost | Merchant health score; cost per sale and traffic shift simulation; their tabs | ADR 3 |
| 8 | Validation | Every Makefile target run from a fresh clone; one defect fixed | below |
| 9 | Brazil | BRL, PIX and Boleto added end to end; every step re-verified | ADR 6 |
| 10 | Documentation | README, ADRs, and a CFO memo generated from the marts | |
| 11 | Realistic merchant health | Two planted merchant problems; a 30 day score with a processing or ux diagnosis; significance marks on the heatmap | ADR 8 |
| 12 | Reviewer pass | Clean clone, README followed literally, scored against the rubric | below |

A first generator, built before the specification of step 2 arrived, was replaced by it.

## Independent checks

Each number was computed twice, through code paths that share nothing. The independent path reads
the raw files with the standard `json` and `csv` modules, or staging with eager Polars, and uses no
mart and no `analytics` code. Values are for the final dataset. The difference is zero in every
row, at the precision shown.

| Step | Number | Pipeline value | Independent value |
| --- | --- | --- | --- |
| 2 | Deliveries and duplicates | 2,429,071 and 23,950 | 2,429,071 and 23,950 |
| 3 | Unique events | 2,405,121 | 2,405,121 |
| 3 | Out of order events | 32,520 | 32,520 |
| 3 | Transactions | 1,200,000 | 1,200,000 |
| 3 | Approved / declined / failed | 877,237 / 199,768 / 34,917 | 877,237 / 199,768 / 34,917 |
| 3 | Approved amount (USD) | 38,100,605.41 | 38,100,605.41 |
| 4 | Colombia card authorization rate | 71.0460% | 71.0460% |
| 4 | Brazil Boleto completion rate | 58.8756% | 58.8756% |
| 5 | z of the PSP_C decline spike, 2026-09-01 | 5.3461 | 5.3461 |
| 5 | z of the PSP_B timeout, 2026-09-12 02:00 | 33.300 | 33.300 |
| 5 | `mrc_058` completion and peer rate | 4.60% and 63.50% | 4.60% and 63.50% |
| 7 | Cost per sale, PSP_C and PSP_D, Colombia cards | $1.4110 and $0.9979 | $1.4110 and $0.9979 |
| 7 | Mexico cards shift: fee savings and GMV per month | $1,729.95 and -$20,681.41 | $1,729.95 and -$20,681.41 |
| 11 | Health score of the lowest merchant, `mrc_030` | 34.7970 | 34.7970 |
| 10 | Memo: net GMV, unpaid vouchers, one card point, per month | $12,700,202; $681,757; $105,358 | $12,700,202; $681,757; $105,358 |
| 6 | Dashboard tiles read from the container | 1,128,578; 79.2%; $38,833,029; $38,100,605 | 1,128,578; 79.2%; $38,833,029; $38,100,605 |

Detection result: every planted anomaly is detected by the rule designed for it. Merchant health
labels exactly the two planted merchants at risk, with the planted diagnoses. The peer rule flags
the voucher merchant and the processing merchant, nobody else. The hourly rule flags 3 of the 4
outage hours and nothing else. The daily rule raises 33 flags, of which 3 are chance (ADR 5).

## Step 8: validation from a fresh clone

Every command was run in a new clone with a new virtual environment.

| Command | Result |
| --- | --- |
| `make install` | installs the pinned dependencies |
| `make lint`, `make typecheck`, `make test`, `make check` | pass |
| `make data` | raw file byte identical to the original checkout |
| `make pipeline`, twice | second run skips generation; staging and marts byte identical across runs |
| `make app` | healthy in 2 seconds, after the fix below |
| `make up`, `make down` | pipeline exits 0, app healthy, host returns 200; stack stops |
| `make check` inside the container | passes |

**Defect found and fixed**: `make app` blocked on Streamlit's first run email prompt on a machine
that had never run Streamlit. `.streamlit/config.toml` now sets `headless = true`.

**Environment note**: the machine used has no `docker compose` plugin, so the stack was run with
the standalone `docker-compose` binary (`make up COMPOSE=docker-compose`), the same Compose engine.

## Step 9: Brazil

The original requirement lists PIX and Boleto for Brazil; the first build covered three countries.

- Generator: Brazil (BRL, America/Sao_Paulo), cards, PIX and Boleto; fees for the new segments.
- Pipeline: BRL money rules and the Sao Paulo time zone; the vocabularies accept the new values.
- Metrics: Boleto is a voucher method, so it gets a completion rate like OXXO.
- Analysis and dashboard: the OXXO views became voucher views covering both methods.
- Every independent check above was rerun on the new dataset.
- Found on review: with more segments the daily anomaly rule raises chance flags. The planted
  anomaly test now asserts that the planted days are flagged and are the strongest flags.

## Step 11: realistic merchant health

The first health score labelled every merchant healthy, because the data held no merchant problem
and the score averaged three months. Changed:

- Generator: a Brazilian merchant whose payments are refused and fail (processing) and a Chilean
  merchant whose customers abandon checkout and whose volume halves (experience), both in the last
  30 days.
- Score: rates on the last 30 days only; abandonment against peers added as a component; a
  diagnosis derived from the main driver; merchants with too few attempts are not labelled.
- Heatmap: a cell is marked only when it is above the overall rate after a Bonferroni correction
  for the 168 cells. One cell is marked, Saturday 02:00 on failure rate: the planted outage.

## Open items

| Item | Status |
| --- | --- |
| Daily anomaly threshold | z >= 3 as specified is noisy and cannot separate a PSP from a large merchant; see ADR 5 |
| Health weights | Judgement, not fitted; no churn label exists |
| Memory | Pipeline and `make check` need about 2.4 GB at the default scale; documented in the README |
| Commit history | Some commit subjects do not follow Conventional Commits |
