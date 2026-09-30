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

A first generator, built before the specification of step 2 arrived, was replaced by it.

## Independent checks

Each number was computed twice, through code paths that share nothing. The independent path reads
the raw files with the standard `json` and `csv` modules, or staging with eager Polars, and uses no
mart and no `analytics` code. Values are for the current dataset, including Brazil.

| Step | Number | Pipeline value | Independent value |
| --- | --- | --- | --- |
| 2 | Deliveries and duplicates | 2,429,420 and 23,765 | 2,429,420 and 23,765 |
| 3 | Unique events | 2,405,655 | 2,405,655 |
| 3 | Out of order events | 32,272 | 32,272 |
| 3 | Transactions | 1,200,000 | 1,200,000 |
| 3 | Approved / declined / failed | 885,346 / 193,737 / 33,707 | 885,346 / 193,737 / 33,707 |
| 3 | Approved amount (USD) | 38,457,571.15 | 38,457,571.15 |
| 4 | Colombia card authorization rate | 71.0288% | 71.0288% |
| 4 | Brazil Boleto completion rate | 59.6446% | 59.6446% |
| 5 | z of the PSP_C decline spike, 2026-09-01 | 8.6024 | 8.6024 |
| 5 | z of the PSP_B timeout, 2026-09-12 02:00 | 27.685 | 27.685 |
| 5 | `mrc_058` completion and peer rate | 4.07% and 63.53% | 4.07% and 63.53% |
| 7 | Health score of the lowest merchant, `mrc_113` | 58.6883 | 58.6883 |
| 7 | Cost per sale, PSP_C and PSP_D, Colombia cards | $1.4166 and $0.9786 | $1.4166 and $0.9786 |
| 7 | Mexico cards shift: fee savings and GMV per month | $1,691.52 and -$19,014.96 | $1,691.52 and -$19,014.96 |
| 6 | Dashboard tiles read from the container | 1,129,756; 79.9%; $39,204,592; $38,457,571 | 1,129,756; 79.9%; $39,204,592; $38,457,571 |

Detection result: the hourly rule raises 4 flags and the merchant rule 1, exactly the planted
outage and merchant. The daily rule raises 13 flags: the 3 planted days, which are the 3 strongest,
and 10 isolated chance flags (ADR 5).

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

## Open items

| Item | Status |
| --- | --- |
| Merchant problems, UX or processing | The dataset plants no merchant decline over time; the health score labels every merchant healthy and has no voucher completion component |
| Daily anomaly threshold | z >= 3 as specified gives 10 chance flags; z >= 5 would leave only the incident |
| Wilson interval on every rate | Missing on the weekday by hour heatmap cells |
| Memory | Pipeline and `make check` need about 2.4 GB at the default scale |
| Commit history | Some commit subjects do not follow Conventional Commits |
