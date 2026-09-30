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
| 1 | Scaffold | Tooling, layout, Dockerfile, Compose stack, stub runner and page | D1, D2 |
| 2 | Generator | `webhooks.jsonl`, `merchants.csv`, `psp_fees.csv`, `planted_anomalies.json`; wired into the runner | D3 |
| 3 | Ingestion | `staging/transactions.parquet`, deduplication, latest status, quality assertions | D4, D5 |
| 4 | Marts and metrics | `fct_transactions`, `agg_daily`, `performance()` with Wilson intervals, highlight logs | D5, D6 |
| 5 | Failure analysis | Five descriptive views, three anomaly detectors, planted anomaly test | D6, D7 |
| 6 | Dashboard | Filters and four working tabs, rebuilt and verified in Docker | D8 |
| 7 | Validation | Full cycle from a fresh clone, documentation review | below |
| 8 | Merchant health | 0 to 100 score with documented weights, label, main driver; Merchant Health tab | D9 |
| 9 | PSP cost | Cost per successful transaction, 20% traffic shift simulation; Cost tab | D10 |

A first generator, built before the specification of step 2 arrived, was replaced by it.

## Independent checks

Each number was computed twice, through code paths that share nothing.

| Step | Number | Pipeline value | Independent value | Independent path |
| --- | --- | --- | --- | --- |
| 2 | Deliveries | 2,430,825 | 2,430,825 | plain `json` re-read of the raw file |
| 2 | Duplicate deliveries | 23,814 | 23,814 | same |
| 2 | Transactions | 1,200,000 | 1,200,000 | same |
| 3 | Unique events in staging | 2,407,011 | 2,407,011 | pure Python replay of the ordering rule |
| 3 | Out of order events | 33,197 | 33,197 | same |
| 3 | Approved / declined / failed | 891,334 / 201,287 / 35,795 | same three values | same |
| 3 | Approved amount (USD) | 37,425,323.52 | 37,425,323.52 | same |
| 4 | Colombia card authorization rate | 71.0167% | 71.0167% | eager recomputation from staging, no marts, no `performance()` |
| 4 | Mexico OXXO completion rate | 59.9731% | 59.9731% | same |
| 5 | z of the PSP_C spike, first day | 7.9681 | 7.9681 | plain Python from staging, no detector code |
| 5 | z of the PSP_B timeout, first hour | 43.139 | 43.139 | same |
| 5 | `mrc_037` completion and peer rate | 4.04% and 63.54% | 4.04% and 63.54% | same |
| 6 | Dashboard attempts and authorization rate (container) | 1,145,522 and 79.3% | 1,145,522 and 79.3% | eager recomputation from local staging |
| 6 | Dashboard GMV and net GMV (container) | $38,143,368 and $37,425,324 | same | same |
| 8 | Health score of the lowest merchant, `mrc_017` | 60.0084 | 60.0084 | plain Python from staging and `merchants.csv` |
| 9 | Cost per success, PSP_C and PSP_D, Colombia cards | $1.4290 and $0.9866 | $1.4290 and $0.9866 | plain Python from staging and `psp_fees.csv` |
| 9 | Mexico cards shift: fee savings and GMV delta per month | $2,032.86 and -$23,113.55 | same | same |
| 9 | Cost tab totals (container) | $4,171, -509, -$26,292 | $4,171.31, -509, -$26,291.62 | local run of the module |

Detection result: 3 daily flags, 4 hourly flags and 1 merchant flag, exactly the three planted
anomalies, with no other flag.

## Step 7: validation from a fresh clone

Every command was run in a new clone with a new virtual environment, before steps 8 and 9. After
them, `make check` passes with 76 tests and the rebuilt Docker stack serves all six tabs without
error.

| Command | Result |
| --- | --- |
| `make install` | installs the pinned dependencies |
| `make lint`, `make typecheck`, `make test` | pass (62 tests) |
| `make check` | passes, about 10 seconds |
| `make data` | 1,200,000 transactions, raw file byte identical to the original checkout |
| `make pipeline`, twice | second run skips generation; staging and marts byte identical across runs |
| `make app` | healthy in 2 seconds, after the fix below |
| `make up` | pipeline exits 0, app healthy in about 24 seconds, host returns 200 |
| `make check` inside the container | passes (62 tests) |
| `make down` | stack stopped |

**Defect found and fixed**: `make app` blocked on Streamlit's first run email prompt on a machine
that had never run Streamlit. `.streamlit/config.toml` now sets `headless = true`.

**Environment note**: the machine used has no `docker compose` plugin, so the stack was run with
the standalone `docker-compose` binary (`make up COMPOSE=docker-compose`), the same Compose engine.

## Open items

| Item | Status |
| --- | --- |
| Merchant conversion drops over time | The dataset plants none, so the health score labels every merchant healthy |
| Voucher completion in the health score | Not a component; the broken OXXO merchant is caught only by the peer detector |
| Cost simulation | Fees and approved GMV are reported side by side; no margin is assumed to net them |
| Wilson interval on every rate | Missing on the hour by weekday heatmap and the OXXO by merchant chart |
| Memory | Pipeline and `make check` need about 2.4 GB at the default scale |
| Commit history | Two commit subjects (`7a6e285`, `d49b884`) do not follow Conventional Commits |
