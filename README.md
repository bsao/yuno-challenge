# TiendaMax Payment Intelligence

A working analytics prototype for TiendaMax, built on raw Yuno transaction webhooks. It answers
four questions the business could not: which payment methods perform well per country, whether the
two new Colombian PSPs are worth keeping, whether merchant problems are UX or processing, and what
failure patterns exist.

It contains a seeded synthetic dataset, a Polars pipeline to Parquet, tested metric definitions
with Wilson intervals, anomaly detection, a merchant health score, a PSP cost simulation and a
Streamlit dashboard. Findings are in [docs/ANALYSIS.md](docs/ANALYSIS.md).

## Quickstart

### Option A: Docker

```bash
git clone https://github.com/bsao/yuno-challenge.git && cd yuno-challenge
docker compose up --build
```

Open <http://localhost:8501>. Give Docker at least 4 GB of memory.

### Option B: local (Python 3.11)

```bash
python3.11 -m venv .venv && source .venv/bin/activate
make install
make pipeline   # generates the data if missing, builds staging and marts, logs the findings
make app        # http://localhost:8501
```

## Checks

```bash
make check                                       # ruff format, ruff lint, mypy strict, pytest
docker compose run --rm pipeline make check      # the same, inside the image
make analysis                                    # rebuild docs/ANALYSIS.md from the marts
```

## Architecture

```text
data_gen/generate.py            seeded, no code shared with the pipeline
        |
        v
RAW      data/raw/webhooks.jsonl        one row per webhook delivery (duplicates, out of order)
         merchants.csv, psp_fees.csv, planted_anomalies.json
        |
        |  pipeline/ingest.py           dedupe by event_id, latest status by event time,
        |                               USD amount, local time; quality assertions
        v
STAGING  data/staging/transactions.parquet      one row per transaction
        |
        |  pipeline/transform.py        merchant attributes, additive aggregation;
        |                               reconciliation assertions
        v
MARTS    data/marts/fct_transactions.parquet    one row per transaction
         data/marts/agg_daily.parquet           date x country x psp x payment_method (counts, sums)
        |
        v
analytics/   metrics (all definitions) -> anomalies, health, cost
        |
        v
app/streamlit_app.py            Overview, Performance, Failures, Anomalies, Merchant Health, Cost
```

`pipeline/run.py` orchestrates the stages. Marts store only counts and sums; every rate is derived
at read time by `analytics/metrics.py`, so any rollup is correct.

## Data generation

`make data` writes 1,200,000 transactions over 90 local days ending 2026-09-30 (TiendaMax's real
volume of about 400,000 a month). The same seed always gives byte identical files.

- Four countries and currencies: Mexico (MXN), Brazil (BRL), Colombia (COP), Chile (CLP).
- Cards (Visa, Mastercard) everywhere; OXXO and SPEI in Mexico; PIX and Boleto in Brazil; PSE in
  Colombia; Webpay in Chile. OXXO and Boleto are cash vouchers that are paid later or expire.
- Four PSPs with different authorization rates per segment; PSP_C and PSP_D serve Colombia only,
  for the last 45 days.
- 120 merchants with a long tail of volume; hourly and weekday seasonality.
- About 1% of webhooks are delivered twice and 3% arrive late, so events are out of order.
- Three planted anomalies, recorded as ground truth in `planted_anomalies.json` and recovered by
  the detectors: a three day decline spike, a merchant whose vouchers expire, a night outage.

## Metric definitions

All in `analytics/metrics.py`; full table in [docs/DECISIONS.md](docs/DECISIONS.md).

| Metric | Definition |
| --- | --- |
| Authorization rate | approved / (approved + declined + failed). Refunded counts as approved; pending and expired are excluded |
| Completion rate | paid / (paid + expired), voucher methods (OXXO, Boleto) only |
| GMV, net GMV | amount approved including later refunds; net subtracts refunds |
| Decline rate, failure rate | declined / attempts (refusals), failed / attempts (technical errors) |
| Confidence | every rate carries its sample size and a Wilson 95% interval |

## Assumptions

- Latest status is decided by event time, not arrival order. Refunds are full refunds.
- A merchant operates in one country. Transaction attributes never change between events.
- USD uses fixed, illustrative rates and exists only to sum across countries.
- Dates, hours and weekdays are local to the country of the transaction.
- PSP fees: a percentage of successful volume plus a fixed fee on every attempt.

## Known limitations

- The pipeline is a full refresh in memory: about 2.4 GB at the default scale, for the pipeline
  and for `make check` (one test runs it at full scale). Use `--transactions` for less.
- The daily anomaly rule (z >= 3) raises chance flags: 13 flags, 3 of them the planted incident.
- The health score has no voucher completion component and labels every merchant healthy; the
  dataset plants no merchant decline over time, so question three is answered only for the
  voucher merchant found by the peer detector.
- Merchant comparisons are not adjusted for payment method mix.
- The cost simulation holds authorization rates constant and assumes no margin.
- The decline heatmap shows sample size but no interval per cell.

## How to productionize

| Concern | Prototype | Production |
| --- | --- | --- |
| Ingestion | one JSONL file | webhook endpoint that acknowledges fast and writes to a queue (Kafka, Pub/Sub, SQS); raw events land immutable and partitioned by arrival date |
| Processing | full refresh | incremental merge into staging keyed on `transaction_id`, with the same "latest by event time" rule; a late event window and periodic reconciliation against PSP settlement files |
| Contracts | one schema | one adapter per PSP that maps its payload to the canonical event; a versioned data contract per adapter, validated at the edge, with a dead letter queue for rejects |
| Orchestration | `pipeline/run.py` | an orchestrator (Airflow, Dagster) with a task per stage, retries, backfills and the quality assertions as gates |
| Storage | local Parquet | object storage with a table format (Iceberg, Delta) for ACID merges and time travel; the marts served from a warehouse |
| Alerting | flags in a tab | detectors on a schedule (hourly and daily), flags routed to the on call channel with the segment, the baseline and the sample size; thresholds tuned on alert precision |
| PCI and PII | synthetic data | no card number or CVV ever enters the platform, only PSP tokens, brand and BIN; customer identifiers are hashed at ingestion; raw payloads sit in a restricted zone with short retention, analytics reads staging and marts only; access is audited |
| Metrics | one Python module | the same definitions published as a semantic layer, so dashboards and notebooks cannot disagree |

## Documentation

- [docs/ANALYSIS.md](docs/ANALYSIS.md): findings and recommendations for the CFO, generated from
  the marts by `make analysis`
- [docs/DECISIONS.md](docs/DECISIONS.md): architecture decision records
- [docs/WORKLOG.md](docs/WORKLOG.md): build steps, independent checks, open items

## Screenshots

![Overview](docs/screenshots/01_overview.png)
![Performance](docs/screenshots/02_performance.png)
![Failures](docs/screenshots/03_failures.png)
![Anomalies](docs/screenshots/04_anomalies.png)
![Merchant Health](docs/screenshots/05_merchant_health.png)
![Cost](docs/screenshots/06_cost.png)
