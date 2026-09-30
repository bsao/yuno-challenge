# Decisions

Short architecture decision records. Each states the context, the decision and its consequences.

## ADR 1. Polars over pandas

**Context.** About 2.4 million webhook deliveries and 1.2 million transactions must be
deduplicated, collapsed and aggregated on a laptop, and the code should read like the SQL a data
team will port it to.

**Decision.** Polars for every transformation, lazy where it helps. NumPy only for seeded random
generation.

**Consequences.**

- The whole pipeline runs in about 7 seconds. Expressions are composable, so one metric
  definition serves the marts, the detectors and the dashboard.
- Strict typing works: Polars is typed, unlike most of the pandas API.
- The dashboard scans Parquet lazily and only ever holds small aggregates.
- Trade off: ingestion collects the deduplicated events in memory, so the pipeline peaks at about
  2.4 GB at the default scale. A streaming sink or an incremental merge removes that ceiling.

## ADR 2. Parquet layers: raw, staging, marts

**Context.** Webhooks arrive duplicated and out of order; analysts need stable tables.

**Decision.** Three layers, each with one grain and quality assertions before it is written.

| Layer | File | Grain |
| --- | --- | --- |
| Raw | `webhooks.jsonl`, `merchants.csv`, `psp_fees.csv` | one row per webhook delivery |
| Staging | `transactions.parquet` | one row per transaction, latest status |
| Mart | `fct_transactions.parquet` | one row per transaction, plus local date and merchant attributes |
| Mart | `agg_daily.parquet` | local date x country x psp x payment_method |

- **Deduplicate by `event_id`**, keeping the first arrival.
- **Latest event wins by event time** (`event_at`), never by arrival order. Ties break on the
  lifecycle rank of the status (pending < final < refunded), then `event_id`.
- **Idempotent by full refresh**: staging is rebuilt from the whole raw file, so replays and
  reordering give the same output (tested; reruns are byte identical).
- **Marts hold only additive measures** (a count per status, approved and refunded USD). Rates are
  not additive, so they are derived at read time and any rollup stays correct. The pipeline knows
  statuses; only `analytics` knows what they mean.
- **Money**: `amount_minor` is an integer. `amount_usd = amount_minor / 10 ** exponent * rate`,
  exponents MXN 2, BRL 2, COP 2, CLP 0; fixed illustrative rates 1 MXN = 0.054, 1 BRL = 0.18,
  1 COP = 0.00025, 1 CLP = 0.00105 USD. USD exists only so countries can be summed.
- **Time**: stored in UTC. Local date, hour and ISO weekday use the IANA zone of the country
  (Chile changes to daylight saving time inside the window). Transactions are bucketed by
  creation time.
- **Assertions** (`pipeline/quality.py`): unique keys; valid vocabularies; positive amounts;
  status mix inside wide guardrails; `raw rows - duplicates == distinct events == events in
  staging`; every count and USD sum of the aggregate reconciles with the fact table. A failure
  stops the pipeline before the file is written.

**Consequences.** A full refresh is simple and provably idempotent, and it does not scale
indefinitely; the production path is an incremental merge with the same ordering rule.

## ADR 3. Metric definitions live in one module

**Context.** A rate computed in three places will disagree in three places.

**Decision.** Every formula is in `analytics/metrics.py`, as Polars expressions over the additive
measures. Each rate is returned with its sample size and Wilson 95% interval.

| Metric | Formula |
| --- | --- |
| `approved` | `n_approved + n_refunded` (a refund was authorized first) |
| `attempts` | `approved + n_declined + n_failed` (pending and expired excluded) |
| `auth_rate` | `approved / attempts` |
| `decline_rate`, `failure_rate` | `n_declined / attempts`, `n_failed / attempts` |
| `refund_rate` | `n_refunded / approved` |
| `gmv_usd` | `approved_amount_usd + refunded_amount_usd` (gross) |
| `net_gmv_usd` | `gmv_usd - refunded_amount_usd` |
| `completion_rate` | `paid / (paid + expired)`, voucher methods (OXXO, Boleto) only |
| `expiration_rate` | `expired / (paid + expired)`, voucher methods only |
| Wilson interval | `center = (p + z²/2n) / (1 + z²/n)`, `half = z·sqrt(p(1-p)/n + z²/4n²) / (1 + z²/n)`, z = 1.96 |

**Consequences.**

- Wilson, not the normal approximation: it stays inside [0, 1] and does not collapse at 0% or
  100%, exactly where small segments mislead. A zero denominator gives null, never 0.
- Vouchers need their own rate: an unpaid voucher expires, it is not declined, so OXXO and Boleto
  show about 96% authorization while about 60% of vouchers are paid.
- Two models sit outside this module, each documented where it lives. The **health score**
  (`analytics/health.py`): authorization against peers 40%, technical failures 20%, refunds 15%,
  30 day volume trend 25%, each mapped linearly between a bad and a good anchor; below 50 is
  `at_risk`. The **fee model** (`analytics/cost.py`): percentage fee on successful volume plus the
  fixed fee on every attempt, divided by successful transactions.

## ADR 4. mypy strict scope

**Context.** Full type hints are required; Streamlit and Plotly expose loosely typed APIs.

**Decision.** `mypy --strict` covers `data_gen`, `pipeline` and `analytics`. The Streamlit app is
excluded.

**Consequences.** Strict mode in the app would add casts and ignores without catching real
defects. To keep the exclusion safe the app holds no metric logic: it filters, caches and draws,
and every number comes from typed code. Ruff, including the docstring rules, still covers the app.

## ADR 5. Anomaly method

**Context.** The data team needs flags it can explain to a PSP, not a black box.

**Decision.** Binomial z scores against a pooled trailing baseline, and a Wilson bound against
peers. `z = (rate - baseline) / sqrt(baseline · (1 - baseline) / attempts)`.

| Detector | Grain | Baseline | Flag when |
| --- | --- | --- | --- |
| Daily decline rate | date x psp x country x method | pooled previous 14 days of the segment | z >= 3 and attempts >= 50 |
| Hourly reason rate | psp x country x local date x hour x reason | pooled previous 14 days, all hours | z >= 5, attempts >= 20, events >= 10 |
| Merchant against peers | merchant x metric (authorization, completion) | other merchants of the same country and category | attempts >= 50 and Wilson upper bound more than 10 points below peers |

**Consequences.**

- All three planted anomalies are detected, and the planted days are the strongest flags.
- **The daily rule is noisy at z >= 3**: on 1,492 scored rows it raises 13 flags, 3 planted
  (z 5.4 to 8.6) and 10 isolated (z 3.0 to 4.3). Low rates on small samples are skewed, so the
  normal approximation overstates z. Raising the threshold to 5 would leave only the incident.
- The hourly rule exists because a two hour outage barely moves a daily rate. It uses z >= 5
  because it scores about 59,000 combinations.
- Pooled baselines weight days by volume. They include earlier anomalous days, so z decays during
  a multi day incident; excluding flagged days is the production refinement.
- Limits: no seasonality model, and merchant authorization is not adjusted for method mix.

## ADR 6. Synthetic data at real scale

**Context.** No real data; the analysis must still be checkable.

**Decision.** A seeded generator that shares no code with the pipeline. It writes a fixed 90 day
window ending 2026-09-30, at TiendaMax's real volume (1,200,000 transactions), with about 1%
duplicated and 3% late deliveries, and three planted anomalies recorded in
`planted_anomalies.json`.

**Consequences.**

- Same seed, byte identical files, so every number in the docs is reproducible.
- Ground truth makes the detectors testable.
- Full scale is needed: the planted night outage covers about 94 attempts; at the 55,000 minimum
  it would cover about 4. The cost is memory (ADR 1); `--transactions` shrinks the dataset.
- Payment methods: cards everywhere; OXXO and SPEI in Mexico; PIX and Boleto in Brazil; PSE in
  Colombia; Webpay in Chile. PSP_C and PSP_D serve Colombia only, for the last 45 days.

## ADR 7. Two containers, one command

**Decision.** One image, two Compose services sharing a volume. `pipeline` runs and exits; `app`
starts only if it succeeded. The container runs as a non root user and its healthcheck is Python,
because slim images have no curl.

**Consequences.** Batch and serving are separated as in production, and a reviewer still runs one
command.
