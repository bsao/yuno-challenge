# TiendaMax Payment Intelligence

Analytics prototype for TiendaMax built on raw Yuno transaction webhooks. It answers which payment
methods perform well per country, whether the two new Colombian PSPs are worth keeping, whether
merchant conversion drops are UX or processing issues, and which failure patterns exist.

## Run it

```bash
docker compose up --build
```

Then open <http://localhost:8501>. The `pipeline` service builds the data and exits; the `app`
service starts the dashboard once the pipeline has completed successfully.

## Develop locally

```bash
python3.11 -m venv .venv && source .venv/bin/activate
make install
make check
```

| Target | What it does |
| --- | --- |
| `make install` | Install runtime and development dependencies |
| `make data` | Generate the synthetic raw webhooks |
| `make pipeline` | Run generate (if needed), ingest, transform and quality checks |
| `make test` | Run pytest |
| `make lint` | Run ruff check |
| `make typecheck` | Run mypy in strict mode |
| `make check` | Format check, lint, mypy and pytest, in that order |
| `make app` | Run the Streamlit dashboard locally |
| `make up` / `make down` | Start or stop the Docker Compose stack |

## Layout

- `data_gen/`: synthetic webhook generator
- `pipeline/`: ingest (raw to staging), transform (staging to marts), quality, run (orchestrator)
- `analytics/`: metrics, anomalies, merchant health, PSP cost
- `app/`: Streamlit dashboard
- `docs/`: [WORKLOG.md](docs/WORKLOG.md) (step by step build trail), [DECISIONS.md](docs/DECISIONS.md), [ANALYSIS.md](docs/ANALYSIS.md), screenshots
- `data/`: generated at run time, not versioned (`raw/`, `staging/`, `marts/`)

## Status

The pipeline runs end to end (`make pipeline` generates, ingests, builds the marts and logs the headline findings); failure analysis and anomaly detection are in `analytics/anomalies.py`. The dashboard views and the health and cost analyses are stubs.
