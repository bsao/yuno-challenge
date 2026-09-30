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
- Conventional Commits, one logical change per commit.

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

**Open after this step**: `pipeline/run.py` is still a stub, so `docker compose up` does not
generate data yet.
