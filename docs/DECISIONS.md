# Decisions

Design decisions and their trade offs, in the order they were made.

## D1. Stack

Python 3.11, Polars (lazy API preferred) for every transformation, Parquet for the staging and mart
layers, Streamlit and Plotly for the dashboard. NumPy is used only for seeded random generation in
`data_gen`. Top level dependencies are pinned to exact versions in `requirements.txt` and
`requirements-dev.txt` so the image builds reproducibly.

## D2. Static typing scope

`mypy --strict` covers `data_gen`, `pipeline` and `analytics`. The Streamlit app is excluded.

Trade off: Streamlit and Plotly expose loosely typed, heavily overloaded APIs, so strict mode in
`app/` would mostly produce casts and ignores without catching real defects. The app is kept thin
(it only reads marts and calls typed functions in `analytics`), so the logic that matters stays
under strict typing. Ruff, including the docstring rules, still applies to `app/`.

## D3. Container topology

One image, two Compose services sharing the named volume `data` mounted at `/app/data`. The
`pipeline` service runs `python -m pipeline.run` and exits; the `app` service starts only when the
pipeline completed successfully. This separates the batch job from the serving process, as it would
be in production, while keeping the reviewer experience to one command. The container runs as a non
root user, and the healthcheck is written in Python because slim images have no curl.
