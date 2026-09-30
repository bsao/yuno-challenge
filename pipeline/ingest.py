"""Raw to staging ingestion.

Purpose: deduplicate and type raw webhook events idempotently.
Inputs: raw event files under ``data/raw``.
Outputs: ``data/staging`` Parquet, one row per ``event_id``.

Not implemented in this scaffold.
"""
