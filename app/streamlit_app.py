"""Streamlit dashboard for TiendaMax payment analytics.

Purpose: present payment performance, PSP evaluation, merchant health and failure patterns.
Inputs: Parquet marts under ``data/marts`` produced by ``pipeline.run``.
Outputs: an interactive web page served on port 8501.

This scaffold renders only the page title; the views arrive once the marts exist.
"""

import streamlit as st

PAGE_TITLE = "TiendaMax Payment Intelligence"

st.set_page_config(page_title=PAGE_TITLE, layout="wide")
st.title(PAGE_TITLE)
st.caption("The analytics marts are not built yet. Views will appear here once the pipeline runs.")
