"""Streamlit dashboard for TiendaMax payment analytics.

Purpose: present payment performance, failure patterns and anomalies to the TiendaMax data team.
Inputs: ``fct_transactions.parquet`` and ``agg_daily.parquet`` under ``$DATA_DIR/marts`` (default
    ``data/marts``), produced by ``pipeline.run``. ``PYTHONPATH`` must include the repository root.
Outputs: an interactive web page served on port 8501.

Design:
    * The page holds no metric logic. Every number comes from ``analytics.metrics`` or
      ``analytics.anomalies``; the functions here only filter, cache and draw.
    * Each cached function scans the Parquet marts lazily and returns a small aggregated frame,
      keyed by the sidebar filters, so the 1.2 million row fact table is never held in the session.
    * Every rate is shown with its sample size and Wilson 95% interval, and every chart carries a
      caption naming the business decision it supports.
    * Anomaly detectors need 14 days of history, so they run on the whole window for the selected
      countries; the date filter then selects which flags are displayed.
"""

import os
from datetime import date
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import polars as pl
import streamlit as st

from analytics import anomalies
from analytics.metrics import performance
from pipeline.transform import AGG_DAILY_FILE, FCT_FILE, aggregate_additive_measures

PAGE_TITLE = "TiendaMax Payment Intelligence"
MARTS_DIR = Path(os.environ.get("DATA_DIR", "data")) / "marts"
FCT_PATH = MARTS_DIR / FCT_FILE
AGG_DAILY_PATH = MARTS_DIR / AGG_DAILY_FILE

MIN_MERCHANT_ATTEMPTS = 200
MERCHANT_ROWS = 10
COUNTRY_NAMES = {"MX": "Mexico", "CO": "Colombia", "CL": "Chile"}
WEEKDAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

# Palette validated for colour vision deficiency on the light surface. Colour follows the
# entity (a PSP keeps its colour under any filter); red is reserved for anomalies.
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
BLUE = "#2a78d6"
ORANGE = "#eb6834"
CRITICAL = "#d03b3b"
PSP_COLORS = {"PSP_A": BLUE, "PSP_B": ORANGE, "PSP_C": "#1baf7a", "PSP_D": "#eda100"}
STATUS_COLORS = {"declined": BLUE, "failed": ORANGE}
SEQUENTIAL = ["#cde2fb", "#86b6ef", "#3987e5", "#1c5cab", "#0d366b"]

Filters = tuple[date, date, tuple[str, ...]]


def _agg_daily(start: date, end: date, countries: tuple[str, ...]) -> pl.LazyFrame:
    """Scan the daily aggregate restricted to the sidebar filters."""
    return pl.scan_parquet(AGG_DAILY_PATH).filter(
        pl.col("date").is_between(start, end) & pl.col("country").is_in(list(countries))
    )


def _fct(start: date, end: date, countries: tuple[str, ...]) -> pl.LazyFrame:
    """Scan the fact table restricted to the sidebar filters."""
    return pl.scan_parquet(FCT_PATH).filter(
        pl.col("local_date").is_between(start, end) & pl.col("country").is_in(list(countries))
    )


@st.cache_data
def load_filter_bounds() -> tuple[date, date, list[str]]:
    """Return the first date, the last date and the countries present in the marts."""
    row = (
        pl.scan_parquet(AGG_DAILY_PATH)
        .select(pl.col("date").min().alias("start"), pl.col("date").max().alias("end"))
        .collect()
        .row(0)
    )
    countries = pl.scan_parquet(AGG_DAILY_PATH).select(pl.col("country").unique().sort()).collect()
    return row[0], row[1], countries.get_column("country").to_list()


@st.cache_data
def load_performance(filters: Filters, dims: tuple[str, ...]) -> pl.DataFrame:
    """Return ``analytics.metrics.performance`` over the daily aggregate for ``dims``."""
    return performance(_agg_daily(*filters), list(dims)).collect()


@st.cache_data
def load_merchant_performance(filters: Filters) -> pl.DataFrame:
    """Return performance per merchant, for merchants with enough attempts to be ranked."""
    keys = ["merchant_id", "country", "merchant_category", "merchant_size_tier"]
    measures = aggregate_additive_measures(_fct(*filters), [*keys, "payment_method"])
    return performance(measures, keys).filter(pl.col("attempts") >= MIN_MERCHANT_ATTEMPTS).collect()


@st.cache_data
def load_failure_views(filters: Filters) -> dict[str, pl.DataFrame]:
    """Return the four failure analysis frames for the sidebar filters."""
    fct = _fct(*filters)
    return {
        "reasons": anomalies.reason_breakdown(fct).collect(),
        "heatmap": anomalies.decline_heatmap(fct).collect(),
        "amount": anomalies.decline_rate_by_amount_bucket(fct).collect(),
        "oxxo_amount": anomalies.oxxo_expiration_by_amount_bucket(fct).collect(),
        "oxxo_merchant": anomalies.oxxo_expiration_by_merchant(fct).collect(),
    }


@st.cache_data
def load_anomalies(countries: tuple[str, ...]) -> dict[str, pl.DataFrame]:
    """Score the whole window for the selected countries with the three detectors."""
    in_countries = pl.col("country").is_in(list(countries))
    fct = pl.scan_parquet(FCT_PATH).filter(in_countries)
    agg_daily = pl.scan_parquet(AGG_DAILY_PATH).filter(in_countries)
    return {
        "daily": anomalies.score_daily_decline_rate(agg_daily).collect(),
        "hourly": anomalies.score_hourly_reason_rate(fct).filter(pl.col("is_anomaly")).collect(),
        "merchant": (
            anomalies.score_merchants_against_peers(fct).filter(pl.col("is_anomaly")).collect()
        ),
    }


def _pct(value: float | None, digits: int = 1) -> str:
    """Format a rate as a percentage, or a dash when it is undefined."""
    return "n/a" if value is None else f"{value:.{digits}%}"


def _interval(low: float | None, high: float | None) -> str:
    """Format a Wilson interval."""
    return "n/a" if low is None or high is None else f"{low:.1%} to {high:.1%}"


def _style(fig: go.Figure, height: int = 340) -> go.Figure:
    """Apply the shared chart style: recessive grid and axes, light surface, top legend."""
    fig.update_layout(
        height=height,
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font={"color": INK_SECONDARY, "size": 13},
        margin={"l": 10, "r": 10, "t": 72, "b": 10},
        title={"y": 0.97, "yanchor": "top", "font": {"color": INK, "size": 16}},
        legend={"orientation": "h", "y": 1.0, "yanchor": "bottom", "x": 0},
        hoverlabel={"bgcolor": "white", "font": {"color": INK}},
    )
    fig.update_xaxes(gridcolor=GRID, linecolor=AXIS, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, linecolor=AXIS, zeroline=False)
    return fig


def _show(fig: go.Figure, decision: str) -> None:
    """Render a chart followed by the one line decision it supports."""
    st.plotly_chart(fig, width="stretch", config={"displayModeBar": False})
    st.caption(f"Decision: {decision}")


def _error_bars(frame: pl.DataFrame, rate: str, low: str, high: str) -> dict[str, Any]:
    """Build asymmetric Plotly error bars from a rate and its Wilson bounds."""
    return {
        "type": "data",
        "symmetric": False,
        "array": (frame.get_column(high) - frame.get_column(rate)).to_list(),
        "arrayminus": (frame.get_column(rate) - frame.get_column(low)).to_list(),
        "color": INK_SECONDARY,
        "thickness": 1.5,
        "width": 4,
    }


def render_overview(filters: Filters) -> None:
    """Render the headline numbers and the daily trends."""
    total = load_performance(filters, ()).row(0, named=True)
    tiles = st.columns(4)
    tiles[0].metric("Attempts", f"{total['attempts']:,}")
    tiles[1].metric("Authorization rate", _pct(total["auth_rate"]))
    tiles[2].metric("GMV (USD)", f"${total['gmv_usd']:,.0f}")
    tiles[3].metric("Net GMV (USD)", f"${total['net_gmv_usd']:,.0f}")
    st.caption(
        f"Authorization rate = approved / (approved + declined + failed); refunded counts as "
        f"approved. Wilson 95% interval {_interval(total['wilson_low'], total['wilson_high'])}, "
        f"n = {total['attempts']:,} attempts. Net GMV subtracts refunds. USD at fixed rates."
    )

    daily = load_performance(filters, ("date",))
    dates = daily.get_column("date").to_list()
    trend = go.Figure()
    trend.add_trace(
        go.Scatter(
            x=dates + dates[::-1],
            y=daily.get_column("wilson_high").to_list()
            + daily.get_column("wilson_low").to_list()[::-1],
            fill="toself",
            fillcolor="rgba(42, 120, 214, 0.15)",
            line={"width": 0},
            hoverinfo="skip",
            showlegend=False,
        )
    )
    trend.add_trace(
        go.Scatter(
            x=dates,
            y=daily.get_column("auth_rate").to_list(),
            customdata=daily.select("attempts", "wilson_low", "wilson_high").rows(),
            mode="lines",
            line={"color": BLUE, "width": 2},
            hovertemplate=(
                "%{x|%b %d}<br>Authorization rate %{y:.1%}<br>"
                "95% interval %{customdata[1]:.1%} to %{customdata[2]:.1%}<br>"
                "n = %{customdata[0]:,}<extra></extra>"
            ),
            showlegend=False,
        )
    )
    trend.update_layout(title="Daily authorization rate with Wilson 95% band", hovermode="x")
    trend.update_yaxes(tickformat=".0%")
    _show(
        _style(trend),
        "spot a sustained drop early enough to escalate to the PSP or reroute traffic.",
    )

    gmv = go.Figure(
        go.Scatter(
            x=dates,
            y=daily.get_column("net_gmv_usd").to_list(),
            mode="lines",
            line={"color": BLUE, "width": 2},
            hovertemplate="%{x|%b %d}<br>Net GMV $%{y:,.0f}<extra></extra>",
        )
    )
    gmv.update_layout(title="Daily net GMV (USD)", hovermode="x")
    gmv.update_yaxes(tickprefix="$", rangemode="tozero")
    _show(_style(gmv, height=280), "size the revenue at stake before prioritising a fix.")


def render_performance(filters: Filters) -> None:
    """Render the method heatmap, the PSP comparison and the merchant rankings."""
    by_method = load_performance(filters, ("country", "payment_method")).filter(
        pl.col("attempts") > 0
    )
    countries = sorted(by_method.get_column("country").unique().to_list())
    methods = sorted(by_method.get_column("payment_method").unique().to_list())
    cells = {(row["country"], row["payment_method"]): row for row in by_method.to_dicts()}
    rates = [[cells.get((c, m), {}).get("auth_rate") for m in methods] for c in countries]
    heatmap = go.Figure(
        go.Heatmap(
            z=rates,
            x=methods,
            y=[COUNTRY_NAMES.get(c, c) for c in countries],
            colorscale=[[i / (len(SEQUENTIAL) - 1), c] for i, c in enumerate(SEQUENTIAL)],
            colorbar={"tickformat": ".0%", "title": "Auth rate"},
            xgap=2,
            ygap=2,
            hoverinfo="skip",
        )
    )
    known = [rate for row in rates for rate in row if rate is not None]
    midpoint = (min(known) + max(known)) / 2 if known else 0.0
    for y, country in enumerate(countries):
        for x, method in enumerate(methods):
            cell = cells.get((country, method))
            if cell is None:
                continue
            label = f"<b>{cell['auth_rate']:.1%}</b><br>n = {cell['attempts']:,}"
            if cell["completion_rate"] is not None:
                label += f"<br>completion {cell['completion_rate']:.1%}"
            heatmap.add_annotation(
                x=x,
                y=y,
                text=label,
                showarrow=False,
                font={"color": "white" if cell["auth_rate"] > midpoint else INK, "size": 12},
            )
    heatmap.update_layout(title="Authorization rate by country and payment method")
    heatmap.update_xaxes(showgrid=False)
    heatmap.update_yaxes(showgrid=False)
    _show(
        _style(heatmap, height=120 + 80 * len(countries)),
        "choose which methods to promote at checkout in each country. For OXXO read the "
        "completion rate: unpaid vouchers expire instead of declining.",
    )

    by_psp = load_performance(filters, ("country", "psp")).filter(pl.col("attempts") > 0)
    comparison = go.Figure()
    for psp, color in PSP_COLORS.items():
        rows = by_psp.filter(pl.col("psp") == psp)
        if rows.height == 0:
            continue
        comparison.add_trace(
            go.Scatter(
                x=rows.get_column("auth_rate").to_list(),
                y=[f"{COUNTRY_NAMES.get(c, c)} · {psp}" for c in rows.get_column("country")],
                customdata=rows.select("attempts", "wilson_low", "wilson_high").rows(),
                error_x=_error_bars(rows, "auth_rate", "wilson_low", "wilson_high"),
                mode="markers",
                marker={"color": color, "size": 11, "line": {"color": SURFACE, "width": 2}},
                name=psp,
                hovertemplate=(
                    "%{y}<br>Authorization rate %{x:.1%}<br>"
                    "95% interval %{customdata[1]:.1%} to %{customdata[2]:.1%}<br>"
                    "n = %{customdata[0]:,}<extra></extra>"
                ),
            )
        )
    labels = sorted(
        (
            f"{COUNTRY_NAMES.get(r['country'], r['country'])} · {r['psp']}"
            for r in by_psp.to_dicts()
        ),
        reverse=True,
    )
    comparison.update_layout(title="Authorization rate by PSP with Wilson 95% intervals")
    comparison.update_xaxes(tickformat=".0%")
    comparison.update_yaxes(categoryorder="array", categoryarray=labels)
    _show(
        _style(comparison, height=120 + 34 * len(labels)),
        "keep, grow or drop a PSP. Intervals that do not overlap are real differences. PSP_C and "
        "PSP_D serve Colombia only since 2026-08-17, so start the date range there to compare "
        "like for like.",
    )
    with st.expander("PSP comparison as a table"):
        st.dataframe(
            by_psp.select(
                "country",
                "psp",
                "attempts",
                pl.col("auth_rate").map_elements(_pct, return_dtype=pl.String),
                pl.struct("wilson_low", "wilson_high")
                .map_elements(
                    lambda s: _interval(s["wilson_low"], s["wilson_high"]), return_dtype=pl.String
                )
                .alias("wilson_95"),
                pl.col("gmv_usd").round(0),
            ),
            hide_index=True,
        )

    merchants = load_merchant_performance(filters)
    table = merchants.select(
        pl.col("merchant_id").alias("Merchant"),
        pl.col("country").alias("Country"),
        pl.col("merchant_category").alias("Category"),
        pl.col("merchant_size_tier").alias("Size"),
        pl.col("attempts").alias("Attempts"),
        pl.col("auth_rate").map_elements(_pct, return_dtype=pl.String).alias("Auth rate"),
        pl.struct("wilson_low", "wilson_high")
        .map_elements(
            lambda s: _interval(s["wilson_low"], s["wilson_high"]), return_dtype=pl.String
        )
        .alias("Wilson 95%"),
        pl.col("net_gmv_usd").round(0).alias("Net GMV USD"),
        pl.col("auth_rate").alias("_rate"),
    )
    top, worst = st.columns(2)
    top.subheader("Top merchants")
    top.dataframe(
        table.sort("_rate", descending=True).head(MERCHANT_ROWS).drop("_rate"), hide_index=True
    )
    worst.subheader("Worst merchants")
    worst.dataframe(table.sort("_rate").head(MERCHANT_ROWS).drop("_rate"), hide_index=True)
    st.caption(
        f"Decision: pick the merchants an account manager should call first. Ranked by "
        f"authorization rate among merchants with at least {MIN_MERCHANT_ATTEMPTS} attempts "
        f"({merchants.height} qualify); not adjusted for payment method mix."
    )


def render_failures(filters: Filters) -> None:
    """Render reasons, the weekday and hour heatmap, ticket size buckets and OXXO expirations."""
    views = load_failure_views(filters)

    reasons = (
        views["reasons"]
        .group_by("final_status", "decline_reason")
        .agg(pl.col("transactions").sum())
        .with_columns((pl.col("transactions") / pl.col("transactions").sum()).alias("share"))
        .sort("transactions")
    )
    reason_chart = go.Figure()
    for status, color in STATUS_COLORS.items():
        rows = reasons.filter(pl.col("final_status") == status)
        reason_chart.add_trace(
            go.Bar(
                x=rows.get_column("transactions").to_list(),
                y=rows.get_column("decline_reason").to_list(),
                orientation="h",
                marker={"color": color, "cornerradius": 4},
                text=[f"{n:,} · {s:.1%}" for n, s in rows.select("transactions", "share").rows()],
                textposition="outside",
                textfont={"color": INK_SECONDARY},
                name=status,
                hovertemplate="%{y}<br>%{x:,} transactions<extra></extra>",
            )
        )
    reason_chart.update_layout(title="Decline and failure reasons by volume and share")
    reason_chart.update_xaxes(range=[0, reasons.get_column("transactions").max() * 1.25])
    reason_chart.update_yaxes(categoryorder="array", categoryarray=reasons["decline_reason"])
    _show(
        _style(reason_chart, height=320),
        "separate what retries and messaging can recover (insufficient funds, timeouts) from "
        "what needs a PSP or risk conversation.",
    )
    with st.expander("Reasons by country and payment method"):
        st.dataframe(
            views["reasons"].with_columns(
                pl.col("share").map_elements(_pct, return_dtype=pl.String)
            ),
            hide_index=True,
        )

    metric = st.radio(
        "Heatmap metric",
        ["decline_rate", "failure_rate"],
        format_func=lambda name: name.replace("_", " ").capitalize(),
        horizontal=True,
    )
    grid = {(r["local_weekday"], r["local_hour"]): r for r in views["heatmap"].to_dicts()}
    hours = list(range(24))
    weekday_rows = [[grid.get((day, hour)) for hour in hours] for day in range(1, 8)]
    heat = go.Figure(
        go.Heatmap(
            z=[[cell[metric] if cell else None for cell in row] for row in weekday_rows],
            customdata=[[cell["attempts"] if cell else 0 for cell in row] for row in weekday_rows],
            x=hours,
            y=WEEKDAY_NAMES,
            colorscale=[[i / (len(SEQUENTIAL) - 1), c] for i, c in enumerate(SEQUENTIAL)],
            colorbar={"tickformat": ".0%"},
            xgap=2,
            ygap=2,
            hovertemplate="%{y} %{x}:00<br>%{z:.1%}<br>n = %{customdata:,}<extra></extra>",
        )
    )
    heat.update_layout(title=f"{metric.replace('_', ' ').capitalize()} by local weekday and hour")
    heat.update_xaxes(title="Local hour", dtick=2, showgrid=False)
    heat.update_yaxes(autorange="reversed", showgrid=False)
    _show(
        _style(heat, height=320),
        "time PSP maintenance windows and on call cover; a dark cell is a recurring outage or "
        "a fraud rule misfiring at a fixed hour.",
    )

    amount = views["amount"]
    buckets = go.Figure(
        go.Bar(
            x=amount.get_column("amount_bucket").to_list(),
            y=amount.get_column("decline_rate").to_list(),
            customdata=amount.select("attempts").rows(),
            error_y=_error_bars(
                amount, "decline_rate", "decline_rate_wilson_low", "decline_rate_wilson_high"
            ),
            marker={"color": BLUE, "cornerradius": 4},
            width=0.55,
            text=[f"n = {n:,}" for n in amount.get_column("attempts")],
            textposition="inside",
            insidetextanchor="start",
            textfont={"color": "white"},
            hovertemplate=(
                "%{x} USD<br>Decline rate %{y:.1%}<br>n = %{customdata[0]:,}<extra></extra>"
            ),
        )
    )
    buckets.update_layout(title="Decline rate by ticket size (USD) with Wilson 95% intervals")
    buckets.update_yaxes(tickformat=".0%", rangemode="tozero")
    _show(
        _style(buckets),
        "decide whether high tickets need 3DS, instalments or a different route.",
    )

    oxxo_amount, oxxo_merchant = views["oxxo_amount"], views["oxxo_merchant"].head(MERCHANT_ROWS)
    if oxxo_amount.height == 0:
        st.info("No OXXO vouchers in the current filters. OXXO is only offered in Mexico.")
        return
    left, right = st.columns(2)
    by_ticket = go.Figure(
        go.Bar(
            x=oxxo_amount.get_column("amount_bucket").to_list(),
            y=oxxo_amount.get_column("expiration_rate").to_list(),
            customdata=oxxo_amount.select("voucher_attempts").rows(),
            error_y=_error_bars(
                oxxo_amount,
                "expiration_rate",
                "expiration_rate_wilson_low",
                "expiration_rate_wilson_high",
            ),
            marker={"color": BLUE, "cornerradius": 4},
            width=0.55,
            hovertemplate=(
                "%{x} USD<br>Expiration rate %{y:.1%}<br>n = %{customdata[0]:,}<extra></extra>"
            ),
        )
    )
    by_ticket.update_layout(title="OXXO expiration rate by ticket size (USD)")
    by_ticket.update_yaxes(tickformat=".0%", range=[0, 1])
    with left:
        _show(
            _style(by_ticket),
            "decide whether voucher reminders should target a ticket size.",
        )
    ordered = oxxo_merchant.sort("expiration_rate")
    by_merchant = go.Figure(
        go.Bar(
            x=ordered.get_column("expiration_rate").to_list(),
            y=ordered.get_column("merchant_id").to_list(),
            orientation="h",
            customdata=ordered.select("voucher_attempts").rows(),
            marker={"color": BLUE, "cornerradius": 4},
            text=[
                f"{rate:.0%} · n = {n:,}"
                for rate, n in ordered.select("expiration_rate", "voucher_attempts").rows()
            ],
            textposition="outside",
            textfont={"color": INK_SECONDARY},
            hovertemplate=(
                "%{y}<br>Expiration rate %{x:.1%}<br>n = %{customdata[0]:,}<extra></extra>"
            ),
        )
    )
    by_merchant.update_layout(title="OXXO expiration rate: 10 highest merchants")
    by_merchant.update_xaxes(tickformat=".0%", range=[0, 1.35])
    with right:
        _show(
            _style(by_merchant),
            "find merchants whose voucher flow is broken; read small n with caution.",
        )


def render_anomalies(filters: Filters) -> None:
    """Render the flagged table and the daily decline rate with anomalous days highlighted."""
    start, end, countries = filters
    scored = load_anomalies(countries)
    daily = scored["daily"].filter(pl.col("date").is_between(start, end))
    daily_flags = daily.filter(pl.col("is_anomaly"))
    hourly_flags = scored["hourly"].filter(pl.col("local_date").is_between(start, end))

    rows: list[dict[str, Any]] = [
        {
            "Detector": "Daily decline rate",
            "When": str(r["date"]),
            "Segment": f"{r['psp']} · {r['country']} · {r['payment_method']}",
            "Observed": _pct(r["decline_rate"]),
            "Expected": _pct(r["baseline_rate"]),
            "Sample": f"{r['attempts']:,} attempts",
            "Strength": f"z = {r['z']:.1f}",
        }
        for r in daily_flags.to_dicts()
    ]
    rows += [
        {
            "Detector": "Hourly reason rate",
            "When": f"{r['local_date']} {r['local_hour']:02d}:00",
            "Segment": f"{r['psp']} · {r['country']} · {r['decline_reason']}",
            "Observed": _pct(r["rate"]),
            "Expected": _pct(r["baseline_rate"]),
            "Sample": f"{r['attempts']:,} attempts",
            "Strength": f"z = {r['z']:.1f}",
        }
        for r in hourly_flags.to_dicts()
    ]
    rows += [
        {
            "Detector": "Merchant against peers",
            "When": "whole window",
            "Segment": f"{r['merchant_id']} · {r['country']} · {r['metric'].replace('_', ' ')}",
            "Observed": _pct(r["rate"]),
            "Expected": _pct(r["peer_rate"]),
            "Sample": f"{r['attempts']:,} attempts",
            "Strength": f"{r['gap'] * 100:.0f} pts below peers",
        }
        for r in scored["merchant"].to_dicts()
    ]
    st.subheader(f"{len(rows)} flagged")
    if rows:
        st.dataframe(pl.DataFrame(rows), hide_index=True)
    else:
        st.info("No anomaly flagged in the current filters.")
    st.caption(
        "Decision: what to investigate today. Daily rule: z >= 3 and at least 50 attempts against "
        "the trailing 14 days. Hourly rule: z >= 5, at least 20 attempts and 10 events. Merchant "
        "rule: Wilson upper bound more than 10 points below country and category peers."
    )

    segments = (
        daily.select(
            pl.format("{} · {} · {}", "psp", "country", "payment_method").alias("segment"),
            pl.col("is_anomaly"),
        )
        .group_by("segment")
        .agg(pl.col("is_anomaly").sum().alias("flags"))
        .sort(["flags", "segment"], descending=[True, False])
    )
    if segments.height == 0:
        return
    choice = st.selectbox(
        "Segment (flagged segments first)", segments.get_column("segment").to_list()
    )
    psp, country, method = choice.split(" · ")
    series = daily.filter(
        (pl.col("psp") == psp)
        & (pl.col("country") == country)
        & (pl.col("payment_method") == method)
    ).sort("date")
    flagged = series.filter(pl.col("is_anomaly"))
    chart = go.Figure()
    chart.add_trace(
        go.Scatter(
            x=series.get_column("date").to_list(),
            y=series.get_column("baseline_rate").to_list(),
            mode="lines",
            line={"color": MUTED, "width": 2, "dash": "dot"},
            name="Trailing 14 day baseline",
            hovertemplate="Baseline %{y:.1%}<extra></extra>",
        )
    )
    chart.add_trace(
        go.Scatter(
            x=series.get_column("date").to_list(),
            y=series.get_column("decline_rate").to_list(),
            customdata=series.select("attempts").rows(),
            mode="lines",
            line={"color": BLUE, "width": 2},
            name="Daily decline rate",
            hovertemplate="Decline rate %{y:.1%}<br>n = %{customdata[0]:,}<extra></extra>",
        )
    )
    chart.add_trace(
        go.Scatter(
            x=flagged.get_column("date").to_list(),
            y=flagged.get_column("decline_rate").to_list(),
            mode="markers",
            marker={
                "color": CRITICAL,
                "size": 12,
                "symbol": "diamond",
                "line": {"color": SURFACE, "width": 2},
            },
            customdata=flagged.select("z").rows(),
            hovertemplate="Anomalous day, z = %{customdata[0]:.1f}<extra></extra>",
            name="Anomalous day",
        )
    )
    chart.update_layout(title=f"Daily decline rate: {choice}", hovermode="x unified")
    chart.update_yaxes(tickformat=".0%", rangemode="tozero")
    _show(
        _style(chart, height=380),
        "confirm a flag is a real break from the segment's own history before paging the PSP.",
    )


def main() -> None:
    """Render the page: sidebar filters and one tab per view."""
    st.set_page_config(page_title=PAGE_TITLE, layout="wide")
    st.title(PAGE_TITLE)
    if not (FCT_PATH.exists() and AGG_DAILY_PATH.exists()):
        st.error(f"Marts not found under {MARTS_DIR}. Run `make pipeline` first.")
        st.stop()

    first, last, available = load_filter_bounds()
    with st.sidebar:
        st.header("Filters")
        picked = st.date_input("Date range", (first, last), min_value=first, max_value=last)
        countries = st.multiselect(
            "Country", available, default=available, format_func=lambda c: COUNTRY_NAMES.get(c, c)
        )
        st.caption("Dates are local to each country. USD amounts use fixed, illustrative rates.")
    if not isinstance(picked, tuple) or len(picked) != 2:
        st.info("Pick an end date to complete the range.")
        st.stop()
    if not countries:
        st.warning("Select at least one country.")
        st.stop()
    filters: Filters = (picked[0], picked[1], tuple(sorted(countries)))

    overview, perf, failures, flagged, health, cost = st.tabs(
        ["Overview", "Performance", "Failures", "Anomalies", "Merchant Health", "Cost"]
    )
    with overview:
        render_overview(filters)
    with perf:
        render_performance(filters)
    with failures:
        render_failures(filters)
    with flagged:
        render_anomalies(filters)
    with health:
        st.info(
            "Placeholder: merchant health (UX versus processing issues) arrives in a later step."
        )
    with cost:
        st.info("Placeholder: PSP cost analysis arrives in a later step.")


main()
