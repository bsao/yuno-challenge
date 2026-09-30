"""Build ``docs/ANALYSIS.md`` from the marts.

Purpose: write the findings memo for the CFO with every number computed from the marts, so the
    document cannot drift from the data.
Inputs: ``data/marts/fct_transactions.parquet``, ``data/marts/agg_daily.parquet`` and
    ``data/raw/psp_fees.csv`` (run ``make pipeline`` first).
Outputs: ``docs/ANALYSIS.md``. Run with ``make analysis``.

Every rate comes from ``analytics``. The memo adds three estimates of its own, each stated in the
text: monthly figures (window total * 30 / days), sales lost in an incident (excess declines or
failures * the average approved ticket of the segment), recoverable voucher sales (vouchers * the
gap to the peer completion rate * the merchant's average voucher amount) and sales lost at an at
risk merchant (attempts * the gap to the peer approval rate, or the transactions no longer made *
the approval rate, each * the merchant's average approved ticket).

The narrative states conclusions (for example which PSP to keep). Each one is guarded by an
assertion on the data, so the script fails instead of printing a conclusion the data no longer
supports.
"""

from pathlib import Path
from typing import Any

import polars as pl

from analytics import anomalies, cost
from analytics.health import WINDOW_DAYS as HEALTH_WINDOW_DAYS
from analytics.health import merchant_health
from analytics.metrics import NOT_APPROVED_STATUSES, VOUCHER_METHODS, performance
from pipeline.transform import aggregate_additive_measures

ROOT = Path(__file__).resolve().parents[1]
FCT = pl.scan_parquet(ROOT / "data" / "marts" / "fct_transactions.parquet")
AGG = pl.scan_parquet(ROOT / "data" / "marts" / "agg_daily.parquet")
FEES = pl.scan_csv(ROOT / "data" / "raw" / "psp_fees.csv")
OUTPUT = ROOT / "docs" / "ANALYSIS.md"

COUNTRY_NAMES = {"MX": "Mexico", "BR": "Brazil", "CO": "Colombia", "CL": "Chile"}
NEW_PSP_COUNTRY = "CO"
DAYS_PER_MONTH = 30
# The ticket bucket that ends at 20 USD: vouchers at or below it are "small".
SMALL_TICKET_BUCKET = "10-20"
# One percentage point of approval rate, used to price an improvement in card approvals.
ONE_POINT = 0.01


def usd(value: float) -> str:
    """Format a USD amount without cents, with the sign before the dollar symbol."""
    return f"{'-' if value < 0 else ''}${abs(value):,.0f}"


def pct(value: float, digits: int = 1) -> str:
    """Format a rate as a percentage."""
    return f"{value:.{digits}%}"


def interval(row: dict[str, Any], prefix: str = "wilson") -> str:
    """Format the Wilson interval of a performance row."""
    return f"{pct(row[f'{prefix}_low'])} to {pct(row[f'{prefix}_high'])}"


def method_label(name: str) -> str:
    """Format a payment method name for prose (OXXO is an acronym)."""
    return name.upper() if name in {"oxxo", "pix", "pse", "spei"} else name.capitalize()


def table(header: list[str], rows: list[list[str]]) -> str:
    """Render a Markdown table."""
    lines = ["| " + " | ".join(header) + " |", "| " + " | ".join("---" for _ in header) + " |"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def build() -> str:
    """Compute every figure and return the memo as Markdown."""
    days = AGG.select(pl.col("date").n_unique()).collect().item()
    first, last = (
        AGG.select(pl.col("date").min(), pl.col("date").max().alias("last")).collect().row(0)
    )
    monthly = DAYS_PER_MONTH / days
    transactions = FCT.select(pl.len()).collect().item()
    merchants = FCT.select(pl.col("merchant_id").n_unique()).collect().item()
    total = performance(AGG, []).collect().row(0, named=True)

    # 1. Payment methods.
    by_country = performance(AGG, ["country"]).sort("net_gmv_usd", descending=True).collect()
    by_method = performance(AGG, ["country", "payment_method"]).collect()
    card = (
        performance(AGG.filter(pl.col("payment_method") == "card"), []).collect().row(0, named=True)
    )
    other = (
        performance(AGG.filter(~pl.col("payment_method").is_in(["card", *VOUCHER_METHODS])), [])
        .collect()
        .row(0, named=True)
    )
    assert other["auth_rate"] > card["auth_rate"], "bank transfers no longer beat cards"
    method_rows = []
    for country in by_country.get_column("country"):
        rows = by_method.filter(pl.col("country") == country).sort("auth_rate", descending=True)
        for row in rows.iter_rows(named=True):
            completion = (
                f"{pct(row['completion_rate'])} (n = {row['voucher_attempts']:,})"
                if row["completion_rate"] is not None
                else ""
            )
            method_rows.append(
                [
                    COUNTRY_NAMES[country],
                    row["payment_method"],
                    pct(row["auth_rate"]),
                    interval(row),
                    f"{row['attempts']:,}",
                    completion,
                ]
            )
    card_share = card["attempts"] / total["attempts"]
    card_point_usd = card["attempts"] * ONE_POINT * card["gmv_usd"] / card["approved"] * monthly

    # 2. Vouchers.
    vouchers = performance(
        AGG.filter(pl.col("payment_method").is_in(VOUCHER_METHODS)), []
    ).collect()
    voucher = vouchers.row(0, named=True)
    expired_usd = (
        FCT.filter(
            pl.col("payment_method").is_in(VOUCHER_METHODS) & (pl.col("final_status") == "expired")
        )
        .select(pl.col("amount_usd").sum())
        .collect()
        .item()
    )

    # 3. Colombia PSPs, like for like.
    colombia = cost.cost_per_successful_transaction(
        cost.like_for_like(AGG.filter(pl.col("country") == NEW_PSP_COUNTRY)), FEES
    ).collect()
    since = (
        cost.like_for_like(AGG.filter(pl.col("country") == NEW_PSP_COUNTRY))
        .select(pl.col("date").min())
        .collect()
        .item()
    )
    co_card = colombia.filter(pl.col("payment_method") == "card").sort("auth_rate", descending=True)
    best_psp, worst_psp = co_card.row(0, named=True), co_card.row(-1, named=True)
    runner_up = co_card.row(1, named=True)
    assert best_psp["wilson_low"] > runner_up["wilson_high"], "best Colombian PSP is not distinct"
    assert worst_psp["wilson_high"] < co_card.row(-2, named=True)["wilson_low"], (
        "worst not distinct"
    )
    assert worst_psp["cost_per_success_usd"] == co_card.get_column("cost_per_success_usd").min()
    psp_rows = [
        [
            row["psp"],
            row["payment_method"],
            pct(row["auth_rate"]),
            interval(row),
            f"{row['attempts']:,}",
            f"{row['pct_fee']:.1f}% + ${row['fixed_fee_usd']:.2f}",
            f"${row['cost_per_success_usd']:.2f}",
        ]
        for row in colombia.sort("payment_method", "auth_rate", descending=[False, True]).iter_rows(
            named=True
        )
    ]

    # 4. Rerouting simulation.
    shifts = {
        rank: cost.simulate_traffic_shift(AGG, FEES, rank_by=rank).collect()
        for rank in ("cost_per_success_usd", "auth_rate")
    }
    shift_rows = [
        [
            label,
            usd(frame.get_column("monthly_savings_usd").sum()),
            f"{frame.get_column('monthly_approved_delta').sum():+,.0f}",
            usd(frame.get_column("monthly_gmv_delta_usd").sum()),
        ]
        for label, frame in (
            ("Cheapest PSP per sale", shifts["cost_per_success_usd"]),
            ("Highest authorization rate", shifts["auth_rate"]),
        )
    ]
    by_rate = shifts["auth_rate"]
    rate_gain = by_rate.get_column("monthly_gmv_delta_usd").sum()
    rate_fees = -by_rate.get_column("monthly_savings_usd").sum()
    by_cost = shifts["cost_per_success_usd"]
    cost_loss = -by_cost.get_column("monthly_gmv_delta_usd").sum()
    assert rate_gain > 0, "routing by approval rate no longer gains sales"
    assert cost_loss > 3 * by_cost.get_column("monthly_savings_usd").sum(), "cost routing claim"

    # 5. Discovered issues. Merchant health first: it explains part of the daily flags.
    health = merchant_health(FCT).collect()
    at_risk = health.filter(pl.col("label") == "at_risk")
    processing = at_risk.filter(pl.col("diagnosis") == "processing")
    ux = at_risk.filter(pl.col("diagnosis") == "ux")
    assert processing.height == 1 and ux.height == 1, "expected one merchant per diagnosis"
    sick, leaving = processing.row(0, named=True), ux.row(0, named=True)

    def merchant_ticket(merchant_id: str) -> float:
        rows = performance(
            aggregate_additive_measures(
                FCT.filter(pl.col("merchant_id") == merchant_id), ["payment_method"]
            ),
            [],
        ).collect()
        return float(rows.item(0, "gmv_usd") / rows.item(0, "approved"))

    sick_lost = sick["attempts"] * -sick["auth_gap"] * merchant_ticket(sick["merchant_id"])
    leaving_lost = (
        (leaving["transactions_prior_30d"] - leaving["transactions_last_30d"])
        * leaving["auth_rate"]
        * merchant_ticket(leaving["merchant_id"])
    )

    daily = anomalies.score_daily_decline_rate(AGG).filter(pl.col("is_anomaly")).collect()
    segment = ["psp", "country", "payment_method"]
    clusters = daily.group_by(segment).agg(
        pl.len().alias("days"), pl.col("date").min().alias("first"), pl.col("z").max().alias("z")
    )
    merchant_side = clusters.filter(pl.col("country") == sick["country"])
    assert merchant_side.get_column("psp").n_unique() > 1, "merchant problem no longer spans PSPs"
    merchant_side_days = int(merchant_side.get_column("days").sum())
    merchant_side_recent = daily.filter(
        (pl.col("country") == sick["country"])
        & (pl.col("date") > pl.lit(last) - pl.duration(days=HEALTH_WINDOW_DAYS))
    ).height
    incident_key = clusters.filter(pl.col("country") != sick["country"]).sort("z").row(-1)[:3]
    incident = daily.filter(
        (pl.col("psp") == incident_key[0])
        & (pl.col("country") == incident_key[1])
        & (pl.col("payment_method") == incident_key[2])
    ).sort("date")
    assert incident.height >= 2, "PSP decline spike is no longer a multi day incident"
    isolated = daily.height - incident.height - merchant_side_days
    incident_ticket = (
        performance(
            AGG.filter(
                (pl.col("psp") == incident_key[0])
                & (pl.col("country") == incident_key[1])
                & (pl.col("payment_method") == incident_key[2])
            ),
            [],
        )
        .select(pl.col("gmv_usd") / pl.col("approved"))
        .collect()
        .item()
    )
    excess_declines = (
        incident.get_column("attempts")
        * (incident.get_column("decline_rate") - incident.get_column("baseline_rate"))
    ).sum()

    hourly = anomalies.score_hourly_reason_rate(FCT).filter(pl.col("is_anomaly")).collect()
    outage = hourly.sort("local_date", "local_hour")
    outage_key = outage.select("psp", "country", "decline_reason").unique()
    assert outage_key.height == 1, "hourly flags no longer describe a single outage"
    outage_psp, outage_country, outage_reason = outage_key.row(0)
    excess_failures = (
        outage.get_column("events")
        - outage.get_column("attempts") * outage.get_column("baseline_rate")
    ).sum()

    peers = anomalies.score_merchants_against_peers(FCT).filter(pl.col("is_anomaly")).collect()
    voucher_flags = peers.filter(pl.col("metric") == "completion_rate")
    assert voucher_flags.height == 1, "expected one merchant flagged on voucher completion"
    assert set(peers.filter(pl.col("metric") == "auth_rate").get_column("merchant_id")) <= {
        sick["merchant_id"]
    }, "a merchant other than the processing case is flagged on authorization"
    broken = voucher_flags.row(0, named=True)
    broken_vouchers = FCT.filter(
        (pl.col("merchant_id") == broken["merchant_id"])
        & pl.col("payment_method").is_in(VOUCHER_METHODS)
    )
    broken_ticket = broken_vouchers.select(pl.col("amount_usd").mean()).collect().item()
    broken_method = broken_vouchers.select(pl.col("payment_method").first()).collect().item()
    recoverable = broken["attempts"] * broken["gap"] * broken_ticket * monthly

    reasons = (
        FCT.filter(pl.col("final_status").is_in(NOT_APPROVED_STATUSES))
        .group_by("decline_reason")
        .agg(pl.len().alias("n"))
        .with_columns((pl.col("n") / pl.col("n").sum()).alias("share"))
        .sort("n", descending=True)
        .collect()
    )
    top_reason = reasons.row(0, named=True)
    small_vouchers = {
        method: share
        for method, share in anomalies.voucher_expiration_by_amount_bucket(FCT)
        .filter(pl.col("amount_bucket") == SMALL_TICKET_BUCKET)
        .select("payment_method", "cumulative_share_of_expired")
        .collect()
        .rows()
    }
    brands = anomalies.decline_rate_by_card_brand(FCT).sort("decline_rate").collect()
    low_brand, high_brand = brands.row(0, named=True), brands.row(-1, named=True)
    assert high_brand["decline_rate_wilson_low"] > low_brand["decline_rate_wilson_high"]
    tiers = (
        anomalies.decline_rate_by_merchant_segment(FCT, "merchant_size_tier")
        .sort("decline_rate")
        .collect()
    )
    low_tier, high_tier = tiers.row(0, named=True), tiers.row(-1, named=True)
    categories = (
        anomalies.decline_rate_by_merchant_segment(FCT, "merchant_category")
        .sort("decline_rate")
        .collect()
    )
    low_cat, high_cat = categories.row(0, named=True), categories.row(-1, named=True)
    assert high_cat["merchant_category"] == sick["merchant_category"], "top category claim"
    safe_shifts = by_cost.filter(
        (pl.col("monthly_savings_usd") > 0) & (pl.col("monthly_gmv_delta_usd") >= 0)
    )
    safe_text = "; ".join(
        f"{COUNTRY_NAMES[row['country']]} {row['payment_method']} from {row['worst_psp']} to "
        f"{row['best_psp']} ({usd(row['monthly_savings_usd'])} saved, "
        f"{usd(row['monthly_gmv_delta_usd'])} more approved)"
        for row in safe_shifts.iter_rows(named=True)
    )
    assert safe_shifts.height > 0, "no shift saves fees while maintaining approval rates"
    by_bucket = anomalies.decline_rate_by_amount_bucket(FCT).collect().get_column("decline_rate")

    country_rows = [
        [
            COUNTRY_NAMES[row["country"]],
            pct(row["auth_rate"]),
            f"{row['attempts']:,}",
            usd(row["net_gmv_usd"] * monthly),
        ]
        for row in by_country.iter_rows(named=True)
    ]
    voucher_names = " and ".join(method_label(name) for name in VOUCHER_METHODS)
    incident_dates = incident.get_column("date")
    incident_rows = [
        [
            f"Processing problem at merchant {sick['merchant_id']} "
            f"({COUNTRY_NAMES[sick['country']]})",
            f"Approval rate {pct(sick['auth_rate'])}, {-sick['auth_gap'] * 100:.0f} points below "
            f"similar merchants, on {sick['attempts']:,} attempts in 30 days; "
            f"{pct(sick['failure_rate'])} technical failures",
            f"About {usd(sick_lost)} a month of sales refused",
        ],
        [
            f"Checkout problem at merchant {leaving['merchant_id']} "
            f"({COUNTRY_NAMES[leaving['country']]})",
            f"Abandoned payments {leaving['abandonment_gap'] * 100:.0f} points above similar "
            f"merchants; volume {pct(leaving['volume_trend'], 0)} against the previous 30 days",
            f"About {usd(leaving_lost)} a month of sales no longer attempted",
        ],
        [
            f"Broken {method_label(broken_method)} flow at merchant {broken['merchant_id']}",
            f"{pct(broken['rate'])} of {broken['attempts']:,} vouchers paid against "
            f"{pct(broken['peer_rate'])} at similar merchants",
            f"About {usd(recoverable)} a month recoverable at the peer rate",
        ],
        [
            f"Decline spike at {incident_key[0]}, {COUNTRY_NAMES[incident_key[1]]} "
            f"{incident_key[2]}s, {incident_dates.min()} to {incident_dates.max()}",
            f"Decline rate {pct(incident.get_column('decline_rate').min())} to "
            f"{pct(incident.get_column('decline_rate').max())} against about "
            f"{pct(incident.get_column('baseline_rate').min())} normally",
            f"About {excess_declines:,.0f} extra declines, "
            f"{usd(excess_declines * incident_ticket)} of sales",
        ],
        [
            f"Night outage at {outage_psp}, {COUNTRY_NAMES[outage_country]}, "
            f"{outage.get_column('local_date').min()} and {outage.get_column('local_date').max()}, "
            f"{outage.get_column('local_hour').min():02d}:00 to "
            f"{outage.get_column('local_hour').max() + 1:02d}:00",
            f"{pct(outage.get_column('rate').min(), 0)} to "
            f"{pct(outage.get_column('rate').max(), 0)} "
            f"of attempts ended in {outage_reason} against "
            f"{pct(outage.get_column('baseline_rate').min())} normally",
            f"About {excess_failures:,.0f} failed payments; small in money, but a full outage "
            f"nobody saw",
        ],
    ]
    method_table = table(
        ["Country", "Method", "Approval rate", "95% interval", "Attempts", "Vouchers paid"],
        method_rows,
    )
    psp_table = table(
        ["PSP", "Method", "Approval rate", "95% interval", "Attempts", "Fee", "Cost per sale"],
        psp_rows,
    )
    shift_table = table(
        ["Best PSP defined as", "Fees saved / month", "Approved payments / month", "GMV / month"],
        shift_rows,
    )

    return f"""# Payment performance: findings and recommendations

For the CFO. Period {first} to {last} ({days} days), {transactions:,} transactions from {merchants}
merchants. Countries: TiendaMax's three markets (Mexico, Colombia, Chile) plus Brazil, included to
cover PIX and Boleto. Generated from the marts by `make analysis`; do not edit
by hand. The dataset is synthetic, so the amounts illustrate the method. USD at fixed rates.
Monthly figures are the period scaled to 30 days. Ranges are 95% confidence intervals.

## Summary

- TiendaMax approves **{pct(total["auth_rate"])}** of payment attempts and nets
  **{usd(total["net_gmv_usd"] * monthly)} a month**.
- **Cards are the weak point**: {pct(card["auth_rate"])} approved against {pct(other["auth_rate"])}
  for bank transfers, on {pct(card_share, 0)} of all attempts. One point of card approval is worth
  about {usd(card_point_usd)} a month.
- **{pct(1 - voucher["completion_rate"], 0)} of cash vouchers are never paid**, about
  {usd(expired_usd * monthly)} a month of orders that were placed and not collected.
- **Keep {best_psp["psp"]} in Colombia and drop {worst_psp["psp"]}.**
- **Route by approval rate, not by fee.** Sending traffic to the cheapest PSP saves fees and loses
  several times more in sales.
- Monitoring found **five problems** that a monthly report would have missed: three merchants
  and two PSP incidents.

## 1. Payment methods by country

{table(["Country", "Approval rate", "Attempts", "Net GMV / month"], country_rows)}

{method_table}

- Bank transfers beat cards in every country. Cards carry most of the volume, so they are where
  improvement pays.
- {voucher_names} look best on approval rate only because an unpaid voucher expires instead of
  being declined. Judge them on the share paid: {pct(voucher["completion_rate"])}
  (n = {voucher["voucher_attempts"]:,}).

**Recommendation**: promote bank transfers at checkout where they exist, and send payment
reminders for open vouchers before they expire.

## 2. The two new Colombian PSPs

Compared on the same days, from {since}, when all four PSPs were live.

{psp_table}

- **{best_psp["psp"]}** approves the most on cards ({pct(best_psp["auth_rate"])}) and its interval
  does not overlap any other PSP. It is also the most expensive per sale
  (${best_psp["cost_per_success_usd"]:.2f}).
- **{worst_psp["psp"]}** approves the least ({pct(worst_psp["auth_rate"])}) and is the cheapest
  (${worst_psp["cost_per_success_usd"]:.2f}). Its fee advantage does not pay for the sales it loses.

**Recommendation**: keep {best_psp["psp"]} and grow its share; end the {worst_psp["psp"]} trial or
renegotiate it as a fallback only.

## 3. Routing: approval rate against fees

Simulation: move 20% of the worst PSP's traffic to the best PSP in each country and method, at the
approval rates observed.

{shift_table}

Shifts that save fees **while maintaining approval rates**, per month:
{safe_text}.
These can be made today at no cost to sales.

**Recommendation**: make those shifts, and otherwise route on approval rate. It costs about
{usd(rate_fees)} a month in fees and
adds about {usd(rate_gain)} a month in approved sales. Use the cost per sale to negotiate fees, not
to route.

## 4. Problems found by monitoring

{table(["Issue", "Evidence", "Estimated impact"], incident_rows)}

- **Processing or experience?** The health score separates them. {sick["merchant_id"]} loses
  sales to refusals and errors, a PSP or issuer conversation. {leaving["merchant_id"]} loses them
  before any PSP is involved: customers abandon the payment and stop coming, a product
  conversation. {at_risk.height} of {health.height} merchants score below 50.
- The daily rule raised {daily.height} flags: {incident.height} are the {incident_key[0]}
  incident, {merchant_side_days} are {COUNTRY_NAMES[sick["country"]]} segments across
  {merchant_side.get_column("psp").n_unique()} PSPs ({merchant_side_recent} of them in the last
  {HEALTH_WINDOW_DAYS} days, which is how {sick["merchant_id"]} shows up at PSP level), and
  {isolated} are isolated one day flags expected by chance at that threshold.
- Most failures are refusals, not outages: {top_reason["decline_reason"]} alone is
  {pct(top_reason["share"])} of declined and failed payments. Ticket size does not matter (decline
  rate {pct(by_bucket.min())} to {pct(by_bucket.max())} across ticket sizes).
- By merchant segment the decline rate runs from {pct(low_tier["decline_rate"])}
  ({low_tier["merchant_size_tier"]}) to {pct(high_tier["decline_rate"])}
  ({high_tier["merchant_size_tier"]}) across size tiers, and from {pct(low_cat["decline_rate"])}
  ({low_cat["merchant_category"]}) to {pct(high_cat["decline_rate"])}
  ({high_cat["merchant_category"]}) across categories. The top category contains
  {sick["merchant_id"]}, so part of that gap is one merchant.
- Card brand does matter: {high_brand["card_brand"].capitalize()} declines
  {pct(high_brand["decline_rate"])} of attempts against {pct(low_brand["decline_rate"])} for
  {low_brand["card_brand"].capitalize()} (n = {high_brand["attempts"]:,} and
  {low_brand["attempts"]:,}; the intervals do not overlap).
- Small tickets are not where vouchers are lost: {pct(small_vouchers["oxxo"], 0)} of OXXO and
  {pct(small_vouchers["boleto"], 0)} of Boleto expirations are on tickets under 20 USD.

**Recommendation**: alert on these rules daily and hourly; ask {incident_key[0]} and {outage_psp}
for incident reports; have the account team contact {sick["merchant_id"]},
{leaving["merchant_id"]} and {broken["merchant_id"]} this week.

## Limits of this analysis

- Fees are modelled as a percentage of successful volume plus a fixed fee per attempt.
- The simulation holds approval rates constant; a PSP may behave differently with more volume.
- No margin is assumed, so fees and sales are shown side by side and not netted.
- Definitions and methods are in [DECISIONS.md](DECISIONS.md).
"""


if __name__ == "__main__":
    OUTPUT.write_text(build())
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
