# Payment performance: findings and recommendations

For the CFO. Period 2026-07-03 to 2026-09-30 (90 days), 1,200,000 transactions from 120
merchants in 4 countries. Generated from the marts by `make analysis`; do not edit
by hand. The dataset is synthetic, so the amounts illustrate the method. USD at fixed rates.
Monthly figures are the period scaled to 30 days. Ranges are 95% confidence intervals.

## Summary

- TiendaMax approves **79.9%** of payment attempts and nets
  **$12,819,190 a month**.
- **Cards are the weak point**: 74.4% approved against 88.5%
  for bank transfers, on 65% of all attempts. One point of card approval is worth
  about $105,396 a month.
- **40% of cash vouchers are never paid**, about
  $681,249 a month of orders that were placed and not collected.
- **Keep PSP_C in Colombia and drop PSP_D.**
- **Route by approval rate, not by fee.** Sending traffic to the cheapest PSP saves fees and loses
  several times more in sales.
- Monitoring found **three incidents** that a monthly report would have missed.

## 1. Payment methods by country

| Country | Approval rate | Attempts | Net GMV / month |
| --- | --- | --- | --- |
| Mexico | 79.3% | 535,924 | $6,201,047 |
| Brazil | 82.3% | 334,518 | $4,020,844 |
| Colombia | 74.5% | 171,595 | $1,722,459 |
| Chile | 84.7% | 87,719 | $874,841 |

| Country | Method | Approval rate | 95% interval | Attempts | Vouchers paid |
| --- | --- | --- | --- | --- | --- |
| Mexico | oxxo | 96.3% | 96.1% to 96.5% | 40,425 | 60.7% (n = 64,123) |
| Mexico | spei | 89.4% | 89.2% to 89.6% | 100,249 |  |
| Mexico | card | 75.0% | 74.8% to 75.1% | 395,250 |  |
| Brazil | boleto | 95.8% | 95.6% to 96.0% | 31,700 | 59.6% (n = 50,930) |
| Brazil | pix | 91.1% | 90.9% to 91.2% | 141,865 |  |
| Brazil | card | 72.0% | 71.8% to 72.2% | 160,953 |  |
| Colombia | pse | 81.0% | 80.7% to 81.3% | 59,494 |  |
| Colombia | card | 71.0% | 70.8% to 71.3% | 112,101 |  |
| Chile | webpay | 87.9% | 87.5% to 88.3% | 26,478 |  |
| Chile | card | 83.2% | 82.9% to 83.5% | 61,241 |  |

- Bank transfers beat cards in every country. Cards carry most of the volume, so they are where
  improvement pays.
- OXXO and Boleto look best on approval rate only because an unpaid voucher expires instead of
  being declined. Judge them on the share paid: 60.2%
  (n = 115,053).

**Recommendation**: promote bank transfers at checkout where they exist, and send payment
reminders for open vouchers before they expire.

## 2. The two new Colombian PSPs

Compared on the same days, from 2026-08-17, when all four PSPs were live.

| PSP | Method | Approval rate | 95% interval | Attempts | Fee | Cost per sale |
| --- | --- | --- | --- | --- | --- | --- |
| PSP_C | card | 76.2% | 75.3% to 77.1% | 8,396 | 3.2% + $0.08 | $1.42 |
| PSP_A | card | 72.4% | 71.8% to 73.0% | 22,436 | 2.9% + $0.10 | $1.34 |
| PSP_B | card | 68.8% | 68.1% to 69.5% | 16,720 | 2.6% + $0.12 | $1.26 |
| PSP_D | card | 64.9% | 63.9% to 65.9% | 8,348 | 2.2% + $0.05 | $0.98 |
| PSP_C | pse | 87.7% | 86.7% to 88.7% | 4,472 | 2.0% + $0.08 | $0.92 |
| PSP_A | pse | 82.6% | 81.9% to 83.3% | 11,720 | 1.7% + $0.10 | $0.82 |
| PSP_B | pse | 79.1% | 78.3% to 80.0% | 9,052 | 1.4% + $0.12 | $0.73 |
| PSP_D | pse | 75.4% | 74.2% to 76.7% | 4,504 | 1.0% + $0.05 | $0.48 |

- **PSP_C** approves the most on cards (76.2%) and its interval
  does not overlap any other PSP. It is also the most expensive per sale
  ($1.42).
- **PSP_D** approves the least (64.9%) and is the cheapest
  ($0.98). Its fee advantage does not pay for the sales it loses.

**Recommendation**: keep PSP_C and grow its share; end the PSP_D trial or
renegotiate it as a fallback only.

## 3. Routing: approval rate against fees

Simulation: move 20% of the worst PSP's traffic to the best PSP in each country and method, at the
approval rates observed.

| Best PSP defined as | Fees saved / month | Approved payments / month | GMV / month |
| --- | --- | --- | --- |
| Cheapest PSP per sale | $4,708 | -1,047 | -$47,721 |
| Highest authorization rate | -$3,835 | +1,211 | $51,672 |

**Recommendation**: route on approval rate. It costs about $3,835 a month in fees and
adds about $51,672 a month in approved sales. Use the cost per sale to negotiate fees, not
to route.

## 4. Incidents found by monitoring

| Issue | Evidence | Estimated impact |
| --- | --- | --- |
| Decline spike at PSP_C, Colombia cards, 2026-09-01 to 2026-09-03 | Decline rate 39.2% to 44.9% against about 19.4% normally | About 113 extra declines, $4,630 of sales |
| Night outage at PSP_B, Mexico, 2026-09-12 and 2026-09-13, 02:00 to 04:00 | 54% to 80% of attempts ended in network_timeout against 1.1% normally | About 59 failed payments; small in money, but a full outage nobody saw |
| Broken OXXO flow at merchant mrc_058 | 4.1% of 3,465 vouchers paid against 63.5% at similar merchants | About $30,500 a month recoverable at the peer rate |

- The daily rule also raised 10 isolated one day flags (strongest z = 4.3
  against 5.4 to 8.6 for the
  incident). At that threshold a few such flags are expected by chance.
- Most failures are refusals, not outages: insufficient_funds alone is
  41.4% of declined and failed payments. Ticket size does not matter (decline
  rate 16.7% to 17.5% across ticket sizes).
- Merchant health: 0 of 120 merchants score below 50 (range
  59 to 86).
  No merchant shows a sustained drop in approvals or volume in this period.

**Recommendation**: alert on these rules daily and hourly, ask PSP_C and PSP_B
for incident reports, and have the account team contact mrc_058.

## Limits of this analysis

- Fees are modelled as a percentage of successful volume plus a fixed fee per attempt.
- The simulation holds approval rates constant; a PSP may behave differently with more volume.
- No margin is assumed, so fees and sales are shown side by side and not netted.
- Definitions and methods are in [DECISIONS.md](DECISIONS.md).
