# Payment performance: findings and recommendations

For the CFO. Period 2026-07-03 to 2026-09-30 (90 days), 1,200,000 transactions from 120
merchants. Countries: TiendaMax's three markets (Mexico, Colombia, Chile) plus Brazil, included to
cover PIX and Boleto. Generated from the marts by `make analysis`; do not edit
by hand. The dataset is synthetic, so the amounts illustrate the method. USD at fixed rates.
Monthly figures are the period scaled to 30 days. Ranges are 95% confidence intervals.

## Summary

- TiendaMax approves **79.2%** of payment attempts and nets
  **$12,700,202 a month**.
- **Cards are the weak point**: 73.9% approved against 87.6%
  for bank transfers, on 65% of all attempts. One point of card approval is worth
  about $105,358 a month.
- **40% of cash vouchers are never paid**, about
  $681,757 a month of orders that were placed and not collected.
- **Keep PSP_C in Colombia and drop PSP_D.**
- **Route by approval rate, not by fee.** Sending traffic to the cheapest PSP saves fees and loses
  several times more in sales.
- Monitoring found **five problems** that a monthly report would have missed: three merchants
  and two PSP incidents.

## 1. Payment methods by country

| Country | Approval rate | Attempts | Net GMV / month |
| --- | --- | --- | --- |
| Mexico | 79.1% | 535,524 | $6,173,554 |
| Brazil | 80.4% | 334,648 | $3,930,597 |
| Colombia | 74.6% | 171,463 | $1,731,070 |
| Chile | 84.4% | 86,943 | $864,981 |

| Country | Method | Approval rate | 95% interval | Attempts | Vouchers paid |
| --- | --- | --- | --- | --- | --- |
| Mexico | oxxo | 96.2% | 96.0% to 96.4% | 40,034 | 60.1% (n = 64,121) |
| Mexico | spei | 89.2% | 89.0% to 89.4% | 100,512 |  |
| Mexico | card | 74.8% | 74.6% to 74.9% | 394,978 |  |
| Brazil | boleto | 92.8% | 92.5% to 93.1% | 31,592 | 58.9% (n = 49,788) |
| Brazil | pix | 89.2% | 89.0% to 89.3% | 141,724 |  |
| Brazil | card | 70.3% | 70.0% to 70.5% | 161,332 |  |
| Colombia | pse | 81.3% | 81.0% to 81.7% | 59,506 |  |
| Colombia | card | 71.0% | 70.8% to 71.3% | 111,957 |  |
| Chile | webpay | 87.8% | 87.4% to 88.2% | 26,040 |  |
| Chile | card | 82.9% | 82.6% to 83.2% | 60,903 |  |

- Bank transfers beat cards in every country. Cards carry most of the volume, so they are where
  improvement pays.
- OXXO and Boleto look best on approval rate only because an unpaid voucher expires instead of
  being declined. Judge them on the share paid: 59.5%
  (n = 113,909).

**Recommendation**: promote bank transfers at checkout where they exist, and send payment
reminders for open vouchers before they expire.

## 2. The two new Colombian PSPs

Compared on the same days, from 2026-08-17, when all four PSPs were live.

| PSP | Method | Approval rate | 95% interval | Attempts | Fee | Cost per sale |
| --- | --- | --- | --- | --- | --- | --- |
| PSP_C | card | 76.4% | 75.4% to 77.3% | 8,443 | 3.2% + $0.08 | $1.41 |
| PSP_A | card | 72.7% | 72.1% to 73.3% | 22,471 | 2.9% + $0.10 | $1.32 |
| PSP_B | card | 69.3% | 68.6% to 70.0% | 16,572 | 2.6% + $0.12 | $1.26 |
| PSP_D | card | 65.0% | 64.0% to 66.0% | 8,400 | 2.2% + $0.05 | $1.00 |
| PSP_C | pse | 87.6% | 86.6% to 88.5% | 4,491 | 2.0% + $0.08 | $0.92 |
| PSP_A | pse | 82.5% | 81.9% to 83.2% | 11,943 | 1.7% + $0.10 | $0.82 |
| PSP_B | pse | 79.2% | 78.3% to 80.0% | 9,005 | 1.4% + $0.12 | $0.72 |
| PSP_D | pse | 75.2% | 73.9% to 76.5% | 4,266 | 1.0% + $0.05 | $0.47 |

- **PSP_C** approves the most on cards (76.4%) and its interval
  does not overlap any other PSP. It is also the most expensive per sale
  ($1.41).
- **PSP_D** approves the least (65.0%) and is the cheapest
  ($1.00). Its fee advantage does not pay for the sales it loses.

**Recommendation**: keep PSP_C and grow its share; end the PSP_D trial or
renegotiate it as a fallback only.

## 3. Routing: approval rate against fees

Simulation: move 20% of the worst PSP's traffic to the best PSP in each country and method, at the
approval rates observed.

| Best PSP defined as | Fees saved / month | Approved payments / month | GMV / month |
| --- | --- | --- | --- |
| Cheapest PSP per sale | $4,675 | -1,050 | -$47,881 |
| Highest authorization rate | -$3,846 | +1,247 | $53,218 |

Shifts that save fees **while maintaining approval rates**, per month:
Chile card from PSP_A to PSP_B ($21 saved, $4,858 more approved); Chile webpay from PSP_A to PSP_B ($29 saved, $2,204 more approved).
These can be made today at no cost to sales.

**Recommendation**: make those shifts, and otherwise route on approval rate. It costs about
$3,846 a month in fees and
adds about $53,218 a month in approved sales. Use the cost per sale to negotiate fees, not
to route.

## 4. Problems found by monitoring

| Issue | Evidence | Estimated impact |
| --- | --- | --- |
| Processing problem at merchant mrc_020 (Brazil) | Approval rate 43.3%, 39 points below similar merchants, on 16,820 attempts in 30 days; 8.9% technical failures | About $291,608 a month of sales refused |
| Checkout problem at merchant mrc_030 (Chile) | Abandoned payments 27 points above similar merchants; volume -61% against the previous 30 days | About $110,459 a month of sales no longer attempted |
| Broken OXXO flow at merchant mrc_058 | 4.6% of 3,585 vouchers paid against 63.5% at similar merchants | About $30,680 a month recoverable at the peer rate |
| Decline spike at PSP_C, Colombia cards, 2026-09-01 to 2026-09-03 | Decline rate 35.2% to 41.5% against about 19.8% normally | About 85 extra declines, $3,455 of sales |
| Night outage at PSP_B, Mexico, 2026-09-12 and 2026-09-13, 02:00 to 04:00 | 60% to 75% of attempts ended in network_timeout against 1.2% normally | About 45 failed payments; small in money, but a full outage nobody saw |

- **Processing or experience?** The health score separates them. mrc_020 loses
  sales to refusals and errors, a PSP or issuer conversation. mrc_030 loses them
  before any PSP is involved: customers abandon the payment and stop coming, a product
  conversation. 2 of 120 merchants score below 50.
- The daily rule raised 33 flags: 3 are the PSP_C
  incident, 27 are Brazil segments across
  2 PSPs (27 of them in the last
  30 days, which is how mrc_020 shows up at PSP level), and
  3 are isolated one day flags expected by chance at that threshold.
- Most failures are refusals, not outages: insufficient_funds alone is
  41.8% of declined and failed payments. Ticket size does not matter (decline
  rate 17.6% to 17.9% across ticket sizes).
- By merchant segment the decline rate runs from 16.9%
  (small) to 18.2%
  (enterprise) across size tiers, and from 14.7%
  (beauty) to 20.1%
  (travel) across categories. The top category contains
  mrc_020, so part of that gap is one merchant.
- Card brand does matter: Mastercard declines
  24.1% of attempts against 22.1% for
  Visa (n = 291,163 and
  438,007; the intervals do not overlap).
- Small tickets are not where vouchers are lost: 28% of OXXO and
  27% of Boleto expirations are on tickets under 20 USD.

**Recommendation**: alert on these rules daily and hourly; ask PSP_C and PSP_B
for incident reports; have the account team contact mrc_020,
mrc_030 and mrc_058 this week.

## Limits of this analysis

- Fees are modelled as a percentage of successful volume plus a fixed fee per attempt.
- The simulation holds approval rates constant; a PSP may behave differently with more volume.
- No margin is assumed, so fees and sales are shown side by side and not netted.
- Definitions and methods are in [DECISIONS.md](DECISIONS.md).
