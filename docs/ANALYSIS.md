# Analysis

Findings on the synthetic dataset (1,200,000 transactions, 2026-07-03 to 2026-09-30, seed 42).
Every rate is an authorization rate unless stated, shown with its Wilson 95% interval and sample
size. Definitions are in [DECISIONS.md](DECISIONS.md). The data is generated, so these findings
demonstrate the method; the planted patterns are listed in `data/raw/planted_anomalies.json`.

Reproduce: `make pipeline` prints the method ranking and the Colombia card PSP comparison; the
dashboard tabs show the rest.

## 1. Which payment methods perform well per country

| Country | Method | Rate | Wilson 95% | Attempts |
|---|---|---|---|---|
| Chile | webpay | 88.2% | 88.0% to 88.5% | 72,697 |
| Chile | card | 83.2% | 83.0% to 83.4% | 169,625 |
| Colombia | pse | 81.1% | 80.9% to 81.4% | 89,573 |
| Colombia | card | 71.0% | 70.8% to 71.2% | 168,473 |
| Mexico | oxxo | 96.2% | 96.0% to 96.3% | 48,293 |
| Mexico | spei | 89.2% | 89.1% to 89.4% | 120,772 |
| Mexico | card | 74.9% | 74.8% to 75.0% | 476,089 |

- Bank transfer methods (webpay, pse, spei) beat cards in every country. Cards are the weakest
  method everywhere and carry the most volume, so they are where a point of improvement is worth
  most. Colombian cards are the weakest segment overall.
- OXXO's 96.2% is misleading: a voucher that is never paid expires, it is not declined. Only
  **60.0%** of resolved vouchers are paid (n = 77,433). Judge OXXO on completion, not authorization.

## 2. Are the two new Colombian PSPs worth keeping

Like for like, from 2026-08-17 when PSP_C and PSP_D went live:

| PSP | Card rate | Wilson 95% | Attempts | PSE rate | Wilson 95% | Attempts | Card fee |
|---|---|---|---|---|---|---|---|
| PSP_C (new) | 76.3% | 75.5% to 77.0% | 12,642 | 87.0% | 86.2% to 87.8% | 6,660 | 3.2% + $0.08 |
| PSP_A | 72.3% | 71.8% to 72.7% | 33,498 | 82.2% | 81.7% to 82.8% | 17,933 | 2.9% + $0.10 |
| PSP_B | 68.9% | 68.4% to 69.5% | 25,388 | 79.2% | 78.5% to 79.9% | 13,304 | 2.6% + $0.12 |
| PSP_D (new) | 65.1% | 64.2% to 65.9% | 12,542 | 75.6% | 74.6% to 76.7% | 6,796 | 2.2% + $0.05 |

- **Keep PSP_C and shift volume to it.** It is the best PSP on both methods and the intervals do
  not overlap with any other PSP. It is also the most expensive, and it had a three day incident
  (finding 4), so grow it with monitoring.
- **Do not keep PSP_D on authorization grounds.** It is the worst PSP on both methods, 7 points
  below PSP_A on cards. It is the cheapest, but a lost sale costs far more than a fee point.
- **Cost confirms it.** Per successful card transaction PSP_D costs $0.99 and PSP_C $1.43 (fee on
  successful volume plus the fixed fee on every attempt). Moving 20% of PSP_C's Colombian traffic
  to PSP_D would save about $1,142 a month in fees and lose about $11,976 a month in approved GMV.
  A cheaper fee does not pay for an 11 point lower authorization rate.

## 3. Are merchant conversion drops UX or processing issues

**The tooling exists; this dataset contains no such drop.** The health score (authorization
against peers 40%, technical failures 20%, refunds 15%, 30 day volume trend 25%) labels all 120
merchants healthy: scores run from 60 to 84, median 69. The main driver separates the two causes: a
processing problem shows as `authorization` or `failure`, a demand or checkout problem as `volume`.
Here 109 merchants are driven by authorization, 10 by volume and 1 by failure, none severely. The
generator plants no conversion drop over time, so there is nothing more to find.

One experience problem is found by the peer detector rather than the score:

- One merchant is flagged against its country and category peers: `mrc_037` completes 4.0% of
  4,730 OXXO vouchers while its peers complete 63.5%. Its customers receive a voucher and do not
  pay, with no PSP refusal involved. That is a checkout or post checkout experience problem, not a
  processing one.
- No merchant is flagged on authorization rate.
- Gap: the health score has no completion component, so it rates `mrc_037` healthy (67).

## 4. What failure patterns exist

- **Reasons** (share of declined and failed transactions): insufficient_funds 41.3%, card_declined
  25.3%, fraud_suspected 18.3%, processor_error 9.0%, network_timeout 6.1%. About 85% of failures
  are issuer or risk refusals; 15% are technical.
- **Ticket size is not a driver.** The decline rate runs from 17.1% under 10 USD to 18.2% above
  250 USD, and OXXO expiration is flat at 39% to 41% across ticket sizes.
- **Time of day is not a driver** of declines (15.5% to 19.5% across 168 weekday and hour cells).
- **Incident: PSP_C, Colombia, cards, 2026-09-01 to 2026-09-03.** The decline rate doubled to 37%
  to 40% against a baseline near 20% (z = 8.0, 6.9, 5.7; 268 to 286 attempts per day).
- **Incident: PSP_B, Mexico, 02:00 to 04:00 on 2026-09-12 and 2026-09-13.** 57% to 82% of attempts
  ended in network_timeout against a baseline of 1.2% (z from 23.8 to 43.1; 23 to 33 attempts per
  hour). Invisible at daily grain, so it needs the hourly detector.
- **Voucher flow broken at `mrc_037`** (finding 3).

All three planted anomalies are detected, with no other flag raised.

## 5. What would rerouting save

Moving 20% of the worst PSP's traffic to the best PSP in each of the 7 segments, at observed
authorization rates, scaled to a month:

| Worst and best defined by | Fee savings / month | Approved transactions / month | Approved GMV / month |
| --- | --- | --- | --- |
| Cost per successful transaction | +$4,171 | -509 | -$26,292 |
| Authorization rate | -$3,314 | +1,422 | +$57,932 |

Chasing the cheapest PSP saves fees and loses more than six times as much in sales. Routing by
authorization rate costs $3,314 in fees to gain $57,932 in approved GMV. Only in Chile is the same
PSP (PSP_B) both cheaper and better. Route on authorization rate first, and use cost to negotiate.
