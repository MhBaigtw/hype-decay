# NOTES

Teaching log. One entry per completed task. Under 200 words each.

Each entry covers: what was built, which AWS service does what, one design
decision and the alternative rejected, and one thing that breaks at 10x load.

---

## Task 0 — Recon (no AWS)

**Built:** nothing deployed. Selftest passed 3 of 3 parser checks. Live run of
`scripts/recon.py`, plus spot checks against the Wikimedia REST API and dump
listings, on 2026-09-13.

**AWS:** none used. No resource exists.

**Measured** (window 2024-09-13 to 2026-09-12):

- Hourly file: 61.4 MiB sample; window mean 57.6 MB; 17,520 of 17,520 present.
- 2-year raw: `pageviews` 939 GiB, `pageview_complete` 437 GiB.
- Backfill at 3 connections (estimate): 24.4 h vs 10.7 h.
- Mobile: 59.7% of `en` views (n = 9,991,180, one hour).
- `pageview_complete` tractable: yes. 0 five-column rows in 384,090 sampled.
- Median GDELT MentionTimeDate − EventTimeDate: 165 min (n = 1,842, one slice).

**Decision: `pageviews`.** Its hourly files equal the REST API's user-agent
totals exactly, so `pageview_complete` adds no bot filtering. It also publishes
hourly (~2 h lag), which Task 6 needs; `pageview_complete` is one daily file.
Rejected: `pageview_complete` for the backfill, despite half the storage,
because pairing it with hourly files means two parsers and two hour conventions.

**Blocker:** 939 GiB on S3 Standard is ~$21.60/month (estimate) against a $30
total budget.

**Breaks at 10x:** backfill stretches to ~10 days under the 3-connection cap.

---

## Task 1 — Account guardrails and Terraform backend

**Built:** 19 resources in us-east-1 via Terraform. `terraform plan` clean
against remote state. Estimated cost ~$0.00/month.

**Which service does what:** S3 holds Terraform state (versioned, encrypted,
TLS-only) and Athena results (7-day expiry). DynamoDB holds the state lock. AWS
Budgets watches spend against $10, alerting at 50/80/100%. A CloudWatch alarm on
`AWS/Billing EstimatedCharges` at $5 is a second, independent tripwire. Both
publish to one SNS topic. The Athena workgroup enforces a 5 GiB per-query scan
limit. IAM role `hype-decay-deploy` is the pipeline identity, assumable only by
the human SSO admin.

**Guardrail seen firing:** an unpartitioned `COUNT(*)` over a public dataset was
cancelled at exactly 5.00 GiB — "Bytes scanned limit was exceeded"
(`scripts/demo_scan_limit.py`). Open: the SNS subscription stays
`PendingConfirmation` until the link is clicked.

**Decision:** the deploy role leads with an explicit Deny on every forbidden
service and every region but us-east-1. Rejected: an Allow list alone, because
Allow lists widen as people unblock themselves, while a Deny cannot be
overridden.

**Breaks at 10x:** one state file behind one lock table serialises every apply.
Ten pipelines would queue on that lock; split state per concern first.
