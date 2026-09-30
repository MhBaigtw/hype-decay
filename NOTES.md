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
TLS-only) and Athena results (7-day expiry). State is locked by a lock file in
that same bucket. AWS Budgets watches spend against $10, alerting at
50/80/100%. A CloudWatch alarm on
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

**Breaks at 10x:** one state file behind one lock serialises every apply. Ten
pipelines would queue on it; split state per concern first.

---

## Task 1 amendment — state locking

**Changed:** the DynamoDB lock table is destroyed. Terraform state is now locked
by a lock file in the state bucket (`use_lockfile = true`).

**Both approaches, for the record:**

1. **DynamoDB table.** A table holds a `LockID` item for the duration of an
   apply, wired up with the backend `dynamodb_table` parameter. This is what
   Task 1 built.
2. **S3 conditional writes.** Terraform puts a `.tflock` object next to the
   state file, and S3 refuses a second conditional write while it exists.

**Why the switch:** Terraform 1.16 deprecates `dynamodb_table` and warns on
every `init`. Both give the same mutual exclusion, so the choice is between two
resources or one. **Trade-off:** locking now rests on S3 semantics alone, so a
single bucket-permission mistake could remove locking without removing access to
the state — with two services, that needed two mistakes.

**Proven by:** the apply that destroyed the table took its own lock through S3.
The old table definition is in git history if it is ever wanted back.

---

## Task 2 — Ingestion, one hour end to end

**Built:** `ingest/ingest_hour.py`. One hour, source URL to curated Parquet at
both tiers, run as `hype-decay-deploy`, not as administrator.

**Which service does what:** a DynamoDB manifest holds one row per source hour
with status, URL, content-length, sha256, row counts and output keys. A
heartbeat lease stops two workers taking the same hour. S3 holds
the curated Parquet plus that hour's gz in the 48-hour fixture.

**Measured, 2026-09-10T18 (hour_start 17:00):** source gz 61.4 MiB becomes
page_hour 2.2 MiB (163,085 rows) and page_daily 22.7 MiB (1,748,350 rows), so
24.9 MiB total, 2.46x smaller than the gz. Reconciliation: 9,991,180 en+en.m
views before exclusions, matching the Task 0 REST figure exactly, with 559,226
removed by exclusions.

**Resumability, demonstrated:** killed 9s in: manifest in-flight, nothing
uploaded; immediate retry refused with 45s of lease left; after expiry attempt 2
completed; a fourth run was a no-op.

**Decision:** a lease plus conditional writes, not a bare status flag, which a
killed worker leaves stuck forever.

**Breaks at 10x:** 17,520 hourly page_daily partials would be ~389 GiB. They must
be compacted per day and the partials deleted, or curated storage alone breaks
the $30 budget.

---

## Task 3 pre-design — does page_daily need every page?

**Measured** (300 pages sampled from `page_daily`, strata from 1 view upward; 64
spikes found by the SPEC definition): at a floor of 10, 2 of 64 spike baselines
move by more than 10%; at 100, 10 of 64. Real spikes lost: 0 at every floor
tested. False spikes created: 0 of 236 quiet pages, at every floor.

**Why detection cannot break for a quiet page.** SPEC requires all three of
`daily_views >= 1000`, `>= baseline + 500`, and `>= 5 * baseline`. For any page
with a baseline under 200, `5 * baseline < 1000` and `baseline + 500 < 700`, so
the absolute 1000-view floor is the binding condition. A spike day clears 1000
by definition and is never floored. So no daily floor up to 200 can change
whether a spike on that page is detected.

**What a floor does break is magnitude, not detection.** `peak_excess =
views(peak) - baseline/24`, so an understated baseline inflates excess and shifts
the half-life. Two of 64 baselines moved at floor 10, which is why the floor
stays at 0 until the bytes it saves are measured against a compacted day.

**Breaks at 10x:** sampling 300 pages took 300 REST calls; a real sweep needs the
curated table, not the API.
