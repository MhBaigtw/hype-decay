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

**Measured** (300 pages from `page_daily`, strata from 1 view up; 64 SPEC
spikes): at floor 10, 2 of 64 spike baselines move more than 10%; at 100, 10 of
64. Real spikes lost: 0 at every floor. False spikes created: 0 of 236 quiet
pages, at every floor.

**Why detection cannot break for a quiet page.** SPEC requires all three of
`daily_views >= 1000`, `>= baseline + 500`, and `>= 5 * baseline`. For any page
with a baseline under 200, `5 * baseline < 1000` and `baseline + 500 < 700`, so
the absolute 1000-view floor is the binding condition. A spike day clears 1000
by definition and is never floored. So no daily floor up to 200 can change
whether a spike on that page is detected.

**What a floor breaks is magnitude, not detection.** `peak_excess = views(peak)
- baseline/24`, so an understated baseline inflates excess and shifts the
half-life. Two of 64 baselines moved at floor 10, so the floor stays at 0 until
the bytes it saves are measured against a compacted day.

---

## Task 3, mid-task — credits, the double-count guard, floor 10

**Credits blinded both tripwires.** Cost Explorer, September by `RECORD_TYPE`:
usage +$0.2414, credit −$0.2414, net zero. Budgets counted credits and read
$0.00. `EstimatedCharges` read $0.00 too, at the total and per service, so the
CloudWatch metric is net of credits and has no gross form. Neither guardrail
could have fired. The budgets now exclude credits and refunds.

**Double-count guard.** A partial written beside a compacted `day.parquet` is
counted twice. The ingester refuses to write into a `compacted` or `compacting`
day. `--force` flips the day row to `invalidated` and quarantines `day.parquet`.
Seen on 2026-09-10: force-ingested T18, compaction refused at 1 of 24 partials,
re-ingested the day, rebuild 190,364,968 → 190,364,968 views.

**Decision: daily floor 10.** It removes 75.5% of page-days and 7.41% of views.
The day shrinks from 92.4 MB to 24.3 MB, so 730 days come to about 16.5 GiB, and
the dbt workgroup cap is 25 GiB. Rejected: floor 0, at 62.8 GiB. Its price is
that baselines must zero-fill (SPEC).

**Breaks at 10x:** correcting one hour re-fetches its whole day.

**On the instance:** `code/` holds commit
`0ac7fc83dbdb75402b66df064f37546e48c969c2`.

---

## Task 3 — measurement run on the instance (2026-10-03)

**Ran:** `i-04d58a137d31e0361`, c7g.xlarge, AL2023 kernel 6.18, commit
`0ac7fc8`. Time box 180 minutes; the timer was verified armed over SSM. Launched
20:02:17Z, terminated 20:05:59Z once the run ended; root volume confirmed gone.
Cost about $0.01.

**Which service does what:** EC2 ran the measurement, SSM Run Command drove it
without an inbound port, and S3 supplied the pinned code.

**Measured, one whole day (24 files, 1,354 MiB):** download 333.9 MiB/s at 3
connections, parse 3.84 s per file. Full window: download 0.8 h, parse 4.7 h,
so processing binds, not the connection cap. Estimated $0.68.

**Memory:** parse worker peak 1,582 MiB, compaction peak 5,435 MiB. Four workers
plus one compaction need 11.49 GiB against 7.6 GiB usable. They do not fit, so
compaction cannot overlap parsing on this box as designed.

**Decision:** terminated by hand, not by the timer. Rejected: letting the timer
do it, which bills three hours for four minutes of work.

**Breaks at 10x:** compaction memory grows with distinct titles per day, and it
already takes 70% of the box.

---

## Task 3 — 3-day backfill trial (2026-10-04)

**Ran:** `backfill.py` for 2024-09-13 to 2024-09-15 on `i-05229fb4658780aa7`,
commit `7390248`, 45-minute time box. Launched 02:03:58Z, terminated by
`terraform apply` at 02:12:51Z; root volume confirmed gone.

**Which service does what:** EC2 ran the runner, DynamoDB held every decision
it made, CloudWatch took per-day progress, and S3 took the Parquet.

**Measured:** 66.6 s a day (42 s ingest, 24 s compaction, about 9 s of that a
trial-only copy). Peaks: parse 1,608 MiB, compaction 5,714 MiB. MemAvailable
never fell below 1,456 MiB. 77 S3 requests a day. All three days matched an
independent recompaction and the manifest's per-hour totals.

**Decision:** parsing and compaction alternate. Rejected: overlapping them,
which needs 11.5 GiB on a 7.6 GiB box. Full-run time box: 18 h, from 58 s a
day × 730 × 1.5.

**Found:** `Manifest.fail` wrote the reserved word `error`, so no hour could
ever be marked failed. moto caught it, and real DynamoDB confirmed it.

**Breaks at 10x:** compaction memory. It left 1.4 GiB free.

---

## Task 3 — full backfill, run 1 stopped at day 351 (2026-10-04)

**Ran:** `i-0d7641b7a9b0af924`, commit `a3d3b03`, 18 h time box. Launched
02:22Z. It shut itself down between 07:53Z and about 08:50Z. CloudTrail shows no
`TerminateInstances`, so nothing outside the box terminated it. The volume
deleted with it, and state was cleaned by a refresh-only apply.

**Got done:** 8,424 hours `done`, 0 failed. 350 days compacted at floor 10,
through 2025-08-27. 2025-08-28 has all 24 hours ingested but no day row, so it
died in that day's compaction.

**Cause, most likely:** memory. The compaction peak ranged from 5.6 to 7.1 GiB by
day against 7.6 GiB usable, and 19 days fell under 500 MiB free. The final log
upload never ran, and the disk is gone, so this is inferred, not observed.

**Decision pending:** fix compaction memory before relaunching. Rejected:
relaunching as-is, which would die again on the next heavy day.

**Breaks at 10x:** whole-day compaction in memory. It does not survive this
scale now.

---

## Task 3 — Backfill, done (2026-10-05)

**Built:** `backfill.py`, a manifest-driven runner, plus a detached,
self-terminating wrapper. Run 2 (`i-077613891baaf869c`, 12 h box) ran 8.65 h and
terminated itself at 03:05Z.

**Which service does what:** EC2 parses, S3 holds the Parquet and the logs,
DynamoDB holds every decision, CloudWatch carries progress and a stall alarm.

**Verified:** 17,520 of 17,520 hours done, 0 failed. 730 days compacted at floor
10, each matching the sum of its 24 hourly totals. No partials, leftovers or
fixtures. Curated zone 52.83 GiB: page_hour 35.84, page_daily 16.99.

**Cost:** 14.42 instance-hours across every launch. $2.82 of October usage plus
$0.37 tax; no credits applied yet.

**Decision:** incremental compaction, peaking at 4,882 MiB. Rejected: the
all-at-once engine, which peaked at 7,393 MiB on 2025-08-28 and killed run 1.
The outputs are identical.

**Breaks at 10x:** one box and three connections. Ten times the window is about
90 h, and a day with 10x the pages would exhaust memory even incrementally.
