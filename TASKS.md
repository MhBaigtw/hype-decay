# TASKS

One task per branch. Stop at the end of each task and report. Do not start the
next one without being told.

Each task lists a **Definition of done**. If you cannot meet it, stop and say
why rather than partially meeting it and moving on.

---

## Task 0 — Recon (no AWS)

Run `scripts/recon.py --selftest`, then edit `USER_AGENT` and run it live.

Record in `NOTES.md`: measured hourly file size, extrapolated 2-year raw
volume, estimated backfill hours at 3 connections, mobile traffic share,
whether `pageview_complete` is tractable, median GDELT mention lag.

**Definition of done:** the numbers are recorded and a recommendation is made
on `pageviews` vs `pageview_complete` with a stated reason. No AWS resource
exists yet.

---

## Task 1 — Account guardrails and Terraform backend

Before any pipeline code.

- AWS Budget at $10 with email alert at 50%, 80%, 100%
- CloudWatch billing alarm as a second independent tripwire
- IAM user or role for deployment, least privilege, no root keys anywhere
- S3 bucket for Terraform remote state, versioned, DynamoDB lock table
- Athena workgroup with a per-query data scan limit of 5 GB
- `.gitignore` covering `.terraform/`, `*.tfstate*`, `.env`, `*.pem`,
  `credentials`

**Definition of done:** `terraform plan` is clean, the budget alert has been
confirmed by email, and an intentionally unpartitioned Athena query is
rejected by the workgroup limit. Demonstrate that rejection — the guardrail is
worthless until it has been seen to fire.

---

## Task 2 — Ingestion, one hour end to end

One hour goes end to end: source URL to Parquet in the curated zone, both grain
tiers, with that hour's source gz retained as part of the 48-hour regression
fixture. Amendment 1 removed the persistent raw zone, so "write byte-identical
source files to `raw/`" no longer stands: the only source bytes that survive are
the fixture, and they survive for 48 hours, not forever.

- Downloader respecting the 3-connection cap and the User-Agent policy
- Converts to Parquet in flight and writes both tiers under `curated/`:
  `page_daily` for the page-days touched, and `page_hour` only where hourly
  views are 10 or more (SPEC, two-tier curated grain)
- That hour's source gz goes to `fixtures/raw_48h/` and nowhere else. It is a
  format-regression fixture, not a raw zone — see the SPEC storage model
- DynamoDB manifest, one row per hour, carrying at minimum:
  - status: pending, in-flight, done, failed
  - the source URL
  - the source content-length
  - a content hash (sha256) of the bytes as fetched
  - rows parsed, for the Task 4 reconciliation

  Status alone is not enough. With no raw zone, these fields are the only thing
  that makes an hour re-fetchable and byte-verifiable later
- Resumable: killing it mid-run and restarting must not duplicate or skip

**Definition of done:** one hour is in `curated/` as Parquet at both tiers, the
manifest row says done and carries its content hash, that hour's gz is in the
48-hour fixture, and re-running the ingester for the same hour is a no-op that
has been SEEN: killed mid-run, restarted to completion, then run a third time
with the no-op shown in the output rather than asserted. Record the measured
Parquet size for both tiers in `NOTES.md` — that one number sizes the whole
backfill.

---

## Task 3 — Backfill

**Status: done, 2026-10-05.** 17,520 of 17,520 hours done, 0 failed; 730 of
730 days compacted at floor 10 and reconciled to their hours
(`ingest/verify_backfill.py`). See NOTES, Task 3.

Scale Task 2 to 2 years without getting the owner's IP banned.

- Runs on the time-boxed instance, not a laptop. CLAUDE.md permits exactly one,
  terminated on completion with a documented shutdown check
- Fetches from the your.org mirror, which carries a byte-identical copy of the
  same tree (sha256 verified against the origin) and is roughly 11x faster. The
  canonical origin URL is recorded in the manifest for every hour regardless, and
  the origin content-length is checked per file
- Still capped at 3 connections. The cap is Wikimedia policy for the origin and
  plain courtesy for a mirror
- Throttled, resumable, restartable after days of downtime
- Compacts each day as soon as its 24 hours are done, then deletes the partials.
  Uncompacted partials are the dominant storage cost, not the data
- Gaps logged explicitly. Wikimedia has had outages; missing hours are real
  and must be visible in the data, never silently filled
- Progress visible without SSH — CloudWatch metric or a manifest query

**Definition of done:** manifest shows every hour in the window as done or
explicitly failed with a reason, and the failure count is under 1%.

---

## Task 4 — Source to curated

Glue Spark job, max 10 DPU, 30 minute timeout.

- Parse the 4-column `pageviews` format. `pageview_complete` is out of scope,
  so there is no second width to handle
- Apply `hour_start = filename_hour - 1` — see SPEC, this is the critical one,
  and it is specific to the `pageviews` dataset
- Union `en` and `en.m` into one project
- Apply the SPEC exclusion list
- Write both tiers: `page_daily` for every page-day, `page_hour` only where
  hourly views are 10 or more. Iceberg, partitioned by `dt`, registered in the
  Glue Catalog

**One month first, before the full run.** Convert a single month, then report:

- measured Parquet size for `page_daily` and for `page_hour`, separately
- DPU-minutes consumed

Extrapolate the cost of the full 24-month run from that measurement. **If the
extrapolation exceeds $8, stop and get approval before converting anything
else.** Task 0 flagged Glue capacity against roughly 1 TB of gzipped source as
unestimated; this measurement closes that gap for the price of one month
instead of the whole budget.

**Definition of done:** the one-month figures above are recorded, and then for
the full run: row counts reconcile within 0.1% against the per-hour rows-parsed
recorded in the manifest at transfer time (there is no raw zone left to
recount), a spot-check of 5 known pages matches the Wikimedia Pageviews web
tool for the same hours, and an Athena query against `page_hour` scans
dramatically fewer bytes than the same query against the 48-hour raw gz
fixture. Record both byte figures in `NOTES.md` — that number goes in the
README.

---

## Task 5 — dbt models

`dbt-athena`. Layered: staging, intermediate, marts.

- `stg_page_hour` — cleaned base grain
- `int_page_daily` — daily rollup
- `int_baselines` — trailing 28-day median with the 2-day offset
- `int_spikes` — spike detection per SPEC, including the 30-day merge rule
- `fct_half_life` — peak, excess curve, half-life, censoring flag

Tests: uniqueness on every declared grain, non-negative views, no nulls in
join keys, and an accepted-range test on `half_life_hours`.

**Definition of done:** `dbt build` passes clean, and the half-life
distribution plus censored rate are printed in `NOTES.md`. If the censored
rate exceeds 40%, stop and flag it — the spike thresholds probably need
revisiting before anything is built on top.

---

## Task 6 — Hourly incremental

- EventBridge Scheduler, hourly, offset far enough past the hour that the
  source file exists
- Step Functions: fetch, land raw, convert, run affected dbt models
- SNS alert on failure
- Idempotent. Running the same hour twice changes nothing.

**Definition of done:** runs unattended for 48 hours with no manual
intervention and no duplicate rows.

---

## Task 7 — API and frontend

- Lambda behind an API Gateway HTTP API. Two endpoints: search a page, return
  its decay curve; and return the leaderboards.
- Results cached, because every uncached call is an Athena scan that costs
  money. Precompute the marts into a small serving table rather than querying
  Athena per request.
- Frontend: search box, decay curve, half-life stated in plain words, both
  leaderboards, and the censored rate shown honestly.

**Definition of done:** a stranger can open the page and understand the
finding without explanation, and the cost per thousand page views is
calculated and recorded.

---

## Task 8 — README and cost writeup

Architecture diagram, the scanned-bytes before and after from Task 4, actual
total spend, the design decisions and what was rejected, and the known
limitations including bot traffic if the simple dataset was used.

**Definition of done:** the README stands alone for a reader who has never
seen the project.

---

## v2 and beyond — do not start without approval

- v2: GDELT ingestion, bounded watchlist matching, news-vs-Wikipedia decay
  comparison
- v3: live social stream, Fargate consumer, Kinesis Firehose to Iceberg

v1 must be shippable and on a resume before either begins.
