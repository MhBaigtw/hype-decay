# SPEC

## The question

When something spikes in public attention, how long does it take for that
attention to fall to half its peak?

v1 answers this for English Wikipedia. v2 adds news via GDELT and asks whether
news decays at a different rate than encyclopedia traffic. v3 adds a live
social stream.

## Definitions — implement exactly these

All times UTC. All grains hourly unless stated.

**`hour_start`** — the beginning of a capture window. The rule is
dataset-specific, not universal.

For the `pageviews` dataset: the source filename carries the END of the window,
so `hour_start = filename_hour - 1 hour`. Confirmed in Task 0 — the totals in
`pageviews-20260910-180000.gz` matched the Wikimedia REST API hour 17 to the
exact view (desktop 4,024,554).

For `pageview_complete` (out of scope, see below): the hourly-count letter is
the hour that STARTS then, so letter A is 00:00-01:00 and no subtraction
applies. Confirmed 24 of 24 hours across 8 project/access series in Task 0.
Never carry the `pageviews` subtraction across to this dataset.

**`page_hour`** — the base grain. One row per (project, page_title,
hour_start), with `views` summed across desktop and mobile.

**`daily_views`** — sum of `views` for a page over a UTC calendar day.

**`baseline`** — the median `daily_views` for that page over the trailing 28
days, ending 2 days before the day being evaluated. The 2-day offset stops a
spike from inflating its own baseline.

**`baseline_hourly`** — `baseline / 24`.

**spike** — a day qualifies when all three hold:
- `daily_views >= 5 * baseline`
- `daily_views >= baseline + 500`
- `daily_views >= 1000`

The absolute floors exist to stop a page going from 2 views to 20 from
registering as a spike. A page can only open one spike per 30-day window;
if it qualifies again inside that window, extend the existing spike rather
than opening a new one.

**`peak_hour`** — the hour with maximum `views` within 48 hours of spike start.

**`peak_excess`** — `views(peak_hour) - baseline_hourly`.

**`excess(h)`** — `views(h) - baseline_hourly`, floored at 0.

**`half_life_hours`** — hours from `peak_hour` to the first hour where
`excess(h) <= 0.5 * peak_excess` AND it stays at or below that level for 3
consecutive hours. The 3-hour debounce prevents a single quiet hour from
ending the measurement early.

**censoring** — if no such hour exists within 30 days of `peak_hour`, set
`half_life_hours = NULL` and `is_censored = true`. Report the censored rate
prominently. Some spikes genuinely never decay because the event permanently
changed how often the page is read, and hiding that is dishonest.

## Curated grain — two tiers

`page_daily` is written for every English page-day.

`page_hour` is written only where hourly views are 10 or more.

Spike qualification requires `daily_views >= 1000`, so a qualifying page has a
half-life point far above a 10-view floor; the truncation affects only the
cosmetic tail of the decay curve.

**Known limitation.** Below the floor, an hour with 0 views and an hour dropped
by the floor are indistinguishable in `page_hour`. The tail of a decay curve is
truncated, not measured. Anything reading `page_hour` must treat a missing hour
as unknown, never as zero — the same rule that applies to a missing source
file. One consequence worth watching: for a spike that only just qualifies, half
of `peak_excess` can land near the floor, so verify against `page_daily` before
trusting a half-life measured near it.

## Exclusions

Drop before analysis:
- `Main_Page` and any per-language equivalent
- any title containing a namespace prefix: `Special:`, `Talk:`, `File:`,
  `Category:`, `Template:`, `Help:`, `Portal:`, `Wikipedia:`, `User:`
- the literal title `-`
- titles that fail UTF-8 decoding

`Main_Page` alone will dominate every leaderboard if it survives.

## Source: Wikimedia hourly pageviews

`https://dumps.wikimedia.org/other/pageviews/{YYYY}/{YYYY-MM}/pageviews-{YYYYMMDD}-{HH}0000.gz`

Gzipped, roughly 50 MB compressed per hour. Four space-separated fields:
`domain_code page_title count_views total_response_size`. The last field is
documented as inaccurate — drop it.

`pageview_complete` (daily bz2 files, carrying page IDs) was evaluated in
Task 0 and is **out of scope**. Task 0 measured that `pageviews` is already
filtered to agent type `user`, so `pageview_complete` adds no bot filtering,
and its daily cadence cannot feed the hourly incremental in Task 6.

Two claims previously in this spec did not survive contact with the data and
have been deleted:

- the 5-column quirk — all 384,090 sampled rows had 6 fields, and a missing
  page ID appears as the literal `null`
- an `agent_type` column — there is none; bot filtering comes from downloading
  the `-user` file

## Source: GDELT 2.0 (v2 only)

`http://data.gdeltproject.org/gdeltv2/lastupdate.txt` lists the current
15-minute slice as `<size> <md5> <url>` for export, mentions and gkg.

The mentions table is tab-delimited, 16 columns, no header. Both
`EventTimeDate` and `MentionTimeDate` are `YYYYMMDDHHMMSS`, but they do not
mean what an earlier draft of this spec assumed. Per the GDELT 2.0 event
codebook, read in Task 0:

- `EventTimeDate` is the 15-minute timestamp when GDELT FIRST RECORDED the
  event (the `DATEADDED` of the original event record), not when the event
  happened.
- `MentionTimeDate` is the timestamp of the current update batch, and is
  identical for every row in a file.

The difference between them is therefore the AGE OF AN EVENT at the moment it
is mentioned, not a publication delay. A single slice yields no decay signal at
all, because every row in it shares one `MentionTimeDate`. The median of 165
minutes measured in Task 0 is the median age of events mentioned in one slice,
and nothing more.

News decay must be measured as mentions per event counted across consecutive
batches: accumulate mention counts per `GlobalEventID` over successive
15-minute files, then measure how that per-event rate falls.

Entity matching against Wikipedia is a **v2 design task, not a v1
implementation detail**. Do not build it during v1. The intended approach is a
bounded watchlist: take the top few hundred spiking Wikipedia pages per day as
the entity list, normalize titles (underscores to spaces), and match against
GDELT GKG name fields. Open-world entity resolution is explicitly out of
scope and will not be attempted.

## Storage model

```
s3://<bucket>/curated/page_daily/           Iceberg, partitioned by dt
s3://<bucket>/curated/page_hour/            Iceberg, partitioned by dt
s3://<bucket>/marts/                        dbt outputs
s3://<bucket>/fixtures/raw_48h/*.gz         48 hours of source gz, fixture only
```

**There is no persistent raw zone.** Conversion to Parquet happens in flight
during the backfill transfer. No untouched copy of the source is retained.

Rationale: Task 0 measured 17,520 of 17,520 hourly files present across the
two-year window, every hour addressable at a stable URL on a permanent public
archive. A second copy buys no durability, and it costs roughly $21.60/month
against a $30 total project budget.

Reproducibility comes from the manifest instead (TASKS, Task 2): source URL,
content-length and content hash per hour. If a parsing bug is found in month
three, the fix is to re-fetch and reprocess the affected hours off the
manifest, byte-verifiable against what was originally read.

Retain 48 hours of raw gz as a format-regression test fixture, and nothing
more. Upstream changing its line format is the failure that fixture catches.

## v1 acceptance

v1 is done when all of these are true:

- 2 years of English Wikipedia hourly data in S3, gaps logged not silently
  skipped
- `page_hour` Iceberg table queryable through Athena with partition pruning
- spike and half-life models built in dbt with tests on grain uniqueness,
  non-negative views, and null handling
- a distribution of half-lives across all detected spikes, with the censored
  rate stated
- a leaderboard of fastest-forgotten and longest-lingering spikes
- an hourly scheduled incremental that keeps it current
- a public page where someone can search a topic and see its decay curve
- README with the architecture diagram, the cost breakdown, and the measured
  before/after on bytes scanned

## Out of scope for v1

Topic categories, Wikidata joins, cross-language comparison, GDELT, the
social stream, any ML, any forecasting. Each of these has sunk a project like
this before. They are v2 and later, in that order.
