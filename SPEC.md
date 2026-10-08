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

**The baseline must be computed over a zero-filled date spine, not over the rows
that exist.** `page_daily` has no row for a page-day below the 10-view floor
(see Curated grain), and none for a page-day with zero views even without a
floor. A median taken over existing rows silently drops exactly the quiet days
that define a baseline.

Worked example. A page's 28-day window holds 26 days at 5 views and 2 days at
400. At floor 10 the 26 quiet days have no rows, so a median over existing rows
is median(400, 400) = **400**, and the spike test needs `5 × 400 = 2,000` views.
A day of 1,500 views is a real spike — the true baseline is 5, and 1,500 clears
`5 × 5`, `5 + 500` and `1,000` — but against 400 it is missed. Zero-filled, the
window is 26 zeros and two 400s, median **0**, and the 1,500-view day qualifies
on the absolute floors exactly as it should.

Requirements on the baseline model:

- Build the spine only for **candidate pages**: any page with at least one
  page-day of `daily_views >= 1000` in the window. A spike day must clear 1,000
  by definition, so no other page can ever need a baseline, and a spine over
  every page would be millions of pages × 730 days of zeros.
- A spine day with no `page_daily` row is **0**. The error this introduces is
  bounded by the floor: the true value was 0–9 views, so `baseline` is
  understated by under 10 views a day and `baseline_hourly` by under 0.4.
- A spine day whose SOURCE is incomplete — the manifest day row is not
  `compacted`, or the compacted day was built with hours missing — is **NULL,
  not 0**, and is left out of the median. Zero-fill stands in for a quiet page,
  never for an outage.

**`baseline_hourly`** — `baseline / 24`.

**spike** — a day qualifies when all three hold:
- `daily_views >= 5 * baseline`
- `daily_views >= baseline + 500`
- `daily_views >= 1000`

The absolute floors exist to stop a page going from 2 views to 20 from
registering as a spike. A page can only open one spike per 30-day window;
if it qualifies again inside that window, extend the existing spike rather
than opening a new one.

The 30-day window is anchored at the spike's START day (Task 5): a page that
qualifies again fewer than 30 days after the start extends that spike; 30 or
more days after the start, it opens a new one. Anchoring at the most recent
qualifying day instead would let a page that qualifies every few weeks chain one
spike indefinitely. A spike's start is 00:00 UTC of its first qualifying day.

**Evaluation starts 30 days into the window.** The baseline needs 28 days ending
2 days before, so the first 30 days of the window (2024-09-13 to 2024-10-12)
cannot have a complete baseline and are not evaluated for spikes.

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

Censoring is TWO flags, reported separately (Task 5), because they mean
opposite things:

- `never_halved` -- all 30 days after the peak were observed and the excess
  never fell to half. A real finding about the page.
- `window_end` -- the data ends before 30 days after the peak, so it may yet
  halve. An artefact of where the data stops, nothing about the page.

The 40% stop rule (TASKS, Task 5) applies to `never_halved` only.

**Hourly zero-fill.** The half-life is measured over an hour spine for each
spike, from 48 hours before its peak to 30 days after (plus the 2 hours the
debounce needs), capped at the last hour of data. A spine hour with no
`page_hour` row is 0. That is sound because all 17,520 source hours are present
(Task 3, verified): a missing page-hour therefore means fewer than 10 views, not
missing data. The true value is 0 to 9, so an excess near zero is understated by
at most 9 -- the reason SPEC already advises checking a half-life measured near
the floor.

**`flat_profile`** -- advisory, never a filter (CLAUDE.md). Human attention has
a daily rhythm; crawlers do not. For each UTC day from the peak's day through 6
days after, with 240+ views (10 an hour on average), each hour's share of the
day; averaged per hour of day; `diurnal_amplitude` = (max - min) of those 24
mean shares, relative to a flat 1/24. A spike is flagged when it has at least
`flat_profile_min_days` such days and its amplitude is below
`flat_profile_max_amplitude` (dbt vars, calibrated in Task 5: NOTES). A spike
without enough active days is not flagged either way.

## Curated grain — two tiers

`page_daily` is written for every English page-day with 10 or more daily views.
The floor is applied when the day is compacted (see Storage model); the hourly
partials before compaction carry every page.

`page_hour` is written only where hourly views are 10 or more.

The daily floor removes 75.5% of page-days and 7.4% of views, and shrinks the
compacted day 3.8x, measured on 2026-09-10 (NOTES, Task 3). It cannot hide a
spike, because a spike day clears 1,000 views, but it does remove the quiet days
a baseline is made of -- which is why the baseline is zero-filled (see
Definitions).

Spike qualification requires `daily_views >= 1000`, so a qualifying page has a
half-life point far above a 10-view floor; the truncation affects only the
cosmetic tail of the decay curve.

**Known limitation.** Below the floor, an hour with 0 views and an hour dropped
by the floor are indistinguishable in `page_hour`. The tail of a decay curve is
truncated, not measured. A missing SOURCE hour would be unknown, never zero --
but Task 3 verified all 17,520 source hours present, so since Task 5 a missing
page-hour is read as fewer than 10 views and zero-filled (see Definitions,
hourly zero-fill). Were a source hour ever missing, that rule would have to be
re-examined before any model ran over it. One consequence worth watching: for
a spike that only just qualifies, half of `peak_excess` can land near the floor,
so verify against `page_daily` before trusting a half-life measured near it.

## Exclusions

Drop before analysis:
- `Main_Page` and any per-language equivalent
- any title containing a namespace prefix: `Special:`, `Talk:`, `File:`,
  `Category:`, `Template:`, `Help:`, `Portal:`, `Wikipedia:`, `User:`
- the literal title `-`
- titles that fail UTF-8 decoding

Applied at ingest, and applied again in dbt staging (`stg_page_daily`,
`stg_page_hour`) so no model depends on the ingester having done it. A test
(`assert_no_excluded_titles_in_staging`) proves none survive, and spells the
rules out literally rather than reusing the macro, so a bug in one cannot hide
in the other.

**Candidate pages (Task 5).** Any page with at least one day of 1,000+ views is
a candidate, materialized once as `int_candidate_pages`. Every later model joins
against it rather than the full history: a spike day must clear 1,000, so no
other page can spike, and `page_daily` cannot be read page by page affordably.

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
s3://<bucket>/curated/iceberg/page_hour/     Iceberg, partitioned by dt, sorted by page_title
s3://<bucket>/curated/iceberg/page_daily/    Iceberg, partitioned by dt, sorted by page_title
s3://<bucket>/curated/page_daily/dt=.../part-<source hour>.parquet   ingest staging: hourly partial
s3://<bucket>/curated/page_daily/dt=.../day.parquet                  ingest staging: compacted day
s3://<bucket>/curated/page_hour/dt=/hour=/  ingest staging: plain Parquet, one file per hour
s3://<bucket>/marts/                        dbt outputs
s3://<bucket>/fixtures/raw_48h/*.gz         48 hours of source gz, fixture only
```

Since Task 4 the Iceberg tables `hype_decay.page_hour` and `hype_decay.page_daily`
are the curated zone and the only thing anything downstream reads. The plain
Parquet prefixes are where the ingester and the compactor stage their output;
the two-year backfill's copy there was deleted once the Iceberg tables were
verified identical to it (NOTES, Task 4).

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

**`page_daily` is compacted once a day is whole.** The ingester works an hour at
a time, so it writes one partial per source hour. `ingest/compact_day.py` sums
the 24 partials for a day into a single `day.parquet` and deletes them. Measured
on the fixture hour, a partial is 22.7 MiB, so keeping partials for the whole
window would cost roughly 389 GiB against 37.7 GiB for all of `page_hour` -- the
partials, not the data, would be the bill.

A day is complete when source hours `D T01`..`D T23` **and** `(D+1) T00` are all
done. That last one is the trap: the file named `(D+1) T00` holds `D` 23:00-24:00.

**The daily-views floor is applied at compaction and nowhere else.** An hour
cannot know whether a page will clear a daily threshold, so flooring per hour
would drop pages that qualify once the day is whole. The floor is 10. A
compacted day can be refloored HIGHER from its own `day.parquet`; it can never
be refloored lower, because the rows under the old floor exist only at the
source, so lowering a floor is a re-ingest.

**A compacted day is closed to partials.** `day.parquet` already holds every
hour of the day, so a partial written beside it is counted twice by anything
that lists the partition, and the total looks entirely plausible. The ingester
refuses to write a partial into a day whose manifest day row is `compacted` or
`compacting`. A forced re-ingest of an hour in a compacted day first flips the
day row to `invalidated` and moves `day.parquet` out of the partition into a
quarantine prefix, so the partition reads as a gap until the day is rebuilt.
Rebuilding needs all 24 partials, and compaction deleted 23 of them, so
invalidating a day means re-ingesting the whole day. The compactor rebuilds
only from the 24 expected partial keys, refuses if a compacted object sits
beside them, and checks the rebuilt day's rows and views against the totals the
invalidation recorded.

Compaction stages the new object, deletes the partials, then puts the compacted
object in place. That order leaves the partition briefly EMPTY rather than
briefly DOUBLE-COUNTED: an empty partition is a visible gap, a double count is a
silent wrong answer. The manifest day row is the authority on which days are
compacted.

**Who writes which format.** The ingester writes plain Parquet with Hive-style
partition prefixes (`dt=`, `hour=`). Athena creates the Iceberg tables and
writes into them from that staging copy (Task 4).

Reasoning: writing Iceberg from the ingester would put a pyiceberg and
Glue-catalog dependency inside a script whose entire job is one HTTP GET, one
parse and three PUTs, and it would duplicate catalog work that Athena does
natively. Keeping table format in one place keeps the ingester something that can
be read in one sitting.

The trade-off, stated plainly: until Task 4 runs there are no snapshots and no
atomic commits over the curated zone, so a reader can see a half-written day. The
**manifest**, not the S3 file listing, is the authority on which hours are
complete. Task 2 wrote plain Parquet while this spec still said Iceberg; this
paragraph replaces that silent divergence.

**Task 4 REWRITES into Iceberg; it does not adopt the files in place.** This
reverses the earlier decision in this spec, which favoured Glue's `add_files`
procedure to avoid holding two copies. Measured on March 2025 of `page_hour`
(NOTES, Task 4), an Athena `INSERT ... ORDER BY dt, page_title` into Iceberg:

- stored the month in 313 MiB instead of 1,699 MiB, 5.4x smaller, because a
  title's 24 hours sit together and compress to almost nothing, and zstd
  replaces snappy
- scanned 10.5x fewer bytes for one page's curve across the month, the lookup
  Task 7 serves, and 1.3x fewer for per-day totals
- replaced one 2 MiB file per hour, each spanning the whole alphabet, with
  about 9 files per day

Adoption would have kept all of that as it was: 17,520 files, unsorted, with no
`dt` column inside the `page_hour` files for an Iceberg partition to be built
from. The double storage the earlier reasoning feared lasts only between the
build and the verification, hours rather than months, at under a cent.

Athena writes these files without min/max statistics on `page_title`, so a page
lookup cannot skip row groups by title; the saving comes from the sorted column
being small, not from skipping. Partitioning by `dt` is what prunes.

**`page_daily` cannot skip data on a title filter, and is not rewritten to.**
Measured on 2025-03-14 (NOTES, Task 4): each day is 5 files of one row group
each; the writer records no min/max for text columns; Iceberg's per-file title
bounds exist but every file spans nearly the whole alphabet, because Athena's
parallel writers each sort their own slice of rows. And because a title occurs
once a day, `page_title` overflows Parquet's dictionary and is stored PLAIN:
14.4 MiB a day. So any query filtered on `page_title` reads that whole column for
every day in its `dt` range -- about 10.3 GiB for the full history, more than
the 5 GiB interactive cap. `page_hour`, where titles repeat and stay
dictionary-encoded, reads about 3x less per day for the same lookup.
Consequences, decided: models read `page_daily` in whole-table passes against
the candidate-pages table, never page by page; and single-page lookups for the
public page come from a small serving table built in Task 7, not from either
curated table.

**Once Iceberg owns a table, nothing writes or deletes files behind it.** New
days are written THROUGH Iceberg -- an Athena `INSERT` from the staging prefix,
once a day is compacted -- never by putting a file under `curated/iceberg/`. A
file that appears there without a metadata commit is invisible to readers, and a
file removed without one makes every snapshot that references it unreadable.
The manifest stays the record of what was fetched; Iceberg is the record of
what is queryable.

**Publishing a day, and what the manifest says about it.** `ingest/publish.py`.
A compacted day is published by replacing that `dt` in both Iceberg tables from
its staging copy, then checking the result against the manifest: `page_daily`
rows and views against the day row, `page_hour` rows per hour against each hour
row's `rows_page_hour`. The day row records it:

- `iceberg_state`: `replacing` while the DELETE and INSERT run, `published` once
  they matched the manifest; `iceberg_published_at` and `iceberg_state_at`
- `staging_removed_at`: when the staging `day.parquet` and the day's 24
  `page_hour` staging files were deleted. At the same moment the fields that
  named them -- `key_compacted` on the day row, `key_page_hour` and
  `key_page_daily` on its hour rows -- are REMOVED. The manifest never names a
  file that does not exist.

Staging is retired only for a day whose `iceberg_state` is `published`: it is
the only other copy until then.

**Correcting a day after the switch.** `ingest/republish_day.py --dt D`:

1. re-ingest D's 24 source hours with `--force`. The day row flips to
   `invalidated` (double-count guard) and the partials come back to staging.
2. recompact. Built from exactly those 24 partials, and checked against the
   totals the invalidation recorded.
3. replace D in Iceberg: `DELETE ... WHERE dt = D`, then `INSERT ... SELECT`
   from staging, in each table, then check against the manifest. Never an INSERT
   alongside the old rows -- that is a double count inside Iceberg. Between the
   DELETE and the INSERT a reader sees D empty: a gap, never twice. A `MERGE`
   would be one commit, but Athena's has no `WHEN NOT MATCHED BY SOURCE`, so it
   could not remove a row the corrected data no longer has.
4. retire D's staging copy, as above.

The script snapshots rows, views and a row-level checksum for every day of D's
month before and after, and fails unless D is as intended and no other day
changed. A refloor of a retired day is refused by `compact_day.py`: there is no
staging file left to refloor.

**Where a correction runs.** A single-day correction may run from the laptop:
the one proof run (2025-03-15) did, with 24 downloads on one connection and one
compaction. **Any correction touching more than one day runs on the backfill
instance, not the laptop.** Three reasons, all measured: a day's compaction
peaks at 3.0 to 4.9 GiB and the laptop had 0.5 GiB free, so it pages; every day
is about 1.4 GB fetched, which belongs on an AWS network rather than the home
connection the Wikimedia rate limits apply to; and the instance's wrapper, stall
alarm and self-termination make a multi-hour job safe to leave, where a laptop
that sleeps -- as it did during backfill run 1's monitoring -- simply stops.

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
