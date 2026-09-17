# SPEC

## The question

When something spikes in public attention, how long does it take for that
attention to fall to half its peak?

v1 answers this for English Wikipedia. v2 adds news via GDELT and asks whether
news decays at a different rate than encyclopedia traffic. v3 adds a live
social stream.

## Definitions — implement exactly these

All times UTC. All grains hourly unless stated.

**`hour_start`** — the beginning of a capture window. The source filename
carries the END of the window, so `hour_start = filename_hour - 1 hour`.

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

Alternative with bot filtering and page IDs: `pageview_complete`, daily bz2
files. Known quirk: rows without a page ID have 5 columns, rows with one have
6. The parser must handle both widths. Prefer this dataset if recon shows it
is tractable — see Task 0.

## Source: GDELT 2.0 (v2 only)

`http://data.gdeltproject.org/gdeltv2/lastupdate.txt` lists the current
15-minute slice as `<size> <md5> <url>` for export, mentions and gkg.

The mentions table is tab-delimited, 16 columns, no header. `EventTimeDate`
and `MentionTimeDate` are `YYYYMMDDHHMMSS`. The gap between them is news
attention lag, measured per mention. That field pair is the reason GDELT is in
this project.

Entity matching against Wikipedia is a **v2 design task, not a v1
implementation detail**. Do not build it during v1. The intended approach is a
bounded watchlist: take the top few hundred spiking Wikipedia pages per day as
the entity list, normalize titles (underscores to spaces), and match against
GDELT GKG name fields. Open-world entity resolution is explicitly out of
scope and will not be attempted.

## Storage model

```
s3://<bucket>/raw/pageviews/dt=YYYY-MM-DD/hour=HH/*.gz      untouched source
s3://<bucket>/curated/page_hour/          Iceberg, partitioned by dt
s3://<bucket>/marts/                      dbt outputs
```

Raw stays byte-identical to the source. Every transform must be reproducible
from raw alone. If a parsing bug is found in month three, the fix is a
reprocess, not a re-download.

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
