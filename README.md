# hype-decay

Measuring how long the internet stays interested in something.

When a topic spikes in public attention, how many hours until that attention
falls to half its peak? Some spikes are gone in a day. Some never fully fade.

Built on AWS. See `SPEC.md` for metric definitions, `TASKS.md` for build
order, `CLAUDE.md` for constraints, `HANDOFF.md` for the Claude Code kickoff
prompt and review loop.

## Status

Tasks 0 to 5 complete: account guardrails, Terraform backend, ingestion, the
two-year backfill (17,520 of 17,520 hours, 0 failed), the curated zone as two
Iceberg tables in the Glue Data Catalog, and the dbt models that detect spikes
and measure how fast attention fades. Task 6, the hourly incremental, is next.

## How fast attention fades

681,885 spikes across two years of English Wikipedia. 22,300 qualify for the
leaderboards: at least 20,000 views of excess on the peak day, and none of the
flags below.

| Attention half-life | Qualifying spikes (n = 21,491) | Including rekindled spikes (n = 24,092) |
|---|---|---|
| fastest quarter | 14 hours or less | 16 hours or less |
| **median** | **27 hours** | **33 hours** |
| slowest quarter | 66 hours or more | 98 hours or more |

**The 27-hour median excludes rekindled spikes** -- 15.1% of all spikes, and
10.6% (2,651 of 24,951) of those big enough to qualify -- because their measured
"decay" includes a second, later event (see Flags below). Counting them anyway
gives a median of **33 hours**. Both figures are uncensored spikes only.

Half of a typical spike's 30-day excess attention arrives within about a day
of its onset; the median spike gets 13.8% of that excess after its first week.

**Why "attention" half-life, and not the obvious one.** The first definition
measured from the busiest single hour: how long until hourly views fall to half
of the peak hour's. It passed every unit test and gave a 3-hour median, and the
sanity check showed why that was wrong:

| Event | Peak-hour half-life | Attention half-life | What happened |
|---|---|---|---|
| Pope_Leo_XIV, 2025-05-08 | 1 h | 6 h | 3.86M views in the announcement hour; days of 7.54M, 2.97M, 933k |
| Liam_Payne, 2024-10-16 | 1 h | 20 h | his SECOND day (3.70M) was bigger than the first (2.44M) |
| Donald_Trump, 2024-11-06 | 9 h | 28 h | 2.78M, then 949k, then 516k |

A news-break hour towers over everything after it, so the peak-hour measure
timed that one hour. The attention half-life instead runs from the spike's
onset to the moment half of its 30-day excess has arrived.

**Flags, never filters.** 18,611 spikes are `burst` (automated traffic counted as
human: `Schutzstaffel` took 7.8M views in one hour and 278 in the next), 102,816
are `rekindled` (a second, bigger event inside the window -- `Rory_McIlroy`'s
March spike swallowing his Masters win), and 735 are calendar list pages. All
stay in the data, flagged; they are only kept off the leaderboards.

## The curated zone, measured

Two Iceberg tables, `hype_decay.page_hour` and `hype_decay.page_daily`, partitioned
by day (`dt`), sorted by page title, zstd-compressed. **18.49 GiB** for two years
of English Wikipedia: `page_daily` 11.96 GiB (1.28 billion page-days), `page_hour`
6.53 GiB (2.70 billion page-hours).

**Partition pruning.** The same query -- one page's total views -- with and without
a filter on the partition column:

| Query on `page_hour` (Taylor_Swift) | Bytes scanned |
|---|---|
| one day                              | 4.9 MiB |
| one month                            | 158.1 MiB |
| whole table, no partition filter     | 3,172 MiB -- 650x the one-day query |

On `page_daily` the same unfiltered query was **cancelled at 5,120 MiB** by the
Athena workgroup's per-query scan limit, the guardrail doing its job; one day
scans 14.9 MiB. Every query in this project filters on `dt`.

**Rewrite versus adopt-in-place.** Before building, one month (March 2025) was
measured both ways: Iceberg metadata over the files as the ingester wrote them,
versus an Athena rewrite sorted by page title. The rewrite stored `page_hour`
**5.4x smaller** (1,699 MiB to 313 MiB) and scanned **10.5x fewer bytes** for a
one-page lookup across the month (1,695 MiB to 162 MiB). Across the whole table
`page_hour` went from 35.84 GiB to 6.53 GiB.

**Correctness.** Every one of the 730 days was compared between the old copy and
the Iceberg tables -- row count, total views and a checksum over every row -- before
the old copy was deleted: identical. Both tables match the ingestion manifest
exactly, per day and per hour. A spot check of 5 well-known pages on 3 dates
against the Wikimedia Pageviews REST API matched **15 of 15 exactly**, including
2,782,082 views of `Donald_Trump` on 2024-11-06.

## Known limitations

**Some automated bursts still get through.** The burst flag catches a one-hour
spike ten times its neighbours, and a two-hour spike that rises out of silence.
It misses a two-hour automated burst on a page that already had a little
traffic: `Turing_test` (2026-04-15) went 44, 58,664, 39,990, then 2,589 an hour,
and the *Hell's Kitchen* season 9 page (2026-04-23) and `Spain` (2026-02-02)
have the same shape. They sit on the fastest-fading leaderboard with 1-hour
half-lives. The rest of that list is real: broadcast moments where attention
really did arrive in one hour -- New Year's Eve performances (Diana Ross, Paul
Carrack, Marc Almond) and Joe Montana on Super Bowl night.

**"Rekindled" is a strict comparison.** A spike is only flagged when a later day
beats its peak day. The slowest-fading leaderboard therefore includes spikes
whose later days came close -- `Carla_Bruni` (a later day at 99% of the peak
day), `Wes_Streeting` (97%), `Kash_Patel` (87%) -- which are sustained or
repeated attention more than a slow fade.

**Anticipated events start their clock early.** The half-life runs from onset,
the first hour within 48 hours of the peak to reach 10% of the peak's excess. For
an event people saw coming, that is the build-up: `Kamala_Harris`'s onset was
39 hours before her election-night peak, so she scores a 43-hour half-life
against Donald Trump's 28, though her post-election drop was steeper.

**Looking up one spike costs about $0.10.** A filter on one spike does not reach
the scans of the hourly table underneath, so every single-spike query reads
gigabytes. The public page (Task 7) must read a small precomputed serving table,
never these models.


**Looking up one page in `page_daily` reads the whole day, every day.** The
title column cannot be skipped: Athena writes no text statistics, and its
parallel writers leave every file spanning the whole alphabet. A one-page,
one-day lookup reads 14.4 MiB, and one page across the full two years about
10.3 GiB, more than the interactive scan cap allows. The models therefore read
`page_daily` in whole-table passes, and single-page lookups for the public page
will come from a small serving table (Task 7).

These are properties of the data, not bugs to be fixed later. Both affect how the
numbers should be read.

**Automated traffic counted as human.** The hourly `pageviews` dataset is already
filtered to Wikimedia's `user` agent class -- confirmed against the Wikimedia REST
API to the exact view -- so this is not the usual "bot traffic is unfiltered"
caveat. The residual problem is traffic Wikimedia MISCLASSIFIES as `user`. Worked
example: `.xyz` took 24,025 views in a single hour on 2026-09-10 and survives the
namespace exclusions, while the REST API splits that day as 121k `user` against
262k `automated`. Inside our data the user-classified share is indistinguishable
from a human reader, so spikes on obscure titles deserve suspicion. A flag for
spikes whose hourly profile is suspiciously flat is planned, since human attention
has a diurnal shape and crawlers do not.

**Counts are per requested title, not per resolved article.** The source records
the title as requested, so a redirect and its target are counted separately:
traffic to `Charlie_Kirk_assassination` does not appear under
`Assassination_of_Charlie_Kirk`. A page with many redirects therefore shows a
lower peak than it really had, and a rename mid-event splits one spike into two
curves. Resolving redirects needs a page-to-canonical mapping the dataset does not
carry, which is why `page_id` was evaluated in Task 0 and why fixing this is out
of scope for v1 rather than merely unfinished.
