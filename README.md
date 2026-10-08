# hype-decay

Measuring how long the internet stays interested in something.

When a topic spikes in public attention, how many hours until that attention
falls to half its peak? Some spikes are gone in a day. Some never fully fade.

Built on AWS. See `SPEC.md` for metric definitions, `TASKS.md` for build
order, `CLAUDE.md` for constraints, `HANDOFF.md` for the Claude Code kickoff
prompt and review loop.

## Status

Tasks 0 to 4 complete: account guardrails, Terraform backend, ingestion, the
two-year backfill (17,520 of 17,520 hours, 0 failed), and the curated zone as two
Iceberg tables in the Glue Data Catalog, queryable in Athena. Task 5, the dbt
models, is next.

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
