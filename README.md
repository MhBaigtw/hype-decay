# hype-decay

Measuring how long the internet stays interested in something.

When a topic spikes in public attention, how many hours until that attention
falls to half its peak? Some spikes are gone in a day. Some never fully fade.

Built on AWS. See `SPEC.md` for metric definitions, `TASKS.md` for build
order, `CLAUDE.md` for constraints, `HANDOFF.md` for the Claude Code kickoff
prompt and review loop.

## Status

Task 0 (recon) and Tasks 1-2 complete: account guardrails, Terraform backend, and
one hour of English Wikipedia pageviews landing in the curated zone as Parquet at
two grain tiers. Task 3, the two-year backfill, is in design.

## Known limitations

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
