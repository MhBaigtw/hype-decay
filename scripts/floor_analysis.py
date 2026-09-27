#!/usr/bin/env python3
"""
Does `page_daily` need every page, or can a minimum daily-views floor pay for
itself? Two measurements, because the question has two halves.

  --baseline-impact   Does a floor destroy the PRE-SPIKE BASELINE? Uses the
                      Wikimedia REST API for daily history, so it needs no
                      backfill and pulls nothing from the dumps server.
  --distribution      What do rows and bytes actually cost at each floor? Reads
                      a real day of page_daily partials out of S3 and re-writes
                      it at each candidate floor to measure Parquet size.

The baseline is the thing at risk. SPEC computes it as the median of trailing 28
daily totals ending 2 days before the day being judged, and spike qualification
needs `daily_views >= 5 * baseline`. A page that sits at 8 views a day and jumps
to 4,000 is exactly the kind of spike this project exists to measure -- and it is
also exactly the page a floor of 10 would erase the history of. Losing baseline
days does not make a spike smaller; it makes the baseline WRONG, which silently
changes which spikes are detected at all.

    python3 floor_analysis.py --baseline-impact --sample 200
    python3 floor_analysis.py --distribution --dt 2026-09-10
"""

import argparse
import io
import json
import os
import random
import statistics
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta

REST = "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article"
USER_AGENT = "hype-decay-analysis/0.1 (MhBaig971@gmail.com)"
POLITE_DELAY_SEC = 0.3  # REST API, not the dumps server; still be polite

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
CANDIDATE_FLOORS = (1, 5, 10, 25, 50, 100)

# SPEC spike definition.
BASELINE_DAYS = 28
BASELINE_OFFSET_DAYS = 2
SPIKE_RATIO = 5
SPIKE_ABSOLUTE_MARGIN = 500
SPIKE_FLOOR_VIEWS = 1000

# Strata by views in the sample file, so the sample is not all popular pages.
# The risk case is a QUIET page that spikes, not Barack Obama.
#
# These strata must be drawn from page_daily, not page_hour: page_hour already
# has a 10-views-an-hour floor, so sampling it excludes almost every page a
# daily floor would affect. Sampling the floored tier to judge a floor is
# circular, and the first version of this script did exactly that.
STRATA = ((1, 2), (2, 5), (5, 10), (10, 50), (50, 1000), (1000, 10 ** 9))


def log(msg):
    print(f"  {msg}", flush=True)


def get_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = json.loads(response.read())
        time.sleep(POLITE_DELAY_SEC)
        return body
    except urllib.error.HTTPError as e:
        time.sleep(POLITE_DELAY_SEC)
        return None if e.code == 404 else None


def daily_history(title, start, end):
    """{date: views} for agent=user. Days the API omits are genuinely absent."""
    quoted = urllib.parse.quote(title, safe="")
    body = get_json(f"{REST}/en.wikipedia/all-access/user/{quoted}/daily/"
                    f"{start:%Y%m%d}/{end:%Y%m%d}")
    if not body or "items" not in body:
        return {}
    return {datetime.strptime(i["timestamp"][:8], "%Y%m%d").date(): i["views"]
            for i in body["items"]}


def baseline_for(history, day):
    """Median of the 28 days ending 2 days before `day`, per SPEC.

    A day the API omits is treated as 0 views, which is what it means for the
    pageviews data: no rows for that page that day. A real upstream outage would
    also look like this, which is a known limitation of using REST as the
    reference here.
    """
    window_end = day - timedelta(days=BASELINE_OFFSET_DAYS)
    window = [window_end - timedelta(days=offset)
              for offset in range(BASELINE_DAYS - 1, -1, -1)]
    values = [history.get(d, 0) for d in window]
    return statistics.median(values), values


def qualifies(daily_views, baseline):
    return (daily_views >= SPIKE_RATIO * baseline
            and daily_views >= baseline + SPIKE_ABSOLUTE_MARGIN
            and daily_views >= SPIKE_FLOOR_VIEWS)


def find_first_spike(history):
    """First day that qualifies and has a full baseline window behind it."""
    if not history:
        return None
    for day in sorted(history):
        earliest = day - timedelta(days=BASELINE_OFFSET_DAYS + BASELINE_DAYS)
        if earliest < min(history):
            continue
        baseline, values = baseline_for(history, day)
        if qualifies(history[day], baseline):
            return {"day": day, "views": history[day],
                    "baseline": baseline, "window": values}
    return None


def sample_titles(bucket, key, per_stratum, seed):
    """Stratified sample of titles from a page_hour partition already in S3."""
    import boto3
    import pyarrow.parquet as pq

    s3 = boto3.Session(profile_name=os.environ.get("AWS_PROFILE")).client("s3")
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    table = pq.read_table(io.BytesIO(body))
    rows = list(zip(table.column("page_title").to_pylist(),
                    table.column("views").to_pylist()))
    log(f"read {len(rows):,} page_hour rows from s3://{bucket}/{key}")

    rng = random.Random(seed)
    picked = []
    for low, high in STRATA:
        bucketed = [t for t, v in rows if low <= v < high]
        chosen = rng.sample(bucketed, min(per_stratum, len(bucketed)))
        picked.extend((t, f"{low}-{high}") for t in chosen)
        log(f"  stratum {low:>5}-{high:<10} {len(bucketed):>7,} pages, sampled {len(chosen)}")
    return picked


def baseline_impact(args):
    print("=" * 72)
    print("A. DOES A DAILY-VIEWS FLOOR DESTROY THE PRE-SPIKE BASELINE?")
    print("=" * 72)

    key = f"curated/page_daily/dt={args.dt}/part-{args.dt}T{args.source_hour}.parquet"
    titles = sample_titles(args.bucket, key, args.sample // len(STRATA), args.seed)

    end = date.fromisoformat(args.dt) + timedelta(days=10)
    start = end - timedelta(days=args.history_days)
    log(f"REST daily history {start} .. {end} for {len(titles)} titles "
        f"(about {len(titles) * POLITE_DELAY_SEC / 60:.1f} min)")

    spikes, quiet, checked, no_history = [], [], 0, 0
    for title, stratum in titles:
        history = daily_history(title, start, end)
        checked += 1
        if not history:
            no_history += 1
            continue
        spike = find_first_spike(history)
        if spike:
            spike["title"] = title
            spike["stratum"] = stratum
            spikes.append(spike)
        else:
            # Pages that never spike matter just as much: a floor that deletes
            # their quiet days drags their baseline toward zero, which can make
            # an ordinary rise look like a spike. That is a FALSE result, and it
            # is worse than a gap because nothing downstream can detect it.
            quiet.append({"title": title, "history": history, "stratum": stratum})
        if checked % 25 == 0:
            log(f"  {checked}/{len(titles)} checked, {len(spikes)} spikes so far")

    print()
    log(f"{checked} titles checked, {no_history} with no REST history, "
        f"{len(spikes)} spikes detected per the SPEC definition")
    if not spikes:
        log("no spikes in this sample: widen --sample or --history-days")
        return 0

    baselines = sorted(s["baseline"] for s in spikes)
    log(f"spike baselines: min {baselines[0]:,.0f}, median "
        f"{statistics.median(baselines):,.0f}, max {baselines[-1]:,.0f}")
    below = Counter()
    for floor in CANDIDATE_FLOORS:
        below[floor] = sum(1 for b in baselines if b < floor)
    log("spikes whose whole baseline sits below the floor: " +
        ", ".join(f"floor {f}: {below[f]}/{len(spikes)}" for f in CANDIDATE_FLOORS))

    print()
    print(f"  {'floor':>6}  {'days lost /28':>21}  {'baseline moved >10%':>19}  "
          f"{'real spikes lost':>16}  {'FALSE spikes created':>20}")
    print("  " + "-" * 88)
    for floor in CANDIDATE_FLOORS:
        lost, moved, missed = [], 0, 0
        for spike in spikes:
            window = spike["window"]
            lost.append(sum(1 for v in window if v < floor))
            as_zero = statistics.median([v if v >= floor else 0 for v in window])
            true_baseline = spike["baseline"]
            if true_baseline and abs(as_zero - true_baseline) / true_baseline > 0.10:
                moved += 1
            if not qualifies(spike["views"], as_zero):
                missed += 1

        # The dangerous direction: quiet pages promoted into spikes because the
        # floor deleted the days that proved they were quiet.
        false_spikes = 0
        for page in quiet:
            history = page["history"]
            for day in sorted(history):
                earliest = day - timedelta(days=BASELINE_OFFSET_DAYS + BASELINE_DAYS)
                if earliest < min(history):
                    continue
                true_baseline, window = baseline_for(history, day)
                as_zero = statistics.median([v if v >= floor else 0 for v in window])
                if qualifies(history[day], as_zero) and not qualifies(history[day], true_baseline):
                    false_spikes += 1
                    break

        print(f"  {floor:>6}  median {statistics.median(lost):>4.1f} max {max(lost):>3}  "
              f"{moved:>16}/{len(spikes)}  {missed:>15}/{len(spikes)}  "
              f"{false_spikes:>17}/{len(quiet)}")

    print()
    log("Reading this table: 'days lost' counts baseline days with no row at")
    log("that floor. 'real spikes lost' is spikes that stop qualifying. 'FALSE")
    log("spikes created' is quiet pages that start qualifying because the floor")
    log("deleted the evidence that they were quiet -- the worst outcome, since")
    log("nothing downstream can tell a fabricated spike from a real one.")

    worst = sorted(spikes, key=lambda s: s["baseline"])[:5]
    print()
    log("quietest spiking pages in the sample (the ones a floor hurts):")
    for spike in worst:
        log(f"  {spike['title'][:44]:<44} baseline {spike['baseline']:>8,.0f} "
            f"-> {spike['views']:>9,} on {spike['day']}")
    return 0


def distribution(args):
    """Rows and bytes retained at each floor, from a real compacted day."""
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    print("=" * 72)
    print(f"B. ROWS AND BYTES AT EACH FLOOR, dt={args.dt}")
    print("=" * 72)

    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"))
    s3 = session.client("s3")
    prefix = f"curated/page_daily/dt={args.dt}/"
    listing = s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix).get("Contents", [])
    if not listing:
        log(f"nothing under s3://{args.bucket}/{prefix} -- ingest the day first")
        return 1
    log(f"{len(listing)} partial object(s), "
        f"{sum(o['Size'] for o in listing) / 2 ** 20:,.1f} MiB total")

    totals = defaultdict(int)
    for obj in listing:
        body = s3.get_object(Bucket=args.bucket, Key=obj["Key"])["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        for title, views in zip(table.column("page_title").to_pylist(),
                                table.column("views").to_pylist()):
            totals[title] += views
    log(f"{len(totals):,} distinct pages across the day, "
        f"{sum(totals.values()):,} views")

    print()
    print(f"  {'floor':>6}  {'rows':>12}  {'rows kept':>10}  {'parquet':>12}  "
          f"{'bytes kept':>11}  {'views kept':>11}")
    print("  " + "-" * 72)
    base_bytes = base_rows = None
    for floor in (0,) + CANDIDATE_FLOORS:
        kept = {t: v for t, v in totals.items() if v >= floor}
        schema = pa.schema([("project", pa.string()), ("page_title", pa.string()),
                            ("dt", pa.date32()), ("views", pa.int64()),
                            ("hours_present", pa.int32())])
        day = date.fromisoformat(args.dt)
        buf = io.BytesIO()
        pq.write_table(pa.Table.from_pydict({
            "project": ["en.wikipedia"] * len(kept),
            "page_title": list(kept),
            "dt": [day] * len(kept),
            "views": list(kept.values()),
            "hours_present": [24] * len(kept),
        }, schema=schema), buf, compression="snappy")
        size = buf.tell()
        if base_bytes is None:
            base_bytes, base_rows = size, len(kept)
        print(f"  {floor:>6}  {len(kept):>12,}  {100 * len(kept) / base_rows:>9.1f}%  "
              f"{size / 2 ** 20:>9,.1f} MiB  {100 * size / base_bytes:>10.1f}%  "
              f"{100 * sum(kept.values()) / sum(totals.values()):>10.2f}%")
    print()
    log("Compare the bytes-kept column against what compaction alone saves:")
    log("24 partials into 1 object removes duplication, a floor removes pages.")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--baseline-impact", action="store_true")
    ap.add_argument("--distribution", action="store_true")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--dt", default="2026-09-10", help="curated partition date")
    ap.add_argument("--hour", default="17", help="hour partition of the sample page_hour file")
    ap.add_argument("--source-hour", default="18", help="source hour in that filename")
    ap.add_argument("--sample", type=int, default=200)
    ap.add_argument("--history-days", type=int, default=90)
    ap.add_argument("--seed", type=int, default=20260927)
    args = ap.parse_args()

    if args.baseline_impact:
        return baseline_impact(args)
    if args.distribution:
        return distribution(args)
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
