#!/usr/bin/env python3
"""
Task 0 cross-checks. Regenerates every number in the Task 0 entry of NOTES.md.

recon.py measures one sample hour and one GDELT slice. This script checks the
claims a single sample cannot support:

  A  hour convention, agent filtering, mobile share -- dump totals vs REST API
  B  volume and gaps            -- every file in the 2-year window, from listings
  C  pageview_complete widths   -- first 2 MB of two daily files
  D  hour-letter convention     -- pageview_complete vs REST, hour by hour
  E  bot misclassification      -- REST agent split for suspect titles
  F  estimates                  -- backfill hours and S3 monthly cost

Stdlib only. No AWS. Sequential and polite: Wikimedia caps you at 3 connections
per IP, so this uses exactly one and sleeps between requests. Do not
parallelise it. The User-Agent comes from recon.py -- edit it there, not here.

    python3 recon_verify.py                 # all sections
    python3 recon_verify.py --only A,F      # selected sections

Findings as measured on 2026-09-13, recorded in NOTES.md:

  hourly file 61.4 MiB sample, 57.6 MB window mean, 17,520 of 17,520 present
  2-year raw: pageviews 939 GiB, pageview_complete 437 GiB
  backfill at 3 connections: 24.4 h vs 10.7 h (estimate)
  mobile 59.7% of en views; pageviews already agent-filtered to user, exactly
  pageview_complete: 6 fields throughout, letter A = the hour starting 00:00
"""

import argparse
import bz2
import gzip
import json
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from recon import (  # noqa: E402  - deliberate: one User-Agent, one policy
    POLITE_DELAY_SEC,
    USER_AGENT,
    human,
    parse_pageviews,
    pick_sample_hour,
    wiki_complete_url,
    wiki_pageviews_url,
)

REST = "https://wikimedia.org/api/rest_v1/metrics/pageviews"
DUMPS = "https://dumps.wikimedia.org/other"

# The 2-year backfill window Task 0 measured. 730 days, inclusive.
WINDOW_START = date(2024, 9, 13)
WINDOW_END = date(2026, 9, 12)

# Measured by recon.py on 2026-09-13: 61.4 MiB in 15.7 s on one connection.
MEASURED_BYTES, MEASURED_SECONDS = 61.4 * 2 ** 20, 15.7
S3_STANDARD_USD_PER_GB_MONTH = 0.023  # us-east-1 list price, not re-checked
BUDGET_USD = 30.0

# Fallback volumes, so section F can run alone. Section B overrides them.
MEASURED_VOLUMES = {"pageviews": 1008.6e9, "pageview_complete": 469.4e9}
FILE_COUNTS = {"pageviews": 17520, "pageview_complete": 730}

LISTING_RE = re.compile(r'href="([^"]+)">[^<]*</a>\s+(\d{2}-\w{3}-\d{4} \d{2}:\d{2})\s+(\d+)')


def section(title):
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def ok(msg):
    print(f"  [ok]    {msg}")


def warn(msg):
    print(f"  [WARN]  {msg}")


def get(url, headers=None):
    """One connection, identified, with a pause after every request."""
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT, **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read()
            time.sleep(POLITE_DELAY_SEC)
            return resp.status, body
    except urllib.error.HTTPError as e:
        time.sleep(POLITE_DELAY_SEC)
        return e.code, e.read()


def rest_hourly(project, access, agent, start, end):
    """Returns {hour: views} from the REST aggregate endpoint.

    REST timestamps label the hour that STARTS then, which is the reference
    the dump filename convention is checked against.
    """
    status, body = get(f"{REST}/aggregate/{project}/{access}/{agent}/hourly/{start}/{end}")
    if status != 200:
        return None
    return {int(i["timestamp"][8:10]): i["views"] for i in json.loads(body)["items"]}


def rest_article_daily(title, agent, day):
    quoted = urllib.parse.quote(title, safe="")
    status, body = get(
        f"{REST}/per-article/en.wikipedia/all-access/{agent}/{quoted}/daily/{day}/{day}")
    if status != 200:
        return None
    return json.loads(body)["items"][0]["views"]


def months_in_window():
    out, d = [], date(WINDOW_START.year, WINDOW_START.month, 1)
    while d <= WINDOW_END:
        out.append(d)
        d = date(d.year + (d.month == 12), d.month % 12 + 1, 1)
    return out


def complete_sample(url, nbytes=2 * 1024 * 1024):
    """First nbytes of a .bz2, decompressed to whole lines. One Range request.

    The final line is dropped because the byte range cuts it mid-row.
    """
    status, body = get(url, {"Range": f"bytes=0-{nbytes - 1}"})
    if status not in (200, 206):
        return status, []
    raw = bz2.BZ2Decompressor().decompress(body).decode("utf-8", "replace")
    return status, raw.splitlines()[:-1]


# ---------------------------------------------------------------------------
# A. hour convention, agent filtering, mobile share
# ---------------------------------------------------------------------------

def check_hour_convention():
    section("A. HOUR CONVENTION, AGENT FILTERING, MOBILE SHARE")
    dt = pick_sample_hour()
    url = wiki_pageviews_url(dt)
    print(f"  sample file : {url}")
    print(f"  SPEC rule   : hour_start = filename_hour - 1, so this file covers "
          f"{dt - timedelta(hours=1):%H}:00-{dt:%H}:00 UTC\n")

    status, body = get(url)
    if status != 200:
        warn(f"download failed, status={status}")
        return
    ok(f"downloaded {human(len(body))}")
    stats = parse_pageviews(gzip.decompress(body).decode("utf-8", "replace"))
    desktop, mobile = stats["en_desktop_views"], stats["en_mobile_views"]
    total = desktop + mobile
    ok(f"dump totals: en {desktop:,} desktop, en.m {mobile:,} mobile")
    ok(f"mobile share {100 * mobile / total:.2f}% of {total:,} en views, one hour")

    fh = dt.hour
    start = f"{dt:%Y%m%d}{max(0, fh - 2):02d}"
    end = f"{dt:%Y%m%d}{min(23, fh + 1):02d}"
    print("\n  REST aggregate, same day, hours either side of the filename hour:")
    table = {}
    for access in ("desktop", "mobile-web", "mobile-app"):
        for agent in ("all-agents", "user", "spider", "automated"):
            hours = rest_hourly("en.wikipedia", access, agent, start, end)
            if hours is None:
                print(f"    {access:<10} {agent:<10} unavailable")
                continue
            table[(access, agent)] = hours
            print(f"    {access:<10} {agent:<10} " +
                  "  ".join(f"{h:02d}h={v:,}" for h, v in sorted(hours.items())))

    print()
    for label, dump_value, accesses in (
        ("en (desktop)", desktop, ["desktop"]),
        ("en.m (mobile)", mobile, ["mobile-web", "mobile-app"]),
    ):
        for agent in ("user", "all-agents"):
            series = [table.get((a, agent)) for a in accesses]
            if any(s is None for s in series):
                continue
            for hour in sorted(series[0]):
                if sum(s.get(hour, 0) for s in series) != dump_value:
                    continue
                verdict = ("CONFIRMS filename_hour - 1" if hour == fh - 1
                           else "CONTRADICTS the spec rule")
                ok(f"{label} == REST {agent} hour {hour:02d} exactly "
                   f"({dump_value:,}) -- {verdict}")
    print("\n       An exact match against agent 'user', rather than")
    print("       'all-agents', is what shows the pageviews dump is already")
    print("       agent-filtered. There is no bot traffic left to remove.")


# ---------------------------------------------------------------------------
# B. volume and gaps across the whole window
# ---------------------------------------------------------------------------

def check_volume_and_gaps():
    section("B. 2-YEAR VOLUME AND GAPS (every file, from the monthly listings)")
    print(f"  window {WINDOW_START} .. {WINDOW_END} "
          f"({(WINDOW_END - WINDOW_START).days + 1} days)")
    volumes = {}
    datasets = (
        ("pageviews", re.compile(r"pageviews-(\d{8})-(\d{2})0000\.gz$"), 24),
        ("pageview_complete", re.compile(r"pageviews-(\d{8})-user\.bz2$"), 1),
    )
    for name, pattern, per_day in datasets:
        total = files = 0
        seen, latest = set(), None
        for month in months_in_window():
            status, body = get(f"{DUMPS}/{name}/{month:%Y}/{month:%Y-%m}/")
            if status != 200:
                warn(f"{name} {month:%Y-%m} listing status={status}")
                continue
            for fname, mtime, size in LISTING_RE.findall(body.decode("utf-8", "replace")):
                m = pattern.search(fname)
                if not m:
                    continue
                fday = datetime.strptime(m.group(1), "%Y%m%d").date()
                key = (fday, m.group(2) if per_day == 24 else "")
                if latest is None or key > latest[0]:
                    latest = (key, fname, mtime)
                if WINDOW_START <= fday <= WINDOW_END:
                    seen.add(key)
                    total += int(size)
                    files += 1
        expected = set()
        day = WINDOW_START
        while day <= WINDOW_END:
            expected |= ({(day, f"{h:02d}") for h in range(24)} if per_day == 24
                         else {(day, "")})
            day += timedelta(days=1)
        missing = sorted(expected - seen)
        volumes[name] = total
        ok(f"{name}: {files:,} files of {len(expected):,} expected, "
           f"{total / 1e9:,.1f} GB ({total / 2 ** 30:,.1f} GiB), "
           f"mean {total / max(files, 1) / 1e6:,.1f} MB per file")
        if missing:
            warn(f"{name}: {len(missing)} missing, e.g. "
                 f"{[f'{d:%Y-%m-%d} {h}'.strip() for d, h in missing[:10]]}")
        else:
            ok(f"{name}: no gaps, every expected file present")
        if latest:
            print(f"     latest published: {latest[1]} at {latest[2]} "
                  f"(listing mtime, assumed UTC)")
    print("\n       Zero gaps is the argument for having no raw zone: upstream")
    print("       is a complete, stable, permanent archive of every hour.")
    return volumes


# ---------------------------------------------------------------------------
# C. pageview_complete field widths
# ---------------------------------------------------------------------------

def check_complete_format():
    section("C. pageview_complete FIELD WIDTHS (the claimed 5-column quirk)")
    print("  Deleted claim: rows without a page ID have 5 columns, others 6.\n")
    grand = Counter()
    for day in (date(2024, 9, 13), date(2026, 9, 10)):
        url = wiki_complete_url(datetime(day.year, day.month, day.day, tzinfo=timezone.utc))
        status, lines = complete_sample(url)
        widths = Counter(len(line.split(" ")) for line in lines)
        grand.update(widths)
        ok(f"{day}: status={status}, {len(lines):,} rows, field counts {dict(widths)}")
        if lines:
            print(f"     example row: {lines[0][:120]}")
            nulls = sum(1 for line in lines
                        if len(line.split(" ")) == 6 and line.split(" ")[2] == "null")
            print(f"     page_id is the literal 'null' in {nulls:,} rows")
    if 5 in grand:
        warn(f"5-field rows DO exist: {grand[5]:,} of {sum(grand.values()):,} sampled")
    else:
        ok(f"no 5-field rows in {sum(grand.values()):,} sampled -- claim not reproduced")
    print("       Only the first 2 MB of each file is read, so this does not")
    print("       prove the quirk never occurs, only that it is not the norm.")


# ---------------------------------------------------------------------------
# D. pageview_complete hour letters
# ---------------------------------------------------------------------------

def check_hour_letters():
    section("D. pageview_complete HOUR LETTERS vs REST (A is which hour?)")
    day = date(2026, 9, 10)
    url = wiki_complete_url(datetime(day.year, day.month, day.day, tzinfo=timezone.utc))
    status, lines = complete_sample(url)
    print(f"  {url}\n  status={status}, {len(lines):,} rows sampled\n")

    hourly = defaultdict(lambda: [0] * 24)
    totals, order, mismatched = Counter(), [], 0
    for line in lines:
        parts = line.split(" ")
        if len(parts) != 6:
            continue
        wiki, _title, _page_id, access, daily, counts = parts
        if not order or order[-1] != wiki:
            order.append(wiki)
        pairs = re.findall(r"([A-X])(\d+)", counts)
        if sum(int(c) for _, c in pairs) != int(daily):
            mismatched += 1
        for letter, count in pairs:
            hourly[(wiki, access)][ord(letter) - ord("A")] += int(count)
        totals[wiki] += int(daily)
    ok(f"rows where the hourly letters do not sum to daily_total: {mismatched}")

    # The last project in the sample is cut off by the byte range, so drop it.
    for wiki in sorted(order[:-1], key=lambda w: -totals[w])[:4]:
        for access in ("desktop", "mobile-web"):
            rest = rest_hourly(wiki, access, "user", f"{day:%Y%m%d}00", f"{day:%Y%m%d}23")
            if rest is None:
                print(f"  {wiki:<16} {access:<10} REST unavailable")
                continue
            dump = hourly[(wiki, access)]
            same = sum(dump[h] == rest.get(h, 0) for h in range(24))
            shifted = sum(dump[h] == rest.get(h - 1, 0) for h in range(1, 24))
            ok(f"{wiki:<16} {access:<10} dump {sum(dump):,} REST {sum(rest.values()):,} | "
               f"letter==REST hour {same}/24, letter==REST hour-1 {shifted}/23")
    print("\n       24/24 on the same index means letter A is the hour STARTING")
    print("       00:00, so the pageviews minus-one-hour rule must NOT be")
    print("       carried across to this dataset.")


# ---------------------------------------------------------------------------
# E. automated traffic misclassified as user
# ---------------------------------------------------------------------------

def check_bot_split():
    section("E. AUTOMATED TRAFFIC MISCLASSIFIED AS user (REST daily split)")
    print("  Every title here survives the SPEC namespace exclusions.\n")
    for title in (".xyz", "XXX_(2002_film)", "Windows_10_version_history", "Charlie_Kirk"):
        parts = []
        for agent in ("all-agents", "user", "automated", "spider"):
            views = rest_article_daily(title, agent, "20260910")
            parts.append(f"{agent}={views:,}" if views is not None else f"{agent}=n/a")
        print(f"  {title:<28} " + "  ".join(parts))
    print("\n       A large automated share is a warning about the title, but the")
    print("       user-classified part of that traffic is inside our data and")
    print("       cannot be separated out. Documented limitation, not a fix.")


# ---------------------------------------------------------------------------
# F. derived estimates
# ---------------------------------------------------------------------------

def report_estimates(volumes=None):
    section("F. ESTIMATES (derived, not measured)")
    throughput = MEASURED_BYTES / MEASURED_SECONDS
    ok(f"measured throughput on one connection: {human(throughput)}/s "
       f"({human(MEASURED_BYTES)} in {MEASURED_SECONDS}s)")
    for name, total in (volumes or MEASURED_VOLUMES).items():
        seconds = total / throughput + FILE_COUNTS[name] * POLITE_DELAY_SEC
        gib = total / 2 ** 30
        monthly = gib * S3_STANDARD_USD_PER_GB_MONTH
        print(f"\n  {name}")
        print(f"    1 connection : {seconds / 3600:6.1f} h")
        print(f"    3 connections: {seconds / 3 / 3600:6.1f} h  "
              f"(assumes throughput scales linearly, untested)")
        print(f"    S3 Standard  : ${monthly:,.2f}/month for {gib:,.1f} GiB, "
              f"{BUDGET_USD / monthly:.1f} months to spend the ${BUDGET_USD:,.0f} budget")
    print()
    warn("throughput came from ONE transfer on a home connection. It excludes "
         "the upload leg into S3 and says nothing about cloud throughput.")


SECTIONS = {
    "A": check_hour_convention,
    "B": check_volume_and_gaps,
    "C": check_complete_format,
    "D": check_hour_letters,
    "E": check_bot_split,
    "F": report_estimates,
}


def main():
    ap = argparse.ArgumentParser(description="Task 0 cross-checks, see module docstring")
    ap.add_argument("--only", default="",
                    help="comma-separated section letters, e.g. A,F (default: all)")
    args = ap.parse_args()

    if "REPLACE_WITH_YOUR_EMAIL" in USER_AGENT:
        print("Edit USER_AGENT in recon.py first. Wikimedia policy requires a")
        print("real contact address and blocks clients that ignore it.")
        return 2

    wanted = [s.strip().upper() for s in args.only.split(",") if s.strip()] or list(SECTIONS)
    unknown = [s for s in wanted if s not in SECTIONS]
    if unknown:
        print(f"unknown section(s) {unknown}; known sections are {list(SECTIONS)}")
        return 2

    print(f"recon_verify started {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")
    print(f"user-agent: {USER_AGENT}")
    volumes = None
    for letter in wanted:
        if letter == "B":
            volumes = check_volume_and_gaps()
        elif letter == "F":
            report_estimates(volumes)
        else:
            SECTIONS[letter]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
