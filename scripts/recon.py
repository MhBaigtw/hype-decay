#!/usr/bin/env python3
"""
Phase 0 recon for the hype-decay project.

Verifies the two data sources before any AWS resource exists:
  1. Wikimedia hourly pageviews  (bulk file ingestion)
  2. GDELT 2.0 mentions feed     (15-minute micro-batch)

Stdlib only. No pip install. No AWS. Run it locally.

    python3 recon.py --selftest      # parser checks, no network
    python3 recon.py                 # live recon against both sources

Before the live run, edit USER_AGENT below. Wikimedia enforces a
User-Agent policy on dumps downloads and blocks clients that ignore it.
"""

import argparse
import csv
import gzip
import io
import sys
import time
import urllib.error
import urllib.request
import zipfile
from collections import Counter
from datetime import datetime, timedelta, timezone

# ---------------------------------------------------------------------------
# CONFIG  -- edit this line before running live
# ---------------------------------------------------------------------------

USER_AGENT = "hype-decay-recon/0.1 (MhBaig971@gmail.com)"

WIKI_BASE = "https://dumps.wikimedia.org/other"
GDELT_BASE = "http://data.gdeltproject.org/gdeltv2"

# Wikimedia caps you at 3 connections per IP and rate limits downloads.
# Everything here is deliberately sequential. Do not parallelise this script.
POLITE_DELAY_SEC = 1.0

# GDELT mentions table column order (v2 codebook, tab-delimited, no header).
GDELT_MENTIONS_COLS = [
    "GlobalEventID", "EventTimeDate", "MentionTimeDate", "MentionType",
    "MentionSourceName", "MentionIdentifier", "SentenceID",
    "Actor1CharOffset", "Actor2CharOffset", "ActionCharOffset",
    "InRawText", "Confidence", "MentionDocLen", "MentionDocTone",
    "MentionDocTranslationInfo", "Extras",
]


# ---------------------------------------------------------------------------
# tiny output helpers
# ---------------------------------------------------------------------------

FINDINGS = []


def section(title):
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def ok(msg):
    print(f"  [ok]    {msg}")


def warn(msg):
    print(f"  [WARN]  {msg}")
    FINDINGS.append(("WARN", msg))


def fail(msg):
    print(f"  [FAIL]  {msg}")
    FINDINGS.append(("FAIL", msg))


def human(n):
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PB"


# ---------------------------------------------------------------------------
# network
# ---------------------------------------------------------------------------

def request(url, method="GET"):
    """Sequential, polite, identified. Returns (status, headers, body_bytes)."""
    req = urllib.request.Request(url, method=method)
    req.add_header("User-Agent", USER_AGENT)
    started = time.time()
    try:
        with urllib.request.urlopen(req, timeout=120) as resp:
            body = resp.read() if method == "GET" else b""
            elapsed = time.time() - started
            time.sleep(POLITE_DELAY_SEC)
            return resp.status, dict(resp.headers), body, elapsed
    except urllib.error.HTTPError as e:
        time.sleep(POLITE_DELAY_SEC)
        return e.code, dict(e.headers or {}), b"", time.time() - started
    except Exception as e:  # noqa: BLE001 - recon script, report and continue
        time.sleep(POLITE_DELAY_SEC)
        return None, {"error": str(e)}, b"", time.time() - started


# ---------------------------------------------------------------------------
# parsers (pure functions -- these are what --selftest exercises)
# ---------------------------------------------------------------------------

def parse_pageviews(raw_text):
    """
    Hourly pageviews format: 4 space-separated fields
        domain_code page_title count_views total_response_size

    Returns a dict of recon stats. Deliberately tolerant: we want to COUNT
    malformed lines, not crash on them.
    """
    stats = {
        "rows": 0,
        "malformed": 0,
        "field_counts": Counter(),
        "domains": Counter(),
        "en_desktop_views": 0,
        "en_mobile_views": 0,
        "top_en": Counter(),
        "bad_ints": 0,
    }

    for line in raw_text.splitlines():
        if not line.strip():
            continue
        parts = line.split(" ")
        stats["rows"] += 1
        stats["field_counts"][len(parts)] += 1

        if len(parts) < 3:
            stats["malformed"] += 1
            continue

        domain, title, views = parts[0], parts[1], parts[2]
        try:
            views = int(views)
        except ValueError:
            stats["bad_ints"] += 1
            stats["malformed"] += 1
            continue

        stats["domains"][domain] += 1

        # The desktop/mobile split is the trap: en.wikipedia and en.m.wikipedia
        # are separate domain codes. Sum them or you undercount by roughly half.
        if domain == "en":
            stats["en_desktop_views"] += views
            stats["top_en"][title] += views
        elif domain == "en.m":
            stats["en_mobile_views"] += views
            stats["top_en"][title] += views

    return stats


def parse_gdelt_mentions(raw_text):
    """
    GDELT mentions: tab-delimited, no header, 16 columns.
    EventTimeDate and MentionTimeDate are YYYYMMDDHHMMSS.

    The gap between those two fields IS news attention decay, per mention.
    """
    stats = {
        "rows": 0,
        "malformed": 0,
        "field_counts": Counter(),
        "sources": Counter(),
        "lag_minutes": [],
        "distinct_events": set(),
    }

    reader = csv.reader(io.StringIO(raw_text), delimiter="\t", quoting=csv.QUOTE_NONE)
    for parts in reader:
        if not parts or (len(parts) == 1 and not parts[0].strip()):
            continue
        stats["rows"] += 1
        stats["field_counts"][len(parts)] += 1

        if len(parts) < 6:
            stats["malformed"] += 1
            continue

        row = dict(zip(GDELT_MENTIONS_COLS, parts))
        stats["distinct_events"].add(row["GlobalEventID"])
        stats["sources"][row["MentionSourceName"]] += 1

        try:
            ev = datetime.strptime(row["EventTimeDate"], "%Y%m%d%H%M%S")
            mn = datetime.strptime(row["MentionTimeDate"], "%Y%m%d%H%M%S")
            stats["lag_minutes"].append((mn - ev).total_seconds() / 60.0)
        except (ValueError, KeyError):
            stats["bad_dates"] = stats.get("bad_dates", 0) + 1

    return stats


def parse_gdelt_lastupdate(raw_text):
    """
    lastupdate.txt is 3 lines of: <size> <md5> <url>
    for the export, mentions and gkg files of the most recent 15-min slice.
    """
    out = {}
    for line in raw_text.splitlines():
        parts = line.split()
        if len(parts) != 3:
            continue
        size, md5, url = parts
        if ".mentions." in url:
            out["mentions"] = (int(size), md5, url)
        elif ".export." in url:
            out["export"] = (int(size), md5, url)
        elif ".gkg." in url:
            out["gkg"] = (int(size), md5, url)
    return out


# ---------------------------------------------------------------------------
# live checks
# ---------------------------------------------------------------------------

def pick_sample_hour():
    """Three days back, 18:00 UTC. Recent enough to be representative,
    old enough that the file is definitely published."""
    t = datetime.now(timezone.utc) - timedelta(days=3)
    return t.replace(hour=18, minute=0, second=0, microsecond=0)


def wiki_pageviews_url(dt):
    return (
        f"{WIKI_BASE}/pageviews/{dt:%Y}/{dt:%Y-%m}/"
        f"pageviews-{dt:%Y%m%d}-{dt:%H}0000.gz"
    )


def wiki_complete_url(dt):
    return (
        f"{WIKI_BASE}/pageview_complete/{dt:%Y}/{dt:%Y-%m}/"
        f"pageviews-{dt:%Y%m%d}-user.bz2"
    )


def check_wikimedia():
    section("1. WIKIMEDIA HOURLY PAGEVIEWS")

    dt = pick_sample_hour()
    url = wiki_pageviews_url(dt)
    print(f"  sample hour : {dt:%Y-%m-%d %H}:00 UTC")
    print(f"  url         : {url}")
    print("  NOTE: the filename timestamp is the END of the capture window.")
    print("        An 18:00 file covers 17:00-18:00. Off-by-one here corrupts")
    print("        every half-life you compute.\n")

    status, headers, body, elapsed = request(url)
    if status != 200:
        fail(f"download failed (status={status}, {headers.get('error', '')})")
        return None

    ok(f"downloaded {human(len(body))} in {elapsed:.1f}s "
       f"({human(len(body) / max(elapsed, 0.01))}/s)")

    try:
        raw = gzip.decompress(body).decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        fail(f"gzip decompress failed: {e}")
        return None

    ok(f"decompressed to {human(len(raw.encode()))} "
       f"({len(raw.encode()) / len(body):.1f}x)")

    stats = parse_pageviews(raw)
    print()
    ok(f"{stats['rows']:,} rows, {stats['malformed']:,} malformed")
    ok(f"field-count distribution: {dict(stats['field_counts'])}")
    ok(f"{len(stats['domains']):,} distinct domain codes")

    en_total = stats["en_desktop_views"] + stats["en_mobile_views"]
    if en_total:
        mobile_pct = 100 * stats["en_mobile_views"] / en_total
        ok(f"en desktop {stats['en_desktop_views']:,} views, "
           f"en.m mobile {stats['en_mobile_views']:,} views "
           f"({mobile_pct:.0f}% mobile)")
        if mobile_pct > 20:
            warn("mobile is a large share -- if you only read domain 'en' you "
                 "will undercount by roughly this much. Union en + en.m.")

    print("\n  top 10 English pages this hour (desktop + mobile):")
    for title, views in stats["top_en"].most_common(10):
        print(f"    {views:>9,}  {title[:60]}")

    # storage extrapolation
    per_hour = len(body)
    year_raw = per_hour * 24 * 365
    print()
    ok(f"extrapolated raw gz: {human(year_raw)}/year, "
       f"{human(year_raw * 2)} for a 2-year backfill")
    print("       Parquet with column pruning should land well under that,")
    print("       but measure it rather than trusting the estimate.")

    # rate limit reality check
    print()
    print("  backfill feasibility:")
    hours_2y = 24 * 730
    est_sec = hours_2y * (elapsed + POLITE_DELAY_SEC) / 3
    print(f"    {hours_2y:,} files for 2 years")
    print(f"    at 3 concurrent connections, roughly {est_sec / 3600:.1f} hours")
    warn("Wikimedia caps at 3 connections per IP and blocks evasion. "
         "Lambda fan-out over these URLs will get you banned. Transfer to "
         "S3 slowly and sequentially, then parallelise FROM S3.")

    # pageview_complete
    print()
    curl = wiki_complete_url(dt)
    print(f"  pageview_complete (daily grain, has page_id): {curl}")
    cstatus, cheaders, _, _ = request(curl, method="HEAD")
    if cstatus == 200:
        size = int(cheaders.get("Content-Length", 0))
        ok(f"exists, {human(size)} bz2 for the full day, all projects")
        print("       This is NOT the bot-filtering upgrade an earlier version")
        print("       of this script claimed. Task 0 measured the hourly files")
        print("       above against the Wikimedia REST API: they already match")
        print("       agent 'user' exactly, so there is no bot traffic to")
        print("       filter out. Here, filtering means choosing the -user file;")
        print("       there is no agent_type column. Rows are 6 fields and a")
        print("       missing page ID is the literal 'null' -- the 5-column")
        print("       quirk appeared in 0 of 384,090 sampled rows. What this")
        print("       dataset does add is page_id and a daily grain. See")
        print("       NOTES.md (Task 0) and scripts/recon_verify.py.")
    else:
        warn(f"pageview_complete HEAD returned {cstatus} -- check the path/date")

    return stats


def check_gdelt():
    section("2. GDELT 2.0 MENTIONS FEED")

    url = f"{GDELT_BASE}/lastupdate.txt"
    print(f"  url: {url}")
    status, headers, body, _ = request(url)
    if status != 200:
        fail(f"lastupdate.txt failed (status={status}, {headers.get('error','')})")
        return None

    files = parse_gdelt_lastupdate(body.decode("utf-8", errors="replace"))
    if not files:
        fail("could not parse lastupdate.txt")
        return None

    for kind, (size, _md5, furl) in files.items():
        ok(f"{kind:<9} {human(size):>10}  {furl.rsplit('/', 1)[-1]}")

    if "mentions" not in files:
        fail("no mentions file in lastupdate.txt")
        return None

    size, _md5, murl = files["mentions"]
    print(f"\n  downloading mentions slice ({human(size)})...")
    status, headers, body, elapsed = request(murl)
    if status != 200:
        fail(f"mentions download failed (status={status})")
        return None
    ok(f"downloaded in {elapsed:.1f}s")

    try:
        with zipfile.ZipFile(io.BytesIO(body)) as zf:
            name = zf.namelist()[0]
            raw = zf.read(name).decode("utf-8", errors="replace")
    except Exception as e:  # noqa: BLE001
        fail(f"unzip failed: {e}")
        return None

    ok(f"unzipped {name} -> {human(len(raw.encode()))}")

    stats = parse_gdelt_mentions(raw)
    print()
    ok(f"{stats['rows']:,} mention rows, {stats['malformed']:,} malformed")
    ok(f"field-count distribution: {dict(stats['field_counts'])}")
    ok(f"{len(stats['distinct_events']):,} distinct events in this 15-min slice")

    if stats["lag_minutes"]:
        lags = sorted(stats["lag_minutes"])
        mid = lags[len(lags) // 2]
        ok(f"mention lag after event: median {mid:,.0f} min, "
           f"min {lags[0]:,.0f}, max {lags[-1]:,.0f}")
        print("       This lag distribution IS news decay. It is the single")
        print("       most useful field pair in the dataset for this project.")

    print("\n  top 10 sources in this slice:")
    for src, n in stats["sources"].most_common(10):
        print(f"    {n:>6,}  {src[:60]}")

    slices_per_day = 96
    print()
    ok(f"~{slices_per_day} slices/day, extrapolated "
       f"{human(size * slices_per_day * 365)}/year of zipped mentions")

    return stats


# ---------------------------------------------------------------------------
# selftest -- proves the parsers work without touching the network
# ---------------------------------------------------------------------------

def selftest():
    section("SELFTEST (no network)")

    pv = "\n".join([
        "en Barack_Obama 1500 0",
        "en.m Barack_Obama 2200 0",
        "en Toronto 300 0",
        "de Berlin 900 0",
        "en.m Toronto 450 0",
        "en Bad_Row notanumber 0",
        "en",
        "",
    ])
    s = parse_pageviews(pv)
    assert s["rows"] == 7, s["rows"]
    assert s["malformed"] == 2, s["malformed"]
    assert s["en_desktop_views"] == 1800, s["en_desktop_views"]
    assert s["en_mobile_views"] == 2650, s["en_mobile_views"]
    assert s["top_en"]["Barack_Obama"] == 3700, s["top_en"]["Barack_Obama"]
    assert s["bad_ints"] == 1
    ok("parse_pageviews: desktop/mobile union, malformed handling, bad ints")

    rows = [
        ["1", "20260910120000", "20260910130000", "1", "bbc.co.uk",
         "http://x", "3", "10", "20", "30", "1", "40", "1200", "-2.5", "", ""],
        ["1", "20260910120000", "20260911000000", "1", "cbc.ca",
         "http://y", "1", "10", "20", "30", "1", "40", "900", "1.0", "", ""],
        ["2", "20260910120000", "BADDATE", "1", "bbc.co.uk",
         "http://z", "1", "10", "20", "30", "1", "40", "900", "1.0", "", ""],
        ["3", "short"],
    ]
    gd = "\n".join("\t".join(r) for r in rows)
    g = parse_gdelt_mentions(gd)
    assert g["rows"] == 4, g["rows"]
    assert g["malformed"] == 1, g["malformed"]
    # row 4 is rejected as malformed before its id is counted, so 2 not 3
    assert len(g["distinct_events"]) == 2, g["distinct_events"]
    assert g["sources"]["bbc.co.uk"] == 2
    assert len(g["lag_minutes"]) == 2
    assert g["lag_minutes"][0] == 60.0, g["lag_minutes"]
    assert g["lag_minutes"][1] == 720.0, g["lag_minutes"]
    ok("parse_gdelt_mentions: lag computation, bad dates, short rows")

    lu = "\n".join([
        "227874 abc123 http://data.gdeltproject.org/gdeltv2/2026.export.CSV.zip",
        "511234 def456 http://data.gdeltproject.org/gdeltv2/2026.mentions.CSV.zip",
        "9912345 ghi789 http://data.gdeltproject.org/gdeltv2/2026.gkg.csv.zip",
    ])
    f = parse_gdelt_lastupdate(lu)
    assert set(f) == {"export", "mentions", "gkg"}, f
    assert f["mentions"][0] == 511234
    ok("parse_gdelt_lastupdate: all three file kinds")

    print("\n  all parser checks passed")


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--selftest", action="store_true",
                    help="run parser checks only, no network")
    ap.add_argument("--skip-wiki", action="store_true")
    ap.add_argument("--skip-gdelt", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return 0

    if "REPLACE_WITH_YOUR_EMAIL" in USER_AGENT:
        print("Edit USER_AGENT at the top of this file first.")
        print("Wikimedia enforces a User-Agent policy on dumps downloads and")
        print("blocks clients that ignore it. Put a real contact address in it.")
        return 2

    print(f"recon started {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC")
    print(f"user-agent: {USER_AGENT}")

    if not args.skip_wiki:
        check_wikimedia()
    if not args.skip_gdelt:
        check_gdelt()

    section("SUMMARY")
    if not FINDINGS:
        print("  no warnings. both sources look usable.")
    else:
        for level, msg in FINDINGS:
            print(f"  [{level}] {msg}")

    fails = sum(1 for lvl, _ in FINDINGS if lvl == "FAIL")
    print(f"\n  {fails} blocking failure(s)")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
