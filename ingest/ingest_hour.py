#!/usr/bin/env python3
"""
One hour of English Wikipedia pageviews, source URL to curated Parquet.

Pipeline for a single hour:

  1. claim the hour in the DynamoDB manifest (lease, so two workers cannot
     process the same hour)
  2. stream the source gz down on ONE connection, hashing as it goes
  3. parse it with pyarrow, union en + en.m, apply the SPEC exclusions
  4. write both curated tiers as Parquet: page_daily, and page_hour where
     hourly views are 10 or more
  5. keep the source gz as part of the 48-hour regression fixture
  6. mark the manifest row done, with content-length, sha256, row counts and
     both URLs -- the canonical one and the one actually fetched

There is no raw zone. The gz in fixtures/raw_48h/ expires on a lifecycle rule;
everything else is re-fetchable from the URLs in the manifest.

THE CRITICAL DETAIL (SPEC, and CLAUDE.md calls it out too): the timestamp in the
source filename is the END of the capture window, so

    hour_start = filename_hour - 1 hour

pageviews-20260910-180000.gz covers 17:00-18:00 UTC and its rows carry
hour_start = 17:00. Task 0 confirmed this against the Wikimedia REST API to the
exact view. Getting it wrong shifts every half-life by an hour, and no test that
was not written knowing the trap will catch it.

PARSING is deliberately hostile to bad data rather than fragile. Measured on the
fixture hour, pyarrow parses in 4.3 s where the old pure-Python loop took 21.9 s,
but a vectorised reader fails the whole file on one malformed row unless told not
to. So: invalid rows are skipped and counted, non-numeric view counts are
dropped and counted, and titles that are not valid UTF-8 are skipped and counted
rather than raising. Every skip lands in the manifest, because a silent skip is
indistinguishable from data that was never there.

Rate limits: Wikimedia allows 3 connections per IP and blocks clients that evade
it. This ingester uses exactly ONE connection. Mirrors publish no number, so the
same courtesy applies.

    python3 ingest_hour.py --source-hour 2026-09-10T18
    python3 ingest_hour.py --source-hour 2026-09-10T18 --source origin --dry-run
"""

import argparse
import datetime as dt
import hashlib
import os
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# --- policy -----------------------------------------------------------------

USER_AGENT = "hype-decay-ingest/0.1 (MhBaig971@gmail.com)"

# The canonical source is Wikimedia. The mirrors carry byte-identical copies of
# the same tree (verified: sha256 of 2026-09-10T18 matches on both), and using
# one keeps ~939 GiB of backfill traffic off Wikimedia infrastructure. The
# canonical URL is recorded in the manifest whichever one is fetched, so an hour
# can always be re-fetched from the authority.
CANONICAL_BASE = "https://dumps.wikimedia.org/other/pageviews"
SOURCES = {
    "origin": CANONICAL_BASE,
    "your.org": "https://dumps.wikimedia.your.org/other/pageviews",
    "umu": "https://ftp.acc.umu.se/mirror/wikimedia.org/other/pageviews",
}

MAX_CONNECTIONS = 3
POLITE_DELAY_SEC = 1.0

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
DEFAULT_TABLE = "hype-decay-manifest"
DEFAULT_REGION = "us-east-1"

# The two domain codes that make up English Wikipedia. Reading only "en"
# undercounts by about 60% (measured in Task 0) because en.m is mobile. Compared
# as bytes, because titles are parsed as binary (see parse()).
EN_DOMAINS = (b"en", b"en.m")

# SPEC exclusions, as bytes for the same reason.
NAMESPACE_PREFIXES = (
    b"Special:", b"Talk:", b"File:", b"Category:", b"Template:",
    b"Help:", b"Portal:", b"Wikipedia:", b"User:",
)
EXCLUDED_TITLES = (b"Main_Page", b"-")

# page_hour is written only at or above this many views (SPEC, two-tier grain).
PAGE_HOUR_MIN_VIEWS = 10

# How long a claim is held before another worker may take the hour. The worker
# extends it while downloading, so an expired lease means the worker died.
LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 15

# Regression fixture. These numbers were verified against the Wikimedia REST API
# in Task 0 -- 4,024,554 desktop + 5,742,163 mobile-web + 224,463 mobile-app --
# so they are an external check on the en/en.m union AND on the hour arithmetic,
# not just a snapshot of our own output. See test_parser_regression.py.
REGRESSION_HOUR = "2026-09-10T18"
REGRESSION_EXPECTED = {
    "views_before_exclusions": 9_991_180,
    "views_kept": 9_431_954,
    "distinct_titles": 1_748_350,
    "rows_page_hour": 163_085,
}


def log(msg):
    print(f"  {msg}", flush=True)


def human(n):
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TB"


# --- hour arithmetic --------------------------------------------------------

def parse_source_hour(text):
    """YYYY-MM-DDTHH, the hour in the source FILENAME (end of the window)."""
    return dt.datetime.strptime(text, "%Y-%m-%dT%H").replace(tzinfo=dt.timezone.utc)


def hour_start_of(source_hour):
    """SPEC: the filename carries the END of the window, so subtract an hour."""
    return source_hour - dt.timedelta(hours=1)


def build_url(base, source_hour):
    return (f"{base}/{source_hour:%Y}/{source_hour:%Y-%m}/"
            f"pageviews-{source_hour:%Y%m%d}-{source_hour:%H}0000.gz")


# --- manifest --------------------------------------------------------------

class Manifest:
    """One row per source hour: what was fetched, from where, and what it held."""

    def __init__(self, table, worker_id):
        self.table = table
        self.worker_id = worker_id

    def get(self, key):
        return self.table.get_item(Key={"source_hour": key}).get("Item")

    def claim(self, key, canonical_url, hour_start):
        now = int(time.time())
        try:
            self.table.update_item(
                Key={"source_hour": key},
                UpdateExpression=(
                    "SET #s = :inflight, worker_id = :w, lease_expires = :lease, "
                    "started_at = :now, source_url_canonical = :url, hour_start = :hs "
                    "ADD attempt :one"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#s) OR #s IN (:pending, :failed) "
                    "OR (#s = :inflight AND lease_expires < :now_n)"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":inflight": "in-flight", ":pending": "pending", ":failed": "failed",
                    ":w": self.worker_id, ":lease": now + LEASE_SECONDS,
                    ":now": dt.datetime.now(dt.timezone.utc).isoformat(),
                    ":now_n": now, ":url": canonical_url,
                    ":hs": hour_start.isoformat(), ":one": 1,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def heartbeat(self, key):
        self.table.update_item(
            Key={"source_hour": key},
            UpdateExpression="SET lease_expires = :lease",
            ExpressionAttributeValues={":lease": int(time.time()) + LEASE_SECONDS},
        )

    def finish(self, key, fields):
        expression = "SET #s = :done, completed_at = :now, " + ", ".join(
            f"{name} = :{name}" for name in fields)
        values = {f":{name}": value for name, value in fields.items()}
        values[":done"] = "done"
        values[":now"] = dt.datetime.now(dt.timezone.utc).isoformat()
        self.table.update_item(
            Key={"source_hour": key},
            UpdateExpression=expression,
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues=values,
        )

    def fail(self, key, reason):
        self.table.update_item(
            Key={"source_hour": key},
            UpdateExpression="SET #s = :failed, error = :reason",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "failed", ":reason": str(reason)[:900]},
        )


# --- download --------------------------------------------------------------

def download(url, dest, manifest=None, key=None):
    """Streams the gz to dest on ONE connection, hashing as it goes."""
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    digest = hashlib.sha256()
    written = 0
    last_beat = time.time()

    with urllib.request.urlopen(request, timeout=180) as response:
        declared = int(response.headers.get("Content-Length") or 0)
        with open(dest, "wb") as out:
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
                written += len(chunk)
                # Keep the claim alive. If this process is killed, the lease
                # stops being extended and the hour frees itself.
                if manifest and key and time.time() - last_beat > HEARTBEAT_SECONDS:
                    manifest.heartbeat(key)
                    last_beat = time.time()

    time.sleep(POLITE_DELAY_SEC)
    return written, digest.hexdigest(), declared


def origin_content_length(source_hour):
    """HEAD the canonical URL, so a mirror's bytes can be checked against it."""
    url = build_url(CANONICAL_BASE, source_hour)
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT},
                                     method="HEAD")
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            length = int(response.headers.get("Content-Length") or 0)
        time.sleep(POLITE_DELAY_SEC)
        return length
    except (urllib.error.HTTPError, urllib.error.URLError, OSError):
        return 0


# --- parse -----------------------------------------------------------------

def parse(gz_path):
    """Sums en + en.m per title, applying the SPEC exclusions.

    Returns (views_by_title, stats). Titles are read as BINARY, not string:
    pyarrow validates UTF-8 when producing a string column and raises on the
    whole file if one title is malformed. Reading bytes and decoding after
    aggregation means one bad title costs one title, not one hour.
    """
    stats = Counter()

    def on_invalid_row(row):
        # Wrong column count. Count it and carry on: a vectorised reader would
        # otherwise abandon the entire hour over a single truncated line.
        stats["rows_skipped_invalid"] += 1
        return "skip"

    table = pacsv.read_csv(
        gz_path,
        read_options=pacsv.ReadOptions(
            column_names=["domain", "title", "views", "bytes"]),
        parse_options=pacsv.ParseOptions(
            delimiter=" ", quote_char=False, invalid_row_handler=on_invalid_row),
        convert_options=pacsv.ConvertOptions(column_types={
            "domain": pa.binary(),
            "title": pa.binary(),
            # Read as bytes and validate explicitly. Letting pyarrow cast to
            # int64 makes one non-numeric count fatal for the file.
            "views": pa.binary(),
            "bytes": pa.binary(),
        }),
    )
    stats["lines"] = table.num_rows + stats["rows_skipped_invalid"]

    english = table.filter(pc.is_in(table.column("domain"),
                                   value_set=pa.array(EN_DOMAINS, pa.binary())))
    stats["rows_other_project"] = table.num_rows - english.num_rows

    # Non-numeric view counts: counted, dropped, never guessed at.
    numeric = pc.match_substring_regex(
        pc.cast(english.column("views"), pa.string()), r"^[0-9]+$")
    stats["rows_skipped_bad_int"] = english.num_rows - pc.sum(
        pc.cast(numeric, pa.int64())).as_py()
    english = english.filter(numeric)

    titles = english.column("title")
    views = pc.cast(pc.cast(english.column("views"), pa.string()), pa.int64())
    stats["views_before_exclusions"] = pc.sum(views).as_py() or 0

    # Per-domain split, for the reconciliation line.
    for domain in EN_DOMAINS:
        mask = pc.equal(english.column("domain"), pa.scalar(domain, pa.binary()))
        label = domain.decode().replace(".", "_")
        stats[f"views_{label}"] = pc.sum(pc.filter(views, mask)).as_py() or 0

    # SPEC exclusions, applied on bytes before anything is decoded.
    keep = pc.invert(pc.is_in(titles, value_set=pa.array(EXCLUDED_TITLES, pa.binary())))
    for prefix in NAMESPACE_PREFIXES:
        keep = pc.and_(keep, pc.invert(pc.starts_with(titles, pattern=prefix)))
    dropped_views = pc.sum(pc.filter(views, pc.invert(keep))).as_py() or 0
    stats["views_dropped_exclusions"] = dropped_views
    stats["rows_dropped_exclusions"] = english.num_rows - pc.sum(
        pc.cast(keep, pa.int64())).as_py()

    kept = pa.table({"title": pc.filter(titles, keep),
                     "views": pc.filter(views, keep)})
    grouped = kept.group_by("title").aggregate([("views", "sum")])

    # Decode only the aggregated keys, skipping any that are not valid UTF-8.
    views_by_title = {}
    for title_bytes, total in zip(grouped.column("title").to_pylist(),
                                  grouped.column("views_sum").to_pylist()):
        try:
            views_by_title[title_bytes.decode("utf-8")] = total
        except UnicodeDecodeError:
            stats["rows_skipped_bad_utf8"] += 1
            stats["views_skipped_bad_utf8"] += total

    stats["rows_parsed"] = kept.num_rows
    stats["views_kept"] = sum(views_by_title.values())
    return views_by_title, stats


# --- curated output --------------------------------------------------------

PAGE_HOUR_SCHEMA = pa.schema([
    ("project", pa.string()),
    ("page_title", pa.string()),
    ("hour_start", pa.timestamp("us", tz="UTC")),
    ("views", pa.int64()),
])

PAGE_DAILY_SCHEMA = pa.schema([
    ("project", pa.string()),
    ("page_title", pa.string()),
    ("dt", pa.date32()),
    ("views", pa.int64()),
    # How many of the day's 24 hours this row is built from. One hour ingested
    # means 1: these rows are CONTRIBUTIONS, not finished daily totals, and a
    # day is only complete at 24. compact_day.py sums them. A missing hour must
    # never read as zero.
    ("hours_present", pa.int32()),
])


def write_parquet(rows, schema, path):
    pq.write_table(pa.Table.from_pydict(rows, schema=schema), path,
                   compression="snappy")
    return path.stat().st_size


def build_tiers(views_by_title, hour_start):
    titles = list(views_by_title)
    counts = [views_by_title[t] for t in titles]
    day = hour_start.date()

    page_daily = {
        "project": ["en.wikipedia"] * len(titles),
        "page_title": titles,
        "dt": [day] * len(titles),
        "views": counts,
        "hours_present": [1] * len(titles),
    }

    hot = [(t, c) for t, c in zip(titles, counts) if c >= PAGE_HOUR_MIN_VIEWS]
    page_hour = {
        "project": ["en.wikipedia"] * len(hot),
        "page_title": [t for t, _ in hot],
        "hour_start": [hour_start] * len(hot),
        "views": [c for _, c in hot],
    }
    return page_hour, page_daily


# --- main ------------------------------------------------------------------

def run(args):
    source_hour = parse_source_hour(args.source_hour)
    hour_start = hour_start_of(source_hour)
    key = f"{source_hour:%Y-%m-%dT%H}"
    canonical_url = build_url(CANONICAL_BASE, source_hour)
    fetch_url = build_url(SOURCES[args.source], source_hour)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"

    print("=" * 70)
    print(f"INGEST {key}  (source filename hour)")
    print("=" * 70)
    log(f"canonical  : {canonical_url}")
    log(f"fetching   : {fetch_url}" + ("" if args.source == "origin" else f"  [mirror: {args.source}]"))
    log(f"hour_start : {hour_start:%Y-%m-%d %H}:00 UTC  (= filename hour minus 1, per SPEC)")
    log(f"worker     : {worker_id}")
    log(f"connections: 1 of {MAX_CONNECTIONS} allowed")

    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    manifest = Manifest(session.resource("dynamodb").Table(args.table), worker_id)
    s3 = session.client("s3")

    existing = manifest.get(key)
    if existing and existing.get("status") == "done" and not args.force:
        print()
        log("NO-OP: the manifest says this hour is already done.")
        log(f"  status         : {existing['status']}")
        log(f"  sha256         : {existing.get('sha256')}")
        log(f"  content_length : {int(existing.get('content_length', 0)):,}")
        log(f"  fetched from   : {existing.get('source_url_fetched')}")
        log(f"  completed_at   : {existing.get('completed_at')}")
        log("Nothing was downloaded and nothing was written. Pass --force to redo it.")
        return 0

    if args.dry_run:
        # A dry run must not touch the manifest. Reading and parsing an hour is
        # not the same as claiming it, and a dry run that left a row in-flight
        # would be lying about an hour that is actually done.
        log("dry run: the manifest will not be claimed or modified")
    else:
        if not manifest.claim(key, canonical_url, hour_start):
            held = manifest.get(key) or {}
            log(f"another worker holds this hour: {held.get('worker_id')}, "
                f"lease expires in {int(held.get('lease_expires', 0)) - int(time.time())}s")
            return 3
        log(f"claimed (attempt {int((manifest.get(key) or {}).get('attempt', 1))})")

    workdir = Path(tempfile.mkdtemp(prefix="hype-decay-"))
    gz_path = workdir / f"pageviews-{source_hour:%Y%m%d}-{source_hour:%H}0000.gz"

    try:
        print()
        log("downloading...")
        started = time.time()
        size, sha256, declared = download(
            fetch_url, gz_path,
            None if args.dry_run else manifest,
            None if args.dry_run else key)
        elapsed = time.time() - started
        log(f"{human(size)} in {elapsed:.1f}s ({human(size / max(elapsed, 0.01))}/s)")
        log(f"sha256 {sha256}")
        if declared and declared != size:
            raise RuntimeError(f"content-length {declared} != bytes received {size}")

        origin_length = 0
        if args.source != "origin" and args.verify_origin_length:
            origin_length = origin_content_length(source_hour)
            if origin_length and origin_length != size:
                raise RuntimeError(
                    f"mirror served {size} bytes but the origin declares "
                    f"{origin_length}: refusing to trust this copy")
            log(f"origin content-length agrees: {origin_length:,} bytes")

        print()
        log("parsing (pyarrow)...")
        parse_started = time.time()
        views_by_title, stats = parse(gz_path)
        log(f"parsed in {time.time() - parse_started:.1f}s")
        log(f"{stats['lines']:,} source lines, {stats['rows_parsed']:,} English rows kept")
        log(f"skipped: invalid rows {stats['rows_skipped_invalid']:,}, "
            f"bad ints {stats['rows_skipped_bad_int']:,}, "
            f"bad utf-8 titles {stats['rows_skipped_bad_utf8']:,}")
        log(f"dropped by exclusions: {stats['rows_dropped_exclusions']:,} rows, "
            f"{stats['views_dropped_exclusions']:,} views")
        # Pre-exclusion, because that is the figure the REST API can be held
        # against: en == desktop/user, en.m == mobile-web/user + mobile-app/user.
        log(f"views before exclusions: en {stats['views_en']:,} desktop + en.m "
            f"{stats['views_en_m']:,} mobile = {stats['views_before_exclusions']:,}")
        log(f"reconciliation: {stats['views_before_exclusions']:,} en+en.m views "
            f"before exclusions = {stats['views_kept']:,} kept + "
            f"{stats['views_dropped_exclusions']:,} excluded + "
            f"{stats['views_skipped_bad_utf8']:,} undecodable")
        log(f"{len(views_by_title):,} distinct titles after union and exclusions")

        if key == REGRESSION_HOUR:
            expected = REGRESSION_EXPECTED["views_before_exclusions"]
            got = stats["views_before_exclusions"]
            verdict = "MATCHES Task 0" if got == expected else "DOES NOT MATCH Task 0"
            log(f"regression hour: {got:,} vs expected {expected:,} -- {verdict}")

        page_hour, page_daily = build_tiers(views_by_title, hour_start)
        hour_rows = len(page_hour["page_title"])
        daily_rows = len(page_daily["page_title"])

        hour_path = workdir / "page_hour.parquet"
        daily_path = workdir / "page_daily.parquet"
        hour_bytes = write_parquet(page_hour, PAGE_HOUR_SCHEMA, hour_path)
        daily_bytes = write_parquet(page_daily, PAGE_DAILY_SCHEMA, daily_path)

        print()
        log(f"page_hour : {hour_rows:,} rows (views >= {PAGE_HOUR_MIN_VIEWS}), "
            f"{human(hour_bytes)} parquet")
        log(f"page_daily: {daily_rows:,} rows (every page), {human(daily_bytes)} parquet")
        log(f"compression: source gz {human(size)} -> both tiers "
            f"{human(hour_bytes + daily_bytes)} "
            f"({size / max(hour_bytes + daily_bytes, 1):.2f}x smaller)")

        # Object keys carry the source hour, so re-running an hour overwrites
        # its own output instead of adding a duplicate.
        hour_key = (f"curated/page_hour/dt={hour_start:%Y-%m-%d}/hour={hour_start:%H}/"
                    f"part-{key}.parquet")
        daily_key = f"curated/page_daily/dt={hour_start:%Y-%m-%d}/part-{key}.parquet"
        fixture_key = (f"fixtures/raw_48h/dt={hour_start:%Y-%m-%d}/"
                       f"hour={hour_start:%H}/{gz_path.name}")

        if args.dry_run:
            print()
            log("dry run: nothing uploaded, manifest untouched")
            for k in (hour_key, daily_key, fixture_key):
                log(f"  would write s3://{args.bucket}/{k}")
            return 0

        print()
        log("uploading...")
        for local, s3_key in ((hour_path, hour_key), (daily_path, daily_key),
                              (gz_path, fixture_key)):
            s3.upload_file(str(local), args.bucket, s3_key)
            log(f"s3://{args.bucket}/{s3_key}")

        manifest.finish(key, {
            "source_url_fetched": fetch_url,
            "source_mirror": args.source,
            "origin_content_length": origin_length,
            "content_length": size,
            "sha256": sha256,
            "rows_parsed": stats["rows_parsed"],
            "rows_skipped_invalid": stats["rows_skipped_invalid"],
            "rows_skipped_bad_int": stats["rows_skipped_bad_int"],
            "rows_skipped_bad_utf8": stats["rows_skipped_bad_utf8"],
            "rows_dropped_exclusions": stats["rows_dropped_exclusions"],
            "views_before_exclusions": stats["views_before_exclusions"],
            "views_kept": stats["views_kept"],
            "rows_page_hour": hour_rows,
            "rows_page_daily": daily_rows,
            "bytes_page_hour": hour_bytes,
            "bytes_page_daily": daily_bytes,
            "bytes_source_gz": size,
            "distinct_titles": len(views_by_title),
            "key_page_hour": hour_key,
            "key_page_daily": daily_key,
            "key_fixture": fixture_key,
        })
        print()
        log("manifest row marked done")
        return 0

    except Exception as e:  # noqa: BLE001 - record why, then surface it
        log(f"FAILED: {e}")
        if args.dry_run:
            log("dry run: manifest untouched")
        else:
            manifest.fail(key, e)
            log("manifest row marked failed; the hour can be re-claimed")
        raise

    finally:
        for leftover in workdir.glob("*"):
            leftover.unlink(missing_ok=True)
        workdir.rmdir()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--source-hour", required=True,
                    help="hour in the SOURCE FILENAME, YYYY-MM-DDTHH (end of window)")
    ap.add_argument("--source", choices=sorted(SOURCES), default="your.org",
                    help="where to fetch from; the canonical URL is recorded either way")
    ap.add_argument("--verify-origin-length", action="store_true", default=True,
                    help="HEAD the origin and refuse a mirror copy of a different size")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--force", action="store_true",
                    help="re-process an hour the manifest already calls done")
    ap.add_argument("--dry-run", action="store_true",
                    help="download and parse, but write nothing and touch no state")
    args = ap.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
