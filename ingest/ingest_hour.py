#!/usr/bin/env python3
"""
Task 2: one hour of English Wikipedia pageviews, source URL to curated Parquet.

Pipeline for a single hour:

  1. claim the hour in the DynamoDB manifest (lease, so two workers cannot
     process the same hour)
  2. stream the source gz down on ONE connection, hashing as it goes
  3. parse it, union en + en.m, apply the SPEC exclusions
  4. write both curated tiers as Parquet: page_daily, and page_hour where
     hourly views are 10 or more
  5. keep the source gz as part of the 48-hour regression fixture
  6. mark the manifest row done, with content-length, sha256 and row counts

There is no raw zone. The gz in fixtures/raw_48h/ expires on a lifecycle rule;
everything else is re-fetchable from Wikimedia using the manifest.

THE CRITICAL DETAIL (SPEC, and CLAUDE.md calls it out too): the timestamp in the
source filename is the END of the capture window, so

    hour_start = filename_hour - 1 hour

pageviews-20260910-180000.gz covers 17:00-18:00 UTC and its rows carry
hour_start = 17:00. Task 0 confirmed this against the Wikimedia REST API to the
exact view. Getting it wrong shifts every half-life by an hour and no test that
was not written knowing the trap will catch it.

Rate limits: Wikimedia allows 3 connections per IP and blocks clients that
evade it. This ingester uses exactly ONE connection and sleeps between requests.
Do not parallelise it over source URLs; parallelise downstream, from S3.

    python3 ingest_hour.py --source-hour 2026-09-10T18
    python3 ingest_hour.py --source-hour 2026-09-10T18 --dry-run
"""

import argparse
import datetime as dt
import gzip
import hashlib
import io
import os
import socket
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter, defaultdict
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# --- policy -----------------------------------------------------------------

USER_AGENT = "hype-decay-ingest/0.1 (MhBaig971@gmail.com)"
WIKI_BASE = "https://dumps.wikimedia.org/other/pageviews"

# Wikimedia caps at 3 per IP. One hour needs one connection; the cap is stated
# here so that whoever writes the Task 3 backfill sees the ceiling.
MAX_CONNECTIONS = 3
POLITE_DELAY_SEC = 1.0

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
DEFAULT_TABLE = "hype-decay-manifest"
DEFAULT_REGION = "us-east-1"

# The two domain codes that make up English Wikipedia. Reading only "en"
# undercounts by about 60% (measured in Task 0), because en.m is mobile.
EN_DOMAINS = ("en", "en.m")

# SPEC exclusions.
NAMESPACE_PREFIXES = (
    "Special:", "Talk:", "File:", "Category:", "Template:",
    "Help:", "Portal:", "Wikipedia:", "User:",
)
EXCLUDED_TITLES = ("Main_Page", "-")

# page_hour is written only at or above this many views (SPEC, two-tier grain).
PAGE_HOUR_MIN_VIEWS = 10

# How long a claim is held before another worker may take the hour. The worker
# extends it while downloading, so an expired lease means the worker died.
LEASE_SECONDS = 60
HEARTBEAT_SECONDS = 15


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


def source_url(source_hour):
    return (f"{WIKI_BASE}/{source_hour:%Y}/{source_hour:%Y-%m}/"
            f"pageviews-{source_hour:%Y%m%d}-{source_hour:%H}0000.gz")


# --- manifest --------------------------------------------------------------

class Manifest:
    """One row per source hour. The record of what was fetched, and what it held.

    With no raw zone, this is what makes an hour re-fetchable and
    byte-verifiable later.
    """

    def __init__(self, table, worker_id):
        self.table = table
        self.worker_id = worker_id

    def get(self, key):
        got = self.table.get_item(Key={"source_hour": key})
        return got.get("Item")

    def claim(self, key, url, hour_start):
        """Claims the hour, or returns False if someone else holds a live lease.

        Claimable when: the row does not exist, or it failed, or it is pending,
        or it is in-flight with an expired lease (the worker died).
        """
        now = int(time.time())
        try:
            self.table.update_item(
                Key={"source_hour": key},
                UpdateExpression=(
                    "SET #s = :inflight, worker_id = :w, lease_expires = :lease, "
                    "started_at = :now, source_url = :url, hour_start = :hs "
                    "ADD attempt :one"
                ),
                ConditionExpression=(
                    "attribute_not_exists(#s) OR #s IN (:pending, :failed) "
                    "OR (#s = :inflight AND lease_expires < :now_n)"
                ),
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":inflight": "in-flight",
                    ":pending": "pending",
                    ":failed": "failed",
                    ":w": self.worker_id,
                    ":lease": now + LEASE_SECONDS,
                    ":now": dt.datetime.now(dt.timezone.utc).isoformat(),
                    ":now_n": now,
                    ":url": url,
                    ":hs": hour_start.isoformat(),
                    ":one": 1,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise

    def heartbeat(self, key):
        """Extends the lease. A dead worker stops doing this, freeing the hour."""
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
    """Streams the gz to dest on ONE connection, hashing as it goes.

    Returns (bytes_written, sha256_hex, declared_content_length).
    """
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    digest = hashlib.sha256()
    written = 0
    last_beat = time.time()

    with urllib.request.urlopen(request, timeout=120) as response:
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


# --- parse -----------------------------------------------------------------

def excluded(title):
    if title in EXCLUDED_TITLES:
        return "excluded_title"
    if title.startswith(NAMESPACE_PREFIXES):
        return "namespace"
    return None


def parse(gz_path):
    """Sums en + en.m per title, applying the SPEC exclusions.

    Returns (views_by_title, stats). Lines that fail UTF-8 decoding are counted
    and dropped, per SPEC, rather than being silently mangled.
    """
    views = Counter()
    stats = Counter()

    with gzip.open(gz_path, "rb") as raw:
        for line_bytes in raw:
            stats["lines"] += 1
            try:
                line = line_bytes.decode("utf-8").rstrip("\n")
            except UnicodeDecodeError:
                stats["dropped_bad_utf8"] += 1
                continue

            parts = line.split(" ")
            if len(parts) != 4:
                stats["malformed"] += 1
                continue

            domain, title, count = parts[0], parts[1], parts[2]
            if domain not in EN_DOMAINS:
                stats["other_project"] += 1
                continue

            try:
                count = int(count)
            except ValueError:
                stats["bad_int"] += 1
                continue

            # Counted BEFORE exclusions so the hour can be reconciled against
            # the Wikimedia REST API. Task 0 measured 9,991,180 views for
            # en + en.m at hour_start 17:00 on 2026-09-10.
            stats["views_before_exclusions"] += count

            reason = excluded(title)
            if reason:
                stats[f"dropped_{reason}"] += 1
                stats["views_dropped_exclusions"] += count
                continue

            stats["rows_parsed"] += 1
            stats[f"views_{domain.replace('.', '_')}"] += count
            views[title] += count

    return views, stats


# --- curated output --------------------------------------------------------

def write_parquet(rows, schema, path):
    table = pa.Table.from_pydict(rows, schema=schema)
    pq.write_table(table, path, compression="snappy")
    return path.stat().st_size


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
    # day is only complete at 24. A missing hour must never read as zero.
    ("hours_present", pa.int32()),
])


def build_tiers(views, hour_start):
    page_hour = {"project": [], "page_title": [], "hour_start": [], "views": []}
    page_daily = {"project": [], "page_title": [], "dt": [], "views": [],
                  "hours_present": []}

    for title, count in views.items():
        # page_daily gets every page, per amendment 2.
        page_daily["project"].append("en.wikipedia")
        page_daily["page_title"].append(title)
        page_daily["dt"].append(hour_start.date())
        page_daily["views"].append(count)
        page_daily["hours_present"].append(1)

        # page_hour is truncated at the floor.
        if count >= PAGE_HOUR_MIN_VIEWS:
            page_hour["project"].append("en.wikipedia")
            page_hour["page_title"].append(title)
            page_hour["hour_start"].append(hour_start)
            page_hour["views"].append(count)

    return page_hour, page_daily


# --- main ------------------------------------------------------------------

def run(args):
    source_hour = parse_source_hour(args.source_hour)
    hour_start = hour_start_of(source_hour)
    key = f"{source_hour:%Y-%m-%dT%H}"
    url = source_url(source_hour)
    worker_id = f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:6]}"

    print("=" * 70)
    print(f"INGEST {key}  (source filename hour)")
    print("=" * 70)
    log(f"source url : {url}")
    log(f"hour_start : {hour_start:%Y-%m-%d %H}:00 UTC  "
        f"(= filename hour minus 1, per SPEC)")
    log(f"worker     : {worker_id}")
    log(f"connections: 1 of {MAX_CONNECTIONS} allowed by Wikimedia")

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
        log(f"  completed_at   : {existing.get('completed_at')}")
        log("Nothing was downloaded and nothing was written. Pass --force to redo it.")
        return 0

    if args.dry_run:
        # A dry run must not touch the manifest. Reading and parsing an hour is
        # not the same as claiming it, and a dry run that left a row in-flight
        # would be lying about an hour that is actually done.
        log("dry run: the manifest will not be claimed or modified")
    else:
        if not manifest.claim(key, url, hour_start):
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
            url, gz_path,
            None if args.dry_run else manifest,
            None if args.dry_run else key)
        elapsed = time.time() - started
        log(f"{human(size)} in {elapsed:.1f}s ({human(size / max(elapsed, 0.01))}/s)")
        log(f"sha256 {sha256}")
        if declared and declared != size:
            raise RuntimeError(f"content-length {declared} != bytes received {size}")
        log(f"content-length verified: {size:,} bytes")

        print()
        log("parsing...")
        views, stats = parse(gz_path)
        log(f"{stats['lines']:,} source lines, {stats['rows_parsed']:,} English rows kept")
        log(f"dropped: namespace {stats['dropped_namespace']:,}, "
            f"title {stats['dropped_excluded_title']:,}, "
            f"bad utf-8 {stats['dropped_bad_utf8']:,}, "
            f"malformed {stats['malformed']:,}, bad int {stats['bad_int']:,}")
        kept_views = stats["views_en"] + stats["views_en_m"]
        log(f"views kept: en {stats['views_en']:,} desktop + en_m "
            f"{stats['views_en_m']:,} mobile = {kept_views:,}")
        log(f"reconciliation: {stats['views_before_exclusions']:,} en+en.m views "
            f"before exclusions = {kept_views:,} kept + "
            f"{stats['views_dropped_exclusions']:,} excluded")
        log(f"{len(views):,} distinct titles after union and exclusions")

        page_hour, page_daily = build_tiers(views, hour_start)
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
        daily_key = (f"curated/page_daily/dt={hour_start:%Y-%m-%d}/"
                     f"part-{key}.parquet")
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
            "content_length": size,
            "sha256": sha256,
            "rows_parsed": stats["rows_parsed"],
            "rows_page_hour": hour_rows,
            "rows_page_daily": daily_rows,
            "bytes_page_hour": hour_bytes,
            "bytes_page_daily": daily_bytes,
            "bytes_source_gz": size,
            "distinct_titles": len(views),
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
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--force", action="store_true",
                    help="re-process an hour the manifest already calls done")
    ap.add_argument("--dry-run", action="store_true",
                    help="download and parse, but write nothing to S3")
    args = ap.parse_args()
    return run(args)


if __name__ == "__main__":
    sys.exit(main())
