#!/usr/bin/env python3
"""
Correct one day after the switch to Iceberg. The procedure SPEC records:

    1. re-ingest its 24 source hours with --force       (staging partials back;
       the day row flips to invalidated, per the double-count guard)
    2. recompact                                         (rebuilt from exactly
       those 24 partials, checked against the totals the invalidation recorded)
    3. replace the day in Iceberg: DELETE that dt, INSERT it from staging, in
       both tables, then check it against the manifest   (publish.replace_in_iceberg)
    4. retire the staging copy and the manifest fields that named it

Never an INSERT alongside the old rows: that is the double count the guard on
the plain-Parquet side exists to prevent, moved into Iceberg.

PROOF built in. Before anything changes, it records rows, views and a row-level
checksum for EVERY day of the target's month, in both tables; afterwards it
records them again. The target day must come back identical (same source bytes,
same code, so a correction that changes nothing must change nothing), and every
other day must be untouched.

    python3 republish_day.py --dt 2025-03-15
"""

import argparse
import datetime as dt
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import ingest_hour  # noqa: E402
import publish  # noqa: E402
from athena import Athena, mib  # noqa: E402
from compact_day import source_hours_for  # noqa: E402

BUCKET = ingest_hour.DEFAULT_BUCKET

SNAPSHOT = {
    "page_daily": """SELECT CAST(dt AS varchar), count(*), sum(views),
        to_hex(checksum(ROW(project, page_title, views, hours_present)))
      FROM page_daily WHERE dt BETWEEN DATE '{lo}' AND DATE '{hi}' GROUP BY dt""",
    "page_hour": """SELECT CAST(dt AS varchar), count(*), sum(views),
        to_hex(checksum(ROW(project, page_title, views, to_unixtime(hour_start))))
      FROM page_hour WHERE dt BETWEEN DATE '{lo}' AND DATE '{hi}' GROUP BY dt""",
}


def log(msg):
    print(f"{dt.datetime.now(dt.timezone.utc):%H:%M:%S}  {msg}", flush=True)


def month_bounds(day):
    lo = day.replace(day=1)
    hi = (lo.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
    return lo, hi


def snapshot(athena, day):
    lo, hi = month_bounds(day)
    return {t: {r[0]: tuple(r[1:]) for r in athena.run(sql.format(lo=lo, hi=hi), fetch=True)["rows"]}
            for t, sql in SNAPSHOT.items()}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", required=True, type=dt.date.fromisoformat)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    args = ap.parse_args()
    day = args.dt

    session = boto3.Session(profile_name=args.profile, region_name="us-east-1")
    s3 = session.client("s3")
    table = session.resource("dynamodb").Table(ingest_hour.DEFAULT_TABLE)
    athena = Athena(profile=args.profile)

    row = table.get_item(Key={"source_hour": f"day#{day}"}).get("Item") or {}
    log(f"day#{day}: status {row.get('status')}, iceberg_state {row.get('iceberg_state')}, "
        f"staging_removed_at {row.get('staging_removed_at')}")
    if row.get("status") != "compacted" or row.get("iceberg_state") != "published":
        log("refusing: republish corrects a day that is compacted and published")
        return 2

    before = snapshot(athena, day)
    log(f"before: {len(before['page_daily'])} days of the month snapshotted; "
        f"{day}: page_daily {before['page_daily'].get(str(day))}, "
        f"page_hour {before['page_hour'].get(str(day))}")

    # 1. re-ingest, one connection, the fixture off
    started = time.time()
    for h in source_hours_for(day):
        ns = argparse.Namespace(
            source_hour=h, source="your.org", verify_origin_length=True, bucket=BUCKET,
            table=ingest_hour.DEFAULT_TABLE, region="us-east-1", profile=args.profile,
            force=True, dry_run=False, no_fixture=True)
        with open(os.devnull, "w") as quiet:
            sys.stdout, real = quiet, sys.stdout
            try:
                code = ingest_hour.run(ns)
            finally:
                sys.stdout = real
        if code != 0:
            log(f"re-ingest of {h} returned {code}; stopping")
            return 1
    log(f"1. re-ingested 24 hours in {time.time() - started:.0f}s; "
        f"day row now {table.get_item(Key={'source_hour': f'day#{day}'})['Item']['status']}")

    # 2. recompact, in its own process as the backfill does
    proc = subprocess.run([sys.executable, str(HERE / "compact_day.py"), "--dt", str(day)]
                          + (["--profile", args.profile] if args.profile else []),
                          capture_output=True, text=True)
    for line in proc.stdout.splitlines():
        if "rebuild vs invalidated" in line or "floor" in line[:20] or "compacted:" in line:
            log("   " + line.strip())
    if proc.returncode != 0:
        log(f"compaction failed ({proc.returncode}):\n{proc.stdout[-1500:]}")
        return 1
    log("2. recompacted")

    # 3. replace in Iceberg, 4. retire staging
    publish.replace_in_iceberg(athena, table, day)
    log("3. replaced in Iceberg (DELETE + INSERT, both tables) and matched to the manifest")
    removed = publish.retire_staging(s3, table, BUCKET, day)
    log(f"4. retired {removed} staging objects")

    after = snapshot(athena, day)
    ok = True
    for t in SNAPSHOT:
        changed = sorted(d for d in set(before[t]) | set(after[t]) if before[t].get(d) != after[t].get(d))
        same_day = before[t].get(str(day)) == after[t].get(str(day))
        others = [d for d in changed if d != str(day)]
        log(f"{t:<10} {day}: {after[t].get(str(day))} -- {'UNCHANGED' if same_day else 'CHANGED'}; "
            f"other days of the month changed: {others or 'none'}")
        ok = ok and same_day and not others
    log(f"Athena scanned {mib(athena.scanned_total)} in total")
    log("PROVEN: day replaced atomically per table, identical, nothing else touched"
        if ok else "NOT PROVEN")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
