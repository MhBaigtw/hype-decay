#!/usr/bin/env python3
"""
Does a compacted day match a fresh, independent compaction of the same partials?

Trial-only. backfill.py --verify-copy copies each day's 24 partials to
curated/_verify/ before compacting, because compaction deletes them. This script
rebuilds the day from those copies in a separate process, applies the floor the
day row records, and compares with the day.parquet the runner placed:

  * table equality, row for row -- both are sorted by page_title, so equal
    tables mean identical content, not just matching totals
  * the unfloored views against the manifest: the sum of views_kept over the
    day's 24 hour rows, each recorded at parse time, before any compaction

The second check is the independent one. The first could agree with the runner
if both made the same mistake; the manifest totals were written before either
compaction ran.

    python3 verify_compaction.py --dt 2024-09-13 --dt 2024-09-14 --cleanup
"""

import argparse
import datetime as dt
import io
import os
import sys
from pathlib import Path

import boto3
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compact_day import aggregate_arrow, source_hours_for  # noqa: E402
from ingest_hour import DEFAULT_BUCKET, DEFAULT_REGION, DEFAULT_TABLE  # noqa: E402


def verify(s3, table, bucket, day, cleanup):
    row = table.get_item(Key={"source_hour": f"day#{day}"}).get("Item") or {}
    floor = int(row.get("floor_applied", 0))
    copies = [f"curated/_verify/page_daily/dt={day}/part-{h}.parquet"
              for h in source_hours_for(day)]
    bodies = [s3.get_object(Bucket=bucket, Key=k)["Body"].read() for k in copies]

    fresh = aggregate_arrow(bodies, day)
    fresh_unfloored_views = pc.sum(fresh["views"]).as_py()
    fresh_unfloored_rows = fresh.num_rows
    fresh = fresh.filter(pc.greater_equal(fresh["views"], floor))

    placed = pq.read_table(io.BytesIO(s3.get_object(
        Bucket=bucket, Key=f"curated/page_daily/dt={day}/day.parquet")["Body"].read()))

    manifest_views = sum(int((table.get_item(Key={"source_hour": h}).get("Item") or {})
                             .get("views_kept", -10**12)) for h in source_hours_for(day))

    checks = {
        "day row compacted": row.get("status") == "compacted",
        "tables identical": placed.equals(fresh.cast(placed.schema)),
        "rows": placed.num_rows == fresh.num_rows,
        "views": pc.sum(placed["views"]).as_py() == pc.sum(fresh["views"]).as_py(),
        "unfloored views == manifest sum of views_kept":
            fresh_unfloored_views == manifest_views == int(row.get("views_unfloored", -1)),
        "unfloored rows == day row": fresh_unfloored_rows == int(row.get("rows_unfloored", -1)),
    }
    ok = all(checks.values())
    print(f"{day}  {'PASS' if ok else 'FAIL'}  floor {floor}  "
          f"rows {placed.num_rows:,}  views {pc.sum(placed['views']).as_py():,}  "
          f"unfloored {fresh_unfloored_rows:,} rows / {fresh_unfloored_views:,} views  "
          f"manifest {manifest_views:,}")
    for name, good in checks.items():
        if not good:
            print(f"      failed: {name}")

    if cleanup and ok:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": k} for k in copies]})
        print(f"      removed {len(copies)} verification copies")
    return ok


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", action="append", required=True, type=dt.date.fromisoformat)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--cleanup", action="store_true",
                    help="delete the _verify copies of every day that passes")
    args = ap.parse_args()
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3, table = session.client("s3"), session.resource("dynamodb").Table(args.table)
    results = [verify(s3, table, args.bucket, d, args.cleanup) for d in args.dt]
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
