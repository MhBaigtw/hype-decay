#!/usr/bin/env python3
"""
The backfill's definition of done, checked against the manifest, Iceberg and S3.

  hours    every one of the 17,520 window hours has a manifest row, done or
           failed with a reason; failures under 1%
  days     every one of the 730 days is compacted at floor 10, and its unfloored
           views equal the sum of views_kept over its 24 hour rows
  iceberg  every day is published (iceberg_state); in hype_decay.page_daily its
           rows and views equal the day row, and in hype_decay.page_hour each
           hour's rows equal that hour row's rows_page_hour
  staging  a day whose staging copy was retired (staging_removed_at) has no
           staging objects left AND no manifest field naming one; a day not yet
           retired still has its day.parquet at the recorded size
  clean    nothing under _staging/, _verify/ or _invalidated/; no fixtures
  size     the bucket, by prefix

Read-only. Athena queries are one month at a time, filtered on dt, in the
scan-capped workgroup. Exit 0 only if every check passes.

    python3 verify_backfill.py
"""

import collections
import datetime as dt
import os
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from athena import Athena, mib  # noqa: E402
from backfill import WINDOW_END, WINDOW_START, window_days  # noqa: E402
from compact_day import DEFAULT_FLOOR, source_hours_for  # noqa: E402
from ingest_hour import DEFAULT_BUCKET, DEFAULT_REGION, DEFAULT_TABLE  # noqa: E402
from publish import staging_keys  # noqa: E402


def scan(table):
    items, kwargs = [], {}
    while True:
        page = table.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def list_keys(s3, bucket, prefix):
    out = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for o in page.get("Contents", []):
            out[o["Key"]] = o["Size"]
    return out


def months(days):
    by = collections.OrderedDict()
    for d in days:
        by.setdefault((d.year, d.month), []).append(d)
    return [(v[0], v[-1]) for v in by.values()]


def gib(n):
    return f"{n / 2**30:,.2f} GiB"


def main():
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                            region_name=DEFAULT_REGION)
    s3 = session.client("s3")
    rows = {i["source_hour"]: i for i in scan(session.resource("dynamodb").Table(DEFAULT_TABLE))}
    days = window_days(WINDOW_START, WINDOW_END)
    hours = [h for d in days for h in source_hours_for(d)]
    checks = {}

    # --- hours ----------------------------------------------------------------
    status = collections.Counter(rows.get(h, {}).get("status", "MISSING") for h in hours)
    failed = [h for h in hours if rows.get(h, {}).get("status") == "failed"]
    unexplained = [h for h in failed if not rows[h].get("error")]
    mismatched = [h for h in hours if rows.get(h, {}).get("status") == "done"
                  and int(rows[h].get("origin_content_length", 0))
                  and int(rows[h]["origin_content_length"]) != int(rows[h]["content_length"])]
    print(f"hours: {len(hours):,} in window, manifest {dict(status)}, "
          f"failed {len(failed)} ({100 * len(failed) / len(hours):.3f}%)")
    checks["every window hour has a row"] = status.get("MISSING", 0) == 0
    checks["every hour done or failed"] = set(status) <= {"done", "failed"}
    checks["every failure has a reason"] = not unexplained
    checks["failures under 1%"] = len(failed) / len(hours) < 0.01
    checks["no mirror/origin size mismatch"] = not mismatched

    # --- days, manifest-only -------------------------------------------------
    day_rows = {d: rows.get(f"day#{d}") or {} for d in days}
    not_compacted = [str(d) for d, r in day_rows.items() if r.get("status") != "compacted"]
    wrong_floor = [str(d) for d, r in day_rows.items()
                   if int(r.get("floor_applied", -1)) != DEFAULT_FLOOR]
    unreconciled = [str(d) for d, r in day_rows.items()
                    if int(r.get("views_unfloored", -1)) !=
                    sum(int(rows.get(h, {}).get("views_kept", 0)) for h in source_hours_for(d))]
    print(f"days: {len(days)}; not compacted {not_compacted[:3]}; floor != {DEFAULT_FLOOR} "
          f"{wrong_floor[:3]}; unfloored views != hour totals {unreconciled[:3]}")
    checks["every day compacted"] = not not_compacted
    checks[f"every day at floor {DEFAULT_FLOOR}"] = not wrong_floor
    checks["every day reconciles to its 24 hours"] = not unreconciled

    # --- Iceberg against the manifest -------------------------------------------
    unpublished = [str(d) for d, r in day_rows.items() if r.get("iceberg_state") != "published"]
    athena = Athena()
    daily, hourly = {}, {}
    for lo, hi in months(days):
        w = f"dt BETWEEN DATE '{lo}' AND DATE '{hi}'"
        for r in athena.run(f"SELECT CAST(dt AS varchar), count(*), sum(views) FROM page_daily "
                            f"WHERE {w} GROUP BY dt", fetch=True)["rows"]:
            daily[r[0]] = (int(r[1]), int(r[2]))
        for r in athena.run(f"SELECT date_format(hour_start + INTERVAL '1' HOUR, '%Y-%m-%dT%H'), "
                            f"count(*) FROM page_hour WHERE {w} GROUP BY 1", fetch=True)["rows"]:
            hourly[r[0]] = int(r[1])
    daily_bad = [str(d) for d, r in day_rows.items()
                 if daily.get(str(d)) != (int(r.get("rows", -1)), int(r.get("views", -1)))]
    hourly_bad = [h for h in hours if hourly.get(h, 0) != int(rows.get(h, {}).get("rows_page_hour", -1))]
    print(f"iceberg: {len(daily)} days in page_daily, {len(hourly):,} hours in page_hour; "
          f"unpublished {unpublished[:3]}; page_daily != day row {daily_bad[:3]}; "
          f"page_hour != hour row {hourly_bad[:3]}; scanned {mib(athena.scanned_total)}")
    checks["every day published to Iceberg"] = not unpublished
    checks["page_daily rows and views per day == manifest"] = not daily_bad and len(daily) == len(days)
    checks["page_hour rows per hour == manifest"] = not hourly_bad and len(hourly) == len(hours)

    # --- staging, and what the manifest claims about it -------------------------
    staged = {**list_keys(s3, DEFAULT_BUCKET, "curated/page_daily/"),
              **list_keys(s3, DEFAULT_BUCKET, "curated/page_hour/")}
    retired = [d for d, r in day_rows.items() if r.get("staging_removed_at")]
    left_behind = [k for d in retired for k in staging_keys(d) if k in staged]
    stale_claims = ([f"day#{d}" for d in retired if "key_compacted" in day_rows[d]]
                    + [h for d in retired for h in source_hours_for(d)
                       if {"key_page_hour", "key_page_daily"} & set(rows.get(h, {}))])
    not_retired_bad = [str(d) for d, r in day_rows.items() if not r.get("staging_removed_at")
                       and staged.get(staging_keys(d)[0]) != int(r.get("bytes_compacted", -1))]
    stray = sorted(set(staged) - {k for d in days for k in staging_keys(d)})
    print(f"staging: {len(retired)} days retired, {len(staged)} staging objects left; "
          f"left behind {left_behind[:2]}; stale manifest claims {stale_claims[:2]}; "
          f"unretired days missing day.parquet {not_retired_bad[:2]}; stray {stray[:2]}")
    checks["retired days have no staging objects"] = not left_behind
    checks["no manifest field names a deleted file"] = not stale_claims
    checks["unretired days still have their day.parquet"] = not not_retired_bad
    checks["no stray staging objects"] = not stray

    # --- clean ------------------------------------------------------------------
    leftovers = {p: len(list_keys(s3, DEFAULT_BUCKET, p)) for p in
                 ("curated/_staging/", "curated/_verify/", "curated/_invalidated/",
                  "curated/iceberg/_trial/")}
    fixtures = list_keys(s3, DEFAULT_BUCKET, "fixtures/")
    print(f"leftovers: {leftovers}; fixtures: {len(fixtures)}")
    checks["no _staging, _verify, _invalidated or _trial objects"] = not any(leftovers.values())
    checks["no fixtures in S3"] = not fixtures

    # --- size -------------------------------------------------------------------
    sizes, counts = collections.Counter(), collections.Counter()
    for k, n in list_keys(s3, DEFAULT_BUCKET, "").items():
        parts = k.split("/")
        top = "/".join(parts[:3]) if k.startswith("curated/iceberg/") else (
            "/".join(parts[:2]) if k.startswith("curated/") else parts[0])
        sizes[top] += n
        counts[top] += 1
    print("bucket by prefix:")
    for top, n in sorted(sizes.items(), key=lambda kv: -kv[1]):
        print(f"  {top:<30} {counts[top]:>7,} objects  {gib(n):>11}")

    print()
    for name, ok in checks.items():
        print(f"  [{'ok' if ok else 'FAIL'}] {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
