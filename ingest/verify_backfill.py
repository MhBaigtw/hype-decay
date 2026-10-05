#!/usr/bin/env python3
"""
Task 3 definition of done, checked against the manifest and S3 together.

  hours   every one of the 17,520 window hours has a manifest row, and each is
          done, or failed with a recorded reason; failures under 1%
  days    every one of the 730 days has a day row, compacted, at floor 10, and
          its unfloored views equal the sum of views_kept over its 24 hour rows
          (each written at parse time, before any compaction)
  layout  each page_daily partition holds exactly one object, day.parquet, of
          the size the day row recorded; no partials anywhere; one page_hour
          object per hour
  clean   nothing under _staging/, _verify/ or _invalidated/; no fixture
          uploaded by the backfill
  size    the curated zone, by prefix

Read-only. Exit 0 only if every check passes.

    python3 verify_backfill.py
"""

import collections
import os
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent))
from backfill import WINDOW_END, WINDOW_START, window_days  # noqa: E402
from compact_day import DEFAULT_FLOOR, source_hours_for  # noqa: E402
from ingest_hour import DEFAULT_BUCKET, DEFAULT_REGION, DEFAULT_TABLE  # noqa: E402


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
    extra = sorted(k for k in rows if not k.startswith("day#") and k not in set(hours))
    print(f"hours: {len(hours):,} in window, manifest {dict(status)}")
    checks["every window hour has a row"] = status.get("MISSING", 0) == 0
    checks["every hour done or failed"] = set(status) <= {"done", "failed"}
    checks["every failure has a reason"] = not unexplained
    checks["failures under 1%"] = len(failed) / len(hours) < 0.01
    print(f"  failed {len(failed)} ({100 * len(failed) / len(hours):.3f}%), "
          f"rows outside the window: {extra[:5]}")
    unverified = [h for h in hours if rows.get(h, {}).get("status") == "done"
                  and int(rows[h].get("origin_content_length", 0)) == 0]
    mismatched = [h for h in hours if rows.get(h, {}).get("status") == "done"
                  and int(rows[h].get("origin_content_length", 0))
                  and int(rows[h]["origin_content_length"]) != int(rows[h]["content_length"])]
    print(f"  origin content-length: {len(unverified)} hour(s) unchecked (HEAD failed), "
          f"{len(mismatched)} mismatched")
    checks["no mirror/origin size mismatch"] = not mismatched

    # --- days -----------------------------------------------------------------
    day_rows = {d: rows.get(f"day#{d}") for d in days}
    not_compacted = [str(d) for d, r in day_rows.items() if not r or r.get("status") != "compacted"]
    wrong_floor = [str(d) for d, r in day_rows.items()
                   if r and int(r.get("floor_applied", -1)) != DEFAULT_FLOOR]
    unreconciled = []
    for d, r in day_rows.items():
        if not r:
            continue
        kept = sum(int(rows.get(h, {}).get("views_kept", 0)) for h in source_hours_for(d))
        if int(r.get("views_unfloored", -1)) != kept:
            unreconciled.append((str(d), int(r.get("views_unfloored", -1)), kept))
    print(f"days: {len(days)} in window, compacted "
          f"{len(days) - len(not_compacted)}, not compacted {not_compacted[:5]}")
    print(f"  floor != {DEFAULT_FLOOR}: {wrong_floor[:5]}; "
          f"unfloored views != manifest sum: {unreconciled[:3]}")
    checks["every day compacted"] = not not_compacted
    checks[f"every day at floor {DEFAULT_FLOOR}"] = not wrong_floor
    checks["every day reconciles to its 24 hours"] = not unreconciled

    # --- layout ---------------------------------------------------------------
    daily = list_keys(s3, DEFAULT_BUCKET, "curated/page_daily/")
    hourly = list_keys(s3, DEFAULT_BUCKET, "curated/page_hour/")
    by_dt = collections.defaultdict(list)
    for k in daily:
        by_dt[k.split("/")[2]].append(k.rsplit("/", 1)[1])
    bad_partitions = {dt: names for dt, names in by_dt.items() if names != ["day.parquet"]}
    missing_partitions = [str(d) for d in days if f"dt={d}" not in by_dt]
    size_mismatch = [str(d) for d in days if day_rows[d]
                     and daily.get(f"curated/page_daily/dt={d}/day.parquet")
                     != int(day_rows[d].get("bytes_compacted", -1))]
    # The page_hour key each hour's manifest row says it wrote.
    expected_hour_keys = set()
    for h in hours:
        r = rows.get(h, {})
        if r.get("key_page_hour"):
            expected_hour_keys.add(r["key_page_hour"])
    missing_hour_objects = sorted(expected_hour_keys - set(hourly))
    stray_hour_objects = sorted(set(hourly) - expected_hour_keys)
    print(f"page_daily: {len(by_dt)} partitions, {len(daily)} objects; not exactly "
          f"[day.parquet]: {list(bad_partitions.items())[:3]}; missing: {missing_partitions[:3]}")
    print(f"  day.parquet size != day row bytes_compacted: {size_mismatch[:3]}")
    print(f"page_hour: {len(hourly):,} objects; missing {len(missing_hour_objects)}, "
          f"not in manifest {len(stray_hour_objects)} {stray_hour_objects[:3]}")
    checks["one day.parquet per day, no partials"] = not bad_partitions and not missing_partitions
    checks["day.parquet sizes match the manifest"] = not size_mismatch
    checks["one page_hour object per hour"] = (not missing_hour_objects
                                              and not stray_hour_objects
                                              and len(hourly) == len(hours))

    # --- clean ----------------------------------------------------------------
    leftovers = {p: len(list_keys(s3, DEFAULT_BUCKET, p)) for p in
                 ("curated/_staging/", "curated/_verify/", "curated/_invalidated/")}
    fixtures = list_keys(s3, DEFAULT_BUCKET, "fixtures/")
    fixture_rows = [h for h in hours if rows.get(h, {}).get("fixture_uploaded", False)
                    or ("key_fixture" in rows.get(h, {})
                        and "fixture_uploaded" not in rows.get(h, {}))]
    print(f"leftovers: {leftovers}; fixtures in S3 now: {len(fixtures)}; "
          f"hours whose manifest records a fixture upload: {len(fixture_rows)} "
          f"{fixture_rows[:2]}{' ...' if len(fixture_rows) > 2 else ''}")
    checks["no _staging, _verify or _invalidated objects"] = not any(leftovers.values())
    checks["no fixtures in S3"] = not fixtures

    # --- size -----------------------------------------------------------------
    everything = list_keys(s3, DEFAULT_BUCKET, "")
    sizes = collections.Counter()
    counts = collections.Counter()
    for k, n in everything.items():
        top = "/".join(k.split("/")[:2]) if k.startswith("curated/") else k.split("/")[0]
        sizes[top] += n
        counts[top] += 1
    print("bucket by prefix:")
    for top, n in sorted(sizes.items(), key=lambda kv: -kv[1]):
        print(f"  {top:<24} {counts[top]:>7,} objects  {gib(n):>12}")
    curated = sum(n for t, n in sizes.items() if t.startswith("curated/"))
    print(f"  curated zone total      {sum(c for t, c in counts.items() if t.startswith('curated/')):>7,} "
          f"objects  {gib(curated):>12}")

    print()
    for name, ok in checks.items():
        print(f"  [{'ok' if ok else 'FAIL'}] {name}")
    return 0 if all(checks.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
