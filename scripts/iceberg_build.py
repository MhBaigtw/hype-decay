#!/usr/bin/env python3
"""
Task 4: rewrite the curated zone into Iceberg tables, then prove them identical.

Decision and measurements: NOTES, Task 4. In short, an Athena rewrite sorted by
page_title beat adopting the existing files in place: on March 2025 it stored
page_hour 5.4x smaller and scanned 10.5x fewer bytes for a one-page lookup.

    build    CREATE the two Iceberg tables, then INSERT one month at a time
             (an Athena INSERT may write at most 100 partitions, and a month of
             page_hour is ~1.7 GiB of source, inside the 5 GiB workgroup cap).
             Each INSERT is one Iceberg commit, so a month is either all there
             or absent; a re-run skips the months already present.
    verify   per day, old vs new: row count, total views and an order-insensitive
             checksum over every row. Then new vs the manifest: page_daily rows
             and views per day against each day row; page_hour rows per hour
             against each hour row's rows_page_hour.

Every query filters on dt, as CLAUDE.md requires, and runs in the scan-capped
workgroup.

    publish  after verify passes: mark every day published on its manifest day row
    retire   delete each published day's staging copy (day.parquet and 24
             page_hour files) and the manifest fields naming them -- refused,
             per day, unless that day is published (ingest/publish.py)

    python3 iceberg_build.py build
    python3 iceberg_build.py verify --report verify.json
    python3 iceberg_build.py publish
    python3 iceberg_build.py retire [--dt 2025-03-15]
"""

import argparse
import datetime as dt
import json
import os
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from athena import Athena, mib  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from backfill import WINDOW_END, WINDOW_START  # noqa: E402
from compact_day import source_hours_for  # noqa: E402
import publish  # noqa: E402

BUCKET = "hype-decay-curated-820697996849"

DDL = {
    "page_hour": f"""CREATE TABLE IF NOT EXISTS page_hour (
        project string, page_title string, hour_start timestamp, views bigint, dt date)
      PARTITIONED BY (dt)
      LOCATION 's3://{BUCKET}/curated/iceberg/page_hour/'
      TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='zstd')""",
    "page_daily": f"""CREATE TABLE IF NOT EXISTS page_daily (
        project string, page_title string, dt date, views bigint, hours_present int)
      PARTITIONED BY (dt)
      LOCATION 's3://{BUCKET}/curated/iceberg/page_daily/'
      TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='zstd')""",
}

INSERT = {
    # Sorted so each title's rows sit together: that is where the 5.4x smaller
    # storage and the cheap page lookups come from.
    "page_hour": """INSERT INTO page_hour
      SELECT project, page_title, hour_start, views, dt FROM page_hour_parquet
      WHERE {where} ORDER BY dt, page_title, hour_start""",
    "page_daily": """INSERT INTO page_daily
      SELECT project, page_title, dt, views, hours_present FROM page_daily_parquet
      WHERE {where} ORDER BY dt, page_title""",
}

# Order-insensitive checksum over every row, with types normalised: the Hive
# table reads hour_start at millisecond precision and Iceberg at microsecond,
# so the timestamp is compared as epoch seconds, not as text.
PER_DAY = {
    "page_hour": """SELECT dt, count(*), sum(views),
        to_hex(checksum(ROW(project, page_title, views, to_unixtime(hour_start))))
      FROM {table} WHERE {where} GROUP BY dt""",
    "page_daily": """SELECT dt, count(*), sum(views),
        to_hex(checksum(ROW(project, page_title, views, hours_present)))
      FROM {table} WHERE {where} GROUP BY dt""",
}


def months():
    first = WINDOW_START.replace(day=1)
    while first <= WINDOW_END:
        nxt = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
        yield max(first, WINDOW_START), min(nxt - dt.timedelta(days=1), WINDOW_END)
        first = nxt


def where(lo, hi):
    return f"dt BETWEEN DATE '{lo}' AND DATE '{hi}'"


def build(a):
    for table, ddl in DDL.items():
        a.run(ddl)
    for lo, hi in months():
        for table in ("page_hour", "page_daily"):
            have = int(a.run(f"SELECT count(*) FROM {table} WHERE {where(lo, hi)}",
                             fetch=True)["rows"][0][0])
            if have:
                print(f"{lo:%Y-%m} {table:<10} already present ({have:,} rows), skipped")
                continue
            r = a.run(INSERT[table].format(where=where(lo, hi)))
            print(f"{lo:%Y-%m} {table:<10} scanned {mib(r['scanned']):>11} in {r['secs']:5.0f}s",
                  flush=True)
    print(f"build total scanned: {mib(a.scanned_total)}")


def scan_manifest():
    table = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                          region_name="us-east-1").resource("dynamodb").Table("hype-decay-manifest")
    items, kwargs = [], {}
    while True:
        page = table.scan(**kwargs)
        items += page["Items"]
        if "LastEvaluatedKey" not in page:
            return {i["source_hour"]: i for i in items}
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def verify(a, report_path):
    manifest = scan_manifest()
    problems, per_day = [], {}

    for lo, hi in months():
        w = where(lo, hi)
        for table in ("page_hour", "page_daily"):
            old = {r[0]: r[1:] for r in a.run(PER_DAY[table].format(
                table=f"{table}_parquet", where=w), fetch=True)["rows"]}
            new = {r[0]: r[1:] for r in a.run(PER_DAY[table].format(
                table=table, where=w), fetch=True)["rows"]}
            for day in sorted(set(old) | set(new)):
                if old.get(day) != new.get(day):
                    problems.append(f"{table} {day}: old {old.get(day)} new {new.get(day)}")
                per_day.setdefault(day, {})[table] = new.get(day)

        # page_hour rows per hour, against each hour row in the manifest.
        hourly = {r[0]: int(r[1]) for r in a.run(
            f"SELECT date_format(hour_start + INTERVAL '1' HOUR, '%Y-%m-%dT%H'), count(*) "
            f"FROM page_hour WHERE {w} GROUP BY 1", fetch=True)["rows"]}
        d = lo
        while d <= hi:
            for h in source_hours_for(d):
                want = int(manifest[h]["rows_page_hour"])
                if hourly.get(h, 0) != want:
                    problems.append(f"page_hour {h}: {hourly.get(h, 0)} rows, manifest {want}")
            d += dt.timedelta(days=1)
        print(f"{lo:%Y-%m} verified, {len(problems)} problem(s) so far", flush=True)

    # page_daily per day against each day row.
    for day, tables in sorted(per_day.items()):
        row = manifest.get(f"day#{day}") or {}
        got = tables.get("page_daily")
        want = (str(int(row.get("rows", -1))), str(int(row.get("views", -1))))
        if not got or tuple(got[:2]) != want:
            problems.append(f"page_daily {day}: {got[:2] if got else None} manifest {want}")

    expected_days = (WINDOW_END - WINDOW_START).days + 1
    summary = {
        "days_compared": len(per_day), "days_expected": expected_days,
        "page_hour_rows": sum(int(t["page_hour"][0]) for t in per_day.values() if t.get("page_hour")),
        "page_daily_rows": sum(int(t["page_daily"][0]) for t in per_day.values() if t.get("page_daily")),
        "page_daily_views": sum(int(t["page_daily"][1]) for t in per_day.values() if t.get("page_daily")),
        "problems": problems, "scanned_bytes": a.scanned_total,
    }
    Path(report_path).write_text(json.dumps(summary, indent=2, default=str))
    print(json.dumps({k: v for k, v in summary.items() if k != "problems"}, indent=2))
    print(f"{len(problems)} problem(s){': ' + problems[0] if problems else ''}")
    return 0 if not problems and len(per_day) == expected_days else 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("step", choices=["build", "verify", "publish", "retire"])
    ap.add_argument("--report", default="iceberg_verify.json")
    ap.add_argument("--dt", type=dt.date.fromisoformat, help="retire: this day only")
    args = ap.parse_args()
    if args.step in ("publish", "retire"):
        session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name="us-east-1")
        table = session.resource("dynamodb").Table("hype-decay-manifest")
        s3 = session.client("s3")
        days = [args.dt] if args.dt else [
            WINDOW_START + dt.timedelta(days=n) for n in range((WINDOW_END - WINDOW_START).days + 1)]
        done = 0
        for day in days:
            if args.step == "publish":
                publish.mark_published(table, day)
                done += 1
            else:
                done += publish.retire_staging(s3, table, BUCKET, day)
        print(f"{args.step}: {len(days)} day(s), {done} {'marked' if args.step == 'publish' else 'staging objects deleted'}")
        return 0
    a = Athena()
    if args.step == "build":
        build(a)
        return 0
    return verify(a, args.report)


if __name__ == "__main__":
    sys.exit(main())
