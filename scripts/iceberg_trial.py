#!/usr/bin/env python3
"""
Task 4 approach comparison, measured on one month of page_hour.

  (a) adopt in place (Glue add_files): Iceberg metadata over the files exactly
      as the ingester wrote them. Queried here through page_hour_parquet, which
      reads those same files, so its scanned bytes are what (a) would scan.
  (b) rewrite: Athena INSERT into a new Iceberg table, ORDER BY dt, page_title,
      zstd. Built here as trial_page_hour under curated/iceberg/_trial/.

Reports storage and file counts for both, whether the rewrite actually came
out sorted (row-group statistics of a written file), and bytes scanned for two
query shapes the project will run: one page's curve across the month (the
Task 7 lookup) and per-day totals (the dbt rollups). Drops the trial table and
its files at the end.

    python3 iceberg_trial.py --month 2025-03
"""

import argparse
import datetime as dt
import io
import os
import sys
from pathlib import Path

import boto3
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "ingest"))
from athena import Athena, mib  # noqa: E402

BUCKET = "hype-decay-curated-820697996849"
TRIAL_PREFIX = "curated/iceberg/_trial/page_hour/"


def prefix_size(s3, prefix):
    n = size = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for o in page.get("Contents", []):
            n, size = n + 1, size + o["Size"]
    return n, size


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--month", default="2025-03")
    ap.add_argument("--page", default="Taylor_Swift")
    ap.add_argument("--keep", action="store_true")
    args = ap.parse_args()

    first = dt.date.fromisoformat(args.month + "-01")
    last = (first.replace(day=28) + dt.timedelta(days=4)).replace(day=1) - dt.timedelta(days=1)
    days = (last - first).days + 1
    where = f"dt BETWEEN DATE '{first}' AND DATE '{last}'"
    a = Athena()
    s3 = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"),
                       region_name="us-east-1").client("s3")

    # --- (a) the files as they are -----------------------------------------
    n_a = size_a = 0
    for d in range(days):
        n, size = prefix_size(s3, f"curated/page_hour/dt={first + dt.timedelta(days=d)}/")
        n_a, size_a = n_a + n, size_a + size

    # --- (b) rewrite into a trial Iceberg table ------------------------------
    a.run("DROP TABLE IF EXISTS trial_page_hour")
    a.run(f"""CREATE TABLE trial_page_hour (
        project string, page_title string, hour_start timestamp, views bigint, dt date)
      PARTITIONED BY (dt)
      LOCATION 's3://{BUCKET}/{TRIAL_PREFIX}'
      TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet', 'write_compression'='zstd')""")
    ins = a.run(f"""INSERT INTO trial_page_hour
      SELECT project, page_title, hour_start, views, dt FROM page_hour_parquet
      WHERE {where} ORDER BY dt, page_title, hour_start""")
    data_n, data_size = prefix_size(s3, TRIAL_PREFIX + "data/")
    meta_n, meta_size = prefix_size(s3, TRIAL_PREFIX + "metadata/")

    # Did the sort survive the write? Read row-group stats of one data file.
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=TRIAL_PREFIX + "data/")
            .get("Contents", [])]
    sample = pq.ParquetFile(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=keys[0])["Body"].read()))
    md = sample.metadata
    titles = sample.read(columns=["page_title"]).column("page_title").to_pylist()
    descents = sum(1 for x, y in zip(titles, titles[1:]) if x > y)
    idx = sample.schema_arrow.get_field_index("page_title")
    def bounds(i):
        st = md.row_group(i).column(idx).statistics
        if st is None or not st.has_min_max:
            return "no min/max statistics", ""
        return st.min[:12], st.max[:12]
    groups = [(md.row_group(i).num_rows, *bounds(i)) for i in range(md.num_row_groups)]

    # --- the two query shapes, against both ----------------------------------
    lookup = (f"SELECT date_trunc('day', hour_start) d, sum(views) v FROM {{t}} "
              f"WHERE {where} AND page_title = '{args.page}' GROUP BY 1")
    daily = f"SELECT dt, count(*) n, sum(views) v FROM {{t}} WHERE {where} GROUP BY dt"
    q = {}
    for shape, sql in (("lookup", lookup), ("daily", daily)):
        for name, table in (("adopt", "page_hour_parquet"), ("rewrite", "trial_page_hour")):
            r = a.run(sql.format(t=table), fetch=True)
            q[(shape, name)] = r
    same = all(sorted(q[(s, "adopt")]["rows"]) == sorted(q[(s, "rewrite")]["rows"])
               for s in ("lookup", "daily"))

    print(f"month {args.month}: {days} days")
    print(f"(a) adopt in place : {n_a:>6,} files, {mib(size_a):>12}  (snappy, unsorted, 1 row group per hour)")
    print(f"(b) Athena rewrite : {data_n:>6,} files, {mib(data_size):>12}  + metadata {meta_n} files {mib(meta_size)}"
          f"   build scanned {mib(ins['scanned'])} in {ins['secs']:.0f}s")
    print(f"    sample file: {md.num_rows:,} rows, {md.num_row_groups} row group(s), "
          f"descents in page_title order: {descents:,}")
    for g in groups[:4]:
        print(f"      rows {g[0]:>9,}  {g[1]!r} .. {g[2]!r}")
    for shape in ("lookup", "daily"):
        ra, rb = q[(shape, "adopt")], q[(shape, "rewrite")]
        print(f"query {shape:<7}: adopt {mib(ra['scanned']):>10} {ra['secs']:5.1f}s | "
              f"rewrite {mib(rb['scanned']):>10} {rb['secs']:5.1f}s | "
              f"{ra['scanned'] / max(rb['scanned'], 1):.1f}x less with the rewrite")
    print(f"results identical between (a) and (b): {same}")
    print(f"total Athena bytes scanned by this trial: {mib(a.scanned_total)}")

    if not args.keep:
        a.run("DROP TABLE IF EXISTS trial_page_hour")
        left = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=TRIAL_PREFIX)
                .get("Contents", [])]
        for i in range(0, len(left), 1000):
            s3.delete_objects(Bucket=BUCKET, Delete={"Objects": [{"Key": k} for k in left[i:i + 1000]]})
        print(f"trial table dropped; {len(left)} trial objects deleted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
