#!/usr/bin/env python3
"""
Compact one day of page_daily: 24 hourly partials become one object.

WHY THIS EXISTS. The ingester processes one hour at a time, so it writes one
page_daily partial per hour -- 24 per day, each carrying every page seen in that
hour. Measured on the fixture hour, a partial is 22.7 MiB, so keeping them for
the whole window would be about 389 GiB of page_daily against 37.7 GiB of
page_hour: the partials, not the data, would be the storage bill. Compaction is
what makes the curated zone affordable, and it is also where a daily-views floor
can be applied, because a floor is meaningless until a day is whole.

WHICH HOURS MAKE A DAY. Not the obvious ones. hour_start = source filename hour
minus 1, so the hours whose hour_start lands inside day D are source hours

    D T01 .. D T23, and (D+1) T00

That last one is the trap: the file named 2026-09-11-000000.gz holds
2026-09-10 23:00-24:00, so a day is not complete until the next day has started.

ORDER OF OPERATIONS. The compacted object is staged, the partials are deleted,
and only then is the compacted object copied into place. That order leaves the
partition briefly EMPTY rather than briefly DOUBLE-COUNTED. An empty partition
is a visible gap; a double count is a silent wrong answer, and this project
treats those differently. The manifest day row is the authority either way.

    python3 compact_day.py --dt 2026-09-10 --dry-run
    python3 compact_day.py --dt 2026-09-10 --floor 0
"""

import argparse
import datetime as dt
import io
import os
import sys
import time

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
DEFAULT_TABLE = "hype-decay-manifest"
DEFAULT_REGION = "us-east-1"

HOURS_PER_DAY = 24

PAGE_DAILY_SCHEMA = pa.schema([
    ("project", pa.string()),
    ("page_title", pa.string()),
    ("dt", pa.date32()),
    ("views", pa.int64()),
    ("hours_present", pa.int32()),
])


def log(msg):
    print(f"  {msg}", flush=True)


def human(n):
    for unit in ("B", "KiB", "MiB", "GiB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} TiB"


def source_hours_for(day):
    """The 24 source filename hours whose hour_start falls inside `day`."""
    hours = [f"{day:%Y-%m-%d}T{hour:02d}" for hour in range(1, 24)]
    hours.append(f"{day + dt.timedelta(days=1):%Y-%m-%d}T00")
    return hours


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", required=True, help="the DAY to compact, YYYY-MM-DD")
    ap.add_argument("--floor", type=int, default=0,
                    help="drop pages whose whole-day views are below this (0 = keep all)")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--dry-run", action="store_true",
                    help="measure and report, write and delete nothing")
    ap.add_argument("--keep-partials", action="store_true",
                    help="write the compacted object but leave the partials in place")
    ap.add_argument("--allow-incomplete", action="store_true",
                    help="compact even though fewer than 24 hours are done")
    args = ap.parse_args()

    day = dt.date.fromisoformat(args.dt)
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    manifest = session.resource("dynamodb").Table(args.table)

    print("=" * 70)
    print(f"COMPACT page_daily dt={day}")
    print("=" * 70)

    # --- is the day actually complete? ------------------------------------
    wanted = source_hours_for(day)
    log(f"a complete day needs source hours {wanted[0]} .. {wanted[-2]} plus {wanted[-1]}")
    done = []
    for key in wanted:
        item = manifest.get_item(Key={"source_hour": key}).get("Item")
        if item and item.get("status") == "done":
            done.append(key)
    missing = [h for h in wanted if h not in done]
    log(f"manifest says {len(done)}/{HOURS_PER_DAY} hours done")
    if missing:
        log(f"missing: {', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}")
        if not args.allow_incomplete:
            log("refusing to compact an incomplete day. A partial day compacted into")
            log("one object looks finished and is not -- pass --allow-incomplete if")
            log("you mean it, and hours_present will record the truth.")
            return 2

    # --- read the partials -------------------------------------------------
    prefix = f"curated/page_daily/dt={day}/"
    listed = s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix).get("Contents", [])
    partials = [o for o in listed if o["Key"].endswith(".parquet")
                and "/part-" in o["Key"]]
    if not partials:
        log(f"no partials under s3://{args.bucket}/{prefix}")
        return 2

    partial_bytes = sum(o["Size"] for o in partials)
    log(f"{len(partials)} partials, {human(partial_bytes)} total")

    totals, hours_seen = {}, {}
    started = time.time()
    for obj in partials:
        body = s3.get_object(Bucket=args.bucket, Key=obj["Key"])["Body"].read()
        table = pq.read_table(io.BytesIO(body))
        for title, views, present in zip(table.column("page_title").to_pylist(),
                                         table.column("views").to_pylist(),
                                         table.column("hours_present").to_pylist()):
            totals[title] = totals.get(title, 0) + views
            hours_seen[title] = hours_seen.get(title, 0) + present
    log(f"read and summed in {time.time() - started:.1f}s: {len(totals):,} distinct "
        f"pages, {sum(totals.values()):,} views")

    # --- apply the floor ---------------------------------------------------
    # The floor belongs HERE, not in the ingester: an hour cannot tell whether a
    # page will clear a daily threshold, so flooring per hour would drop pages
    # that qualify once the day is whole.
    if args.floor > 0:
        kept = {t: v for t, v in totals.items() if v >= args.floor}
        log(f"floor {args.floor}: {len(kept):,} of {len(totals):,} pages kept "
            f"({100 * len(kept) / len(totals):.1f}%), "
            f"{100 * sum(kept.values()) / max(sum(totals.values()), 1):.2f}% of views")
    else:
        kept = totals
        log("floor 0: every page kept")

    titles = sorted(kept)
    compacted = pa.Table.from_pydict({
        "project": ["en.wikipedia"] * len(titles),
        "page_title": titles,
        "dt": [day] * len(titles),
        "views": [kept[t] for t in titles],
        "hours_present": [hours_seen[t] for t in titles],
    }, schema=PAGE_DAILY_SCHEMA)

    buf = io.BytesIO()
    pq.write_table(compacted, buf, compression="snappy")
    compact_bytes = buf.tell()

    print()
    log(f"compacted: {compacted.num_rows:,} rows, {human(compact_bytes)}")
    log(f"was {human(partial_bytes)} across {len(partials)} partials -> "
        f"{partial_bytes / max(compact_bytes, 1):.1f}x smaller")
    log(f"projected for 730 days: {human(compact_bytes * 730)} compacted vs "
        f"{human(partial_bytes * 730)} if partials were kept")

    if args.dry_run:
        print()
        log("dry run: nothing written, nothing deleted")
        return 0

    # --- stage, delete, place ---------------------------------------------
    final_key = f"{prefix}day.parquet"
    staging_key = f"curated/_staging/page_daily/dt={day}/day.parquet"

    buf.seek(0)
    s3.put_object(Bucket=args.bucket, Key=staging_key, Body=buf.getvalue())
    log(f"staged s3://{args.bucket}/{staging_key}")

    deleted = 0
    if not args.keep_partials:
        for batch_start in range(0, len(partials), 1000):
            batch = partials[batch_start:batch_start + 1000]
            s3.delete_objects(Bucket=args.bucket, Delete={
                "Objects": [{"Key": o["Key"]} for o in batch]})
            deleted += len(batch)
        log(f"deleted {deleted} partials (partition is momentarily empty, by design)")

    s3.copy_object(Bucket=args.bucket, Key=final_key,
                   CopySource={"Bucket": args.bucket, "Key": staging_key})
    s3.delete_object(Bucket=args.bucket, Key=staging_key)
    log(f"placed s3://{args.bucket}/{final_key}")

    manifest.put_item(Item={
        "source_hour": f"day#{day}",
        "status": "compacted",
        "dt": str(day),
        "hours_done": len(done),
        "floor_applied": args.floor,
        "rows": compacted.num_rows,
        "views": sum(kept.values()),
        "bytes_compacted": compact_bytes,
        "bytes_partials_before": partial_bytes,
        "partials_deleted": deleted,
        "key_compacted": final_key,
        "compacted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    })
    log(f"manifest day row written: day#{day}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
