#!/usr/bin/env python3
"""
Compact one day of page_daily: 24 hourly partials become one object.

WHY THIS EXISTS. The ingester processes one hour at a time, so it writes one
page_daily partial per hour -- 24 per day, each carrying every page seen in that
hour. Measured on the fixture hour a partial is 22.7 MiB, so keeping them for the
whole window would be about 389 GiB of page_daily against 37.7 GiB of page_hour:
the partials, not the data, would be the storage bill. Compaction is what makes
the curated zone affordable, and it is also where a daily-views floor is applied,
because a floor is meaningless until a day is whole.

WHICH HOURS MAKE A DAY. Not the obvious ones. hour_start = source filename hour
minus 1, so the hours whose hour_start lands inside day D are source hours

    D T01 .. D T23, and (D+1) T00

That last one is the trap: the file named 2026-09-11-000000.gz holds
2026-09-10 23:00-24:00, so a day is not complete until the next day has started.

CRASH SAFETY. Deleting 24 objects and writing 1 is not atomic, and the dangerous
window is between the delete and the placement: crash there and the day is gone
from S3 entirely. So the order is

    1. write the compacted object to a STAGING key
    2. write a manifest day row: status=compacting, holding the staging key
    3. delete the partials
    4. copy staging -> final
    5. delete staging, mark the row compacted

A re-run that finds status=compacting does not re-read partials -- they may be
gone. It finishes placement from the staging key the row recorded. That is the
whole point of writing the row before the first delete: the recovery information
outlives the data it describes.

The order also means the partition is briefly EMPTY rather than briefly
DOUBLE-COUNTED. An empty partition is a visible gap; a double count is a silent
wrong answer. The manifest day row is the authority either way.

    python3 compact_day.py --dt 2026-09-10 --dry-run
    python3 compact_day.py --dt 2026-09-10 --engine both
    python3 compact_day.py --dt 2026-09-10 --crash-after-delete   # test recovery
"""

import argparse
import datetime as dt
import io
import os
import sys
import time

import boto3
import pyarrow as pa
import pyarrow.compute as pc
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


def peak_rss_mib():
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        return 0.0


def source_hours_for(day):
    """The 24 source filename hours whose hour_start falls inside `day`."""
    hours = [f"{day:%Y-%m-%d}T{hour:02d}" for hour in range(1, 24)]
    hours.append(f"{day + dt.timedelta(days=1):%Y-%m-%d}T00")
    return hours


def s3_exists(s3, bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


# --- aggregation engines ---------------------------------------------------

def aggregate_python(bodies, day):
    """The original dict loop. Kept only as the baseline for the comparison."""
    totals, hours_seen = {}, {}
    for body in bodies:
        table = pq.read_table(io.BytesIO(body))
        for title, views, present in zip(table.column("page_title").to_pylist(),
                                         table.column("views").to_pylist(),
                                         table.column("hours_present").to_pylist()):
            totals[title] = totals.get(title, 0) + views
            hours_seen[title] = hours_seen.get(title, 0) + present
    titles = sorted(totals)
    return pa.Table.from_pydict({
        "project": ["en.wikipedia"] * len(titles),
        "page_title": titles,
        "dt": [day] * len(titles),
        "views": [totals[t] for t in titles],
        "hours_present": [hours_seen[t] for t in titles],
    }, schema=PAGE_DAILY_SCHEMA)


def aggregate_arrow(bodies, day):
    """concat_tables then a single grouped aggregate: no Python-level loop."""
    tables = [pq.read_table(io.BytesIO(body)) for body in bodies]
    combined = pa.concat_tables(tables)
    grouped = combined.group_by("page_title").aggregate([
        ("views", "sum"), ("hours_present", "sum")])
    grouped = grouped.sort_by("page_title")
    rows = grouped.num_rows
    return pa.Table.from_arrays([
        pa.array(["en.wikipedia"] * rows, pa.string()),
        grouped.column("page_title").combine_chunks().cast(pa.string()),
        pa.array([day] * rows, pa.date32()),
        grouped.column("views_sum").combine_chunks().cast(pa.int64()),
        grouped.column("hours_present_sum").combine_chunks().cast(pa.int32()),
    ], schema=PAGE_DAILY_SCHEMA)


ENGINES = {"arrow": aggregate_arrow, "python": aggregate_python}


# --- recovery --------------------------------------------------------------

def finish_placement(s3, manifest, bucket, row, day):
    """Complete a compaction that died between the delete and the copy."""
    staging_key = row.get("key_staging")
    final_key = row.get("key_compacted")
    log(f"found an interrupted compaction: status=compacting")
    log(f"  staging : {staging_key}")
    log(f"  final   : {final_key}")

    if final_key and s3_exists(s3, bucket, final_key):
        log("the final object is already in place; the copy had completed")
    elif staging_key and s3_exists(s3, bucket, staging_key):
        log("final missing, staging present: finishing the copy from staging")
        s3.copy_object(Bucket=bucket, Key=final_key,
                       CopySource={"Bucket": bucket, "Key": staging_key})
        log(f"placed s3://{bucket}/{final_key}")
    else:
        log("BOTH the staging and final objects are missing. The partials were")
        log("deleted and the compacted copy is gone: this day must be re-ingested.")
        manifest.update_item(
            Key={"source_hour": f"day#{day}"},
            UpdateExpression="SET #s = :failed, error = :why",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":failed": "failed",
                ":why": "compacting interrupted and both staging and final are absent"},
        )
        return 1

    if staging_key and s3_exists(s3, bucket, staging_key):
        s3.delete_object(Bucket=bucket, Key=staging_key)
        log("staging object removed")

    manifest.update_item(
        Key={"source_hour": f"day#{day}"},
        UpdateExpression="SET #s = :done, recovered_at = :now REMOVE key_staging",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":done": "compacted",
            ":now": dt.datetime.now(dt.timezone.utc).isoformat()},
    )
    log("manifest day row marked compacted -- recovery complete")
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", required=True, help="the DAY to compact, YYYY-MM-DD")
    ap.add_argument("--floor", type=int, default=0,
                    help="drop pages whose whole-day views are below this (0 = keep all)")
    ap.add_argument("--engine", choices=["arrow", "python", "both"], default="arrow",
                    help="both times each engine and cross-checks them")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--dry-run", action="store_true",
                    help="measure and report, write and delete nothing")
    ap.add_argument("--keep-partials", action="store_true")
    ap.add_argument("--allow-incomplete", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="recompact a day the manifest already calls compacted")
    ap.add_argument("--crash-after-delete", action="store_true",
                    help="TEST HOOK: exit hard after deleting partials, before the "
                         "copy, to exercise the recovery path")
    args = ap.parse_args()

    day = dt.date.fromisoformat(args.dt)
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    manifest = session.resource("dynamodb").Table(args.table)
    day_key = f"day#{day}"

    print("=" * 72)
    print(f"COMPACT page_daily dt={day}")
    print("=" * 72)

    # --- resume or refuse before touching anything ------------------------
    row = manifest.get_item(Key={"source_hour": day_key}).get("Item")
    if row and row.get("status") == "compacting":
        return finish_placement(s3, manifest, args.bucket, row, day)
    if row and row.get("status") == "compacted" and not args.force:
        log(f"NO-OP: already compacted at {row.get('compacted_at')}, "
            f"{int(row.get('rows', 0)):,} rows, "
            f"{human(int(row.get('bytes_compacted', 0)))}")
        log("Pass --force to redo it.")
        return 0

    # --- is the day actually complete? ------------------------------------
    wanted = source_hours_for(day)
    log(f"a complete day needs {wanted[0]} .. {wanted[-2]} plus {wanted[-1]}")
    done = [k for k in wanted
            if (manifest.get_item(Key={"source_hour": k}).get("Item") or {}
                ).get("status") == "done"]
    missing = [h for h in wanted if h not in done]
    log(f"manifest says {len(done)}/{HOURS_PER_DAY} hours done")
    if missing:
        log(f"missing: {', '.join(missing[:8])}{' ...' if len(missing) > 8 else ''}")
        if not args.allow_incomplete:
            log("refusing to compact an incomplete day: a partial day compacted into")
            log("one object looks finished and is not. Pass --allow-incomplete if you")
            log("mean it, and hours_present will record the truth.")
            return 2

    # --- read the partials -------------------------------------------------
    prefix = f"curated/page_daily/dt={day}/"
    listed = s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix).get("Contents", [])
    partials = [o for o in listed if o["Key"].endswith(".parquet") and "/part-" in o["Key"]]
    if not partials:
        log(f"no partials under s3://{args.bucket}/{prefix}")
        return 2
    partial_bytes = sum(o["Size"] for o in partials)
    log(f"{len(partials)} partials, {human(partial_bytes)} total")

    started = time.time()
    bodies = [s3.get_object(Bucket=args.bucket, Key=o["Key"])["Body"].read()
              for o in partials]
    log(f"downloaded partials in {time.time() - started:.1f}s")

    # --- aggregate, timing each engine asked for --------------------------
    timings, results = {}, {}
    for name in (["arrow", "python"] if args.engine == "both" else [args.engine]):
        started = time.time()
        results[name] = ENGINES[name](bodies, day)
        timings[name] = time.time() - started
        log(f"{name:<6} aggregate: {timings[name]:6.2f}s, "
            f"{results[name].num_rows:,} rows")

    if args.engine == "both":
        arrow_t, python_t = results["arrow"], results["python"]
        same_rows = arrow_t.num_rows == python_t.num_rows
        same_views = (pc.sum(arrow_t.column("views")).as_py()
                      == pc.sum(python_t.column("views")).as_py())
        same_hours = (pc.sum(arrow_t.column("hours_present")).as_py()
                      == pc.sum(python_t.column("hours_present")).as_py())
        log(f"cross-check: rows {'match' if same_rows else 'DIFFER'}, "
            f"views {'match' if same_views else 'DIFFER'}, "
            f"hours_present {'match' if same_hours else 'DIFFER'}")
        if not (same_rows and same_views and same_hours):
            log("the two engines disagree; refusing to write either result")
            return 1
        log(f"speedup: python {timings['python']:.2f}s -> arrow "
            f"{timings['arrow']:.2f}s = {timings['python'] / timings['arrow']:.1f}x")

    compacted = results.get("arrow") or results[args.engine]

    # --- apply the floor ---------------------------------------------------
    total_views = pc.sum(compacted.column("views")).as_py()
    if args.floor > 0:
        keep = pc.greater_equal(compacted.column("views"), args.floor)
        floored = compacted.filter(keep)
        log(f"floor {args.floor}: {floored.num_rows:,} of {compacted.num_rows:,} pages "
            f"kept ({100 * floored.num_rows / compacted.num_rows:.1f}%), "
            f"{100 * pc.sum(floored.column('views')).as_py() / total_views:.2f}% of views")
        compacted = floored
    else:
        log("floor 0: every page kept")

    buf = io.BytesIO()
    pq.write_table(compacted, buf, compression="snappy")
    compact_bytes = buf.tell()

    print()
    log(f"compacted: {compacted.num_rows:,} rows, {human(compact_bytes)}")
    log(f"was {human(partial_bytes)} across {len(partials)} partials -> "
        f"{partial_bytes / max(compact_bytes, 1):.1f}x smaller")
    log(f"projected 730 days: {human(compact_bytes * 730)} compacted vs "
        f"{human(partial_bytes * 730)} if partials were kept")
    if peak_rss_mib():
        log(f"peak RSS this process: {peak_rss_mib():,.0f} MiB")

    if args.dry_run:
        print()
        log("dry run: nothing written, nothing deleted")
        return 0

    # --- stage, record, delete, place -------------------------------------
    final_key = f"{prefix}day.parquet"
    staging_key = f"curated/_staging/page_daily/dt={day}/day.parquet"

    buf.seek(0)
    s3.put_object(Bucket=args.bucket, Key=staging_key, Body=buf.getvalue())
    log(f"staged s3://{args.bucket}/{staging_key}")

    # The recovery row goes in BEFORE the first delete, so the information
    # needed to finish outlives the partials it replaces.
    manifest.put_item(Item={
        "source_hour": day_key,
        "status": "compacting",
        "dt": str(day),
        "hours_done": len(done),
        "floor_applied": args.floor,
        "rows": compacted.num_rows,
        "views": pc.sum(compacted.column("views")).as_py(),
        "bytes_compacted": compact_bytes,
        "bytes_partials_before": partial_bytes,
        "partials_total": len(partials),
        "engine": "arrow",
        "key_staging": staging_key,
        "key_compacted": final_key,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    })
    log("manifest day row written: status=compacting, staging key recorded")

    deleted = 0
    if not args.keep_partials:
        for start in range(0, len(partials), 1000):
            batch = partials[start:start + 1000]
            s3.delete_objects(Bucket=args.bucket, Delete={
                "Objects": [{"Key": o["Key"]} for o in batch]})
            deleted += len(batch)
        log(f"deleted {deleted} partials (partition momentarily empty, by design)")

    if args.crash_after_delete:
        log("TEST HOOK: exiting hard between the delete and the copy.")
        log("The day now exists ONLY at the staging key, and only the manifest")
        log("row knows where that is. Re-run this command to recover.")
        sys.stdout.flush()
        os._exit(9)

    s3.copy_object(Bucket=args.bucket, Key=final_key,
                   CopySource={"Bucket": args.bucket, "Key": staging_key})
    s3.delete_object(Bucket=args.bucket, Key=staging_key)
    log(f"placed s3://{args.bucket}/{final_key}")

    manifest.update_item(
        Key={"source_hour": day_key},
        UpdateExpression=("SET #s = :done, compacted_at = :now, "
                          "partials_deleted = :deleted REMOVE key_staging"),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":done": "compacted",
            ":now": dt.datetime.now(dt.timezone.utc).isoformat(),
            ":deleted": deleted},
    )
    log(f"manifest day row marked compacted: {day_key}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
