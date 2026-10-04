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
outlives the data it describes. Staging is deleted only after the copy, so a
staging object that still exists always wins over whatever the final key holds.

WHAT A DAY IS REBUILT FROM. Exactly the 24 expected partials, by key, and never
a compacted object sitting beside them. The manifest saying an hour is done is
not enough on its own: after compaction every hour is still done and its partial
is gone. A day the ingester INVALIDATED (a forced re-ingest into a compacted
day, see ingest_hour.guard_day) is rebuilt like a fresh one, and the result is
checked against the totals the invalidation recorded.

REFLOOR. --force on a compacted day re-reads day.parquet and applies a HIGHER
floor. That needs no partials, because raising a floor only drops rows. Lowering
one is refused: the rows under the old floor no longer exist anywhere but the
source, so that is a re-ingest.

THE FLOOR. Default 10, applied here and nowhere else (SPEC). See NOTES, Task 3,
for the bytes it saves and the baseline zero-fill it obliges.

The order also means the partition is briefly EMPTY rather than briefly
DOUBLE-COUNTED. An empty partition is a visible gap; a double count is a silent
wrong answer. The manifest day row is the authority either way.

    python3 compact_day.py --dt 2026-09-10 --dry-run
    python3 compact_day.py --dt 2026-09-10 --engine arrow       # old all-at-once engine
    python3 compact_day.py --dt 2026-09-10 --crash-after-delete   # test recovery
    python3 compact_day.py --dt 2026-09-10 --force --floor 10      # refloor
"""

import argparse
import datetime as dt
import io
import json
import os
import sys
import time
from collections import Counter

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

DEFAULT_BUCKET = "hype-decay-curated-820697996849"
DEFAULT_TABLE = "hype-decay-manifest"
DEFAULT_REGION = "us-east-1"

HOURS_PER_DAY = 24

# S3 requests sent by this process, by operation, reported in the RESULT line.
S3_REQUESTS = Counter()

# SPEC: the daily-views floor is applied at compaction and nowhere else.
DEFAULT_FLOOR = 10

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


def partial_key(day, source_hour):
    """Where ingest_hour.py writes the page_daily partial for one source hour."""
    return f"curated/page_daily/dt={day}/part-{source_hour}.parquet"


def check_partition(keys, day):
    """Compares what is in the partition against the 24 partials a day needs."""
    expected = {partial_key(day, h): h for h in source_hours_for(day)}
    present = set(keys)
    return {
        "missing": [h for k, h in expected.items() if k not in present],
        "unexpected": sorted(k for k in present
                             if "/part-" in k and k not in expected),
        "has_day_object": f"curated/page_daily/dt={day}/day.parquet" in present,
    }


def refloor_allowed(previous_floor, new_floor):
    """Raising a floor drops rows; lowering one needs rows that are gone."""
    return new_floor >= previous_floor


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


def aggregate_incremental(bodies, day):
    """Folds one partial at a time into a running aggregate. The default.

    aggregate_arrow concatenates all 24 partials -- about 42 million rows --
    and groups them in one go, and on the backfill box that peaked between 5.6
    and 7.1 GiB depending on the day, against 7.6 GiB usable. Run 1 of the
    backfill died in exactly that step. Here the most ever held is the running
    total (one row per page seen so far, about 7 million by the end of a day)
    plus a single hour, so the peak tracks distinct pages, not 24x them.

    `bodies` may be any iterable, so the caller can fetch each partial only
    when it is needed. The result is identical to aggregate_arrow: same
    schema, same rows, same order -- test_compaction_engines.py checks that,
    and compare_engines.py checked it on a real day.
    """
    running = None
    for body in bodies:
        hour = pq.read_table(io.BytesIO(body), columns=["page_title", "views",
                                                        "hours_present"])
        hour = pa.table({
            "page_title": hour.column("page_title"),
            "views": hour.column("views").cast(pa.int64()),
            "hours_present": hour.column("hours_present").cast(pa.int64()),
        })
        both = hour if running is None else pa.concat_tables([running, hour])
        grouped = both.group_by("page_title").aggregate([
            ("views", "sum"), ("hours_present", "sum")])
        running = pa.table({
            "page_title": grouped.column("page_title"),
            "views": grouped.column("views_sum"),
            "hours_present": grouped.column("hours_present_sum"),
        }).combine_chunks()
        del hour, both, grouped
    if running is None:
        running = pa.table({"page_title": pa.array([], pa.string()),
                            "views": pa.array([], pa.int64()),
                            "hours_present": pa.array([], pa.int64())})

    running = running.sort_by("page_title")
    rows = running.num_rows
    return pa.Table.from_arrays([
        pa.array(["en.wikipedia"] * rows, pa.string()),
        running.column("page_title").combine_chunks().cast(pa.string()),
        pa.array([day] * rows, pa.date32()),
        running.column("views").combine_chunks().cast(pa.int64()),
        running.column("hours_present").combine_chunks().cast(pa.int32()),
    ], schema=PAGE_DAILY_SCHEMA)


ENGINES = {"incremental": aggregate_incremental, "arrow": aggregate_arrow,
           "python": aggregate_python}


# --- recovery --------------------------------------------------------------

def finish_placement(s3, manifest, bucket, row, day):
    """Complete a compaction that died between the delete and the copy."""
    staging_key = row.get("key_staging")
    final_key = row.get("key_compacted")
    log(f"found an interrupted compaction: status=compacting")
    log(f"  staging : {staging_key}")
    log(f"  final   : {final_key}")

    # Staging first. It is deleted only AFTER the copy, so if it still exists the
    # copy may not have happened -- and on a refloor the final key exists the
    # whole time, holding the OLD day. Checking the final key first would
    # declare that stale object finished.
    if staging_key and s3_exists(s3, bucket, staging_key):
        log("staging present: copying it into place (idempotent)")
        s3.copy_object(Bucket=bucket, Key=final_key,
                       CopySource={"Bucket": bucket, "Key": staging_key})
        log(f"placed s3://{bucket}/{final_key}")
    elif final_key and s3_exists(s3, bucket, final_key):
        log("staging gone, final present: the copy had completed")
    else:
        log("BOTH the staging and final objects are missing. The partials were")
        log("deleted and the compacted copy is gone: this day must be re-ingested.")
        manifest.update_item(
            Key={"source_hour": f"day#{day}"},
            UpdateExpression="SET #s = :failed, #e = :why",   # error is reserved
            ExpressionAttributeNames={"#s": "status", "#e": "error"},
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


def place(s3, manifest, args, day, compacted, row_fields, partials=()):
    """Stage, record, delete partials, place, mark compacted. The crash-safe core.

    Shared by a rebuild from partials and a refloor of day.parquet: both end in
    one new object at the final key, and both must survive dying at any line.
    """
    final_key = f"curated/page_daily/dt={day}/day.parquet"
    staging_key = f"curated/_staging/page_daily/dt={day}/day.parquet"

    buf = io.BytesIO()
    pq.write_table(compacted, buf, compression="snappy")
    s3.put_object(Bucket=args.bucket, Key=staging_key, Body=buf.getvalue())
    log(f"staged s3://{args.bucket}/{staging_key}")

    # The recovery row goes in BEFORE the first delete, so the information
    # needed to finish outlives the partials it replaces.
    manifest.put_item(Item={
        **row_fields,
        "source_hour": f"day#{day}",
        "status": "compacting",
        "dt": str(day),
        "floor_applied": args.floor,
        "rows": compacted.num_rows,
        "views": pc.sum(compacted.column("views")).as_py() or 0,
        "bytes_compacted": buf.tell(),
        "engine": args.engine,
        "key_staging": staging_key,
        "key_compacted": final_key,
        "started_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    })
    log("manifest day row written: status=compacting, staging key recorded")

    deleted = 0
    if partials and not args.keep_partials:
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
        Key={"source_hour": f"day#{day}"},
        UpdateExpression=("SET #s = :done, compacted_at = :now, "
                          "partials_deleted = :deleted REMOVE key_staging"),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":done": "compacted",
            ":now": dt.datetime.now(dt.timezone.utc).isoformat(),
            ":deleted": deleted},
    )
    log(f"manifest day row marked compacted: day#{day}")


def apply_floor(compacted, floor):
    total = pc.sum(compacted.column("views")).as_py() or 0
    if floor <= 0:
        log("floor 0: every page kept")
        return compacted
    floored = compacted.filter(pc.greater_equal(compacted.column("views"), floor))
    kept_views = pc.sum(floored.column("views")).as_py() or 0
    log(f"floor {floor}: {floored.num_rows:,} of {compacted.num_rows:,} pages "
        f"kept ({100 * floored.num_rows / max(compacted.num_rows, 1):.1f}%), "
        f"{kept_views:,} of {total:,} views ({100 * kept_views / max(total, 1):.2f}%)")
    return floored


def report_size(compacted, was_bytes, was_label):
    buf = io.BytesIO()
    pq.write_table(compacted, buf, compression="snappy")
    size = buf.tell()
    print()
    log(f"compacted: {compacted.num_rows:,} rows, "
        f"{pc.sum(compacted.column('views')).as_py() or 0:,} views, {human(size)}")
    log(f"was {human(was_bytes)} {was_label} -> {was_bytes / max(size, 1):.1f}x smaller")
    log(f"projected 730 days: {human(size * 730)}")
    if peak_rss_mib():
        log(f"peak RSS this process: {peak_rss_mib():,.0f} MiB")
    return size


def refloor(s3, manifest, args, day, row):
    """--force on a compacted day: re-read day.parquet, apply a higher floor."""
    previous = int(row.get("floor_applied", 0))
    log(f"REFLOOR: day is compacted at floor {previous}, asked for floor {args.floor}")
    if not refloor_allowed(previous, args.floor):
        log(f"refusing: rows under floor {previous} were dropped at compaction and")
        log("exist nowhere but the source. Lowering the floor is a re-ingest.")
        return 2

    final_key = f"curated/page_daily/dt={day}/day.parquet"
    body = s3.get_object(Bucket=args.bucket, Key=final_key)["Body"].read()
    current = pq.read_table(io.BytesIO(body))
    current_views = pc.sum(current.column("views")).as_py() or 0
    log(f"read {human(len(body))}, {current.num_rows:,} rows, {current_views:,} views")

    compacted = apply_floor(current, args.floor)
    report_size(compacted, len(body), f"at floor {previous}")
    if args.dry_run:
        print()
        log("dry run: nothing written")
        return 0

    # put_item replaces the row, so provenance from the original compaction --
    # and from a rebuild, if there was one -- has to be carried explicitly.
    carried = {k: row[k] for k in ("hours_done", "bytes_partials_before",
                                   "partials_total", "rows_unfloored",
                                   "views_unfloored", "rebuilt_after",
                                   "rows_previous", "views_previous") if k in row}
    place(s3, manifest, args, day, compacted, {
        **carried,
        "refloored_from": previous,
        "rows_before_refloor": current.num_rows,
        "views_before_refloor": current_views,
        "bytes_before_refloor": len(body),
    })
    return 0


def _main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", required=True, help="the DAY to compact, YYYY-MM-DD")
    ap.add_argument("--floor", type=int, default=DEFAULT_FLOOR,
                    help=f"drop pages whose whole-day views are below this "
                         f"(default {DEFAULT_FLOOR}; 0 = keep all)")
    ap.add_argument("--engine", choices=sorted(ENGINES), default="incremental",
                    help="incremental (default) holds one hour at a time; arrow and "
                         "python load all 24 and are kept for comparison only "
                         "(see compare_engines.py)")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--dry-run", action="store_true",
                    help="measure and report, write and delete nothing")
    ap.add_argument("--keep-partials", action="store_true")
    ap.add_argument("--allow-incomplete", action="store_true")
    ap.add_argument("--force", action="store_true",
                    help="on a compacted day: refloor day.parquet at a higher --floor")
    ap.add_argument("--crash-after-delete", action="store_true",
                    help="TEST HOOK: exit hard after deleting partials, before the "
                         "copy, to exercise the recovery path")
    args = ap.parse_args()

    day = dt.date.fromisoformat(args.dt)
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    s3 = session.client("s3")
    s3.meta.events.register(
        "before-send.s3",
        lambda event_name=None, **_: S3_REQUESTS.update([event_name.rsplit(".", 1)[-1]]))
    manifest = session.resource("dynamodb").Table(args.table)
    day_key = f"day#{day}"

    print("=" * 72)
    print(f"COMPACT page_daily dt={day}")
    print("=" * 72)

    # --- resume, refloor or refuse before touching anything ---------------
    row = manifest.get_item(Key={"source_hour": day_key}).get("Item") or {}
    status = row.get("status")
    if status == "compacting":
        return finish_placement(s3, manifest, args.bucket, row, day)
    if status == "compacted":
        if args.force:
            return refloor(s3, manifest, args, day, row)
        log(f"NO-OP: already compacted at {row.get('compacted_at')}, "
            f"floor {int(row.get('floor_applied', 0))}, "
            f"{int(row.get('rows', 0)):,} rows, "
            f"{human(int(row.get('bytes_compacted', 0)))}")
        log("Pass --force with a higher --floor to refloor it.")
        return 0
    if status == "invalidated":
        log(f"day was INVALIDATED by a forced re-ingest of {row.get('invalidated_by')} "
            f"at {row.get('invalidated_at')}")
        log(f"  it held {int(row.get('rows_previous', 0)):,} rows, "
            f"{int(row.get('views_previous', 0)):,} views at floor "
            f"{int(row.get('floor_applied', 0))}; rebuilding from partials")

    # --- is the day actually complete? ------------------------------------
    wanted = source_hours_for(day)
    log(f"a complete day needs {wanted[0]} .. {wanted[-2]} plus {wanted[-1]}")
    done = [k for k in wanted
            if (manifest.get_item(Key={"source_hour": k}).get("Item") or {}
                ).get("status") == "done"]
    log(f"manifest says {len(done)}/{HOURS_PER_DAY} hours done")

    prefix = f"curated/page_daily/dt={day}/"
    listed = []
    for page in s3.get_paginator("list_objects_v2").paginate(
            Bucket=args.bucket, Prefix=prefix):
        listed.extend(page.get("Contents", []))
    check = check_partition([o["Key"] for o in listed], day)
    log(f"partition holds {HOURS_PER_DAY - len(check['missing'])}/{HOURS_PER_DAY} "
        f"expected partials")

    # A compacted object beside partials means the partition already reads as
    # double. Compacting would bury that; stop and make it visible instead.
    if check["has_day_object"]:
        log("REFUSING: day.parquet is sitting beside partials, so this partition")
        log("currently double-counts. The day row should have been invalidated and")
        log("the object quarantined; investigate before compacting.")
        return 2
    if check["unexpected"]:
        log(f"REFUSING: partials that do not belong to this day: {check['unexpected']}")
        return 2

    # Both tests, because they fail differently: the manifest says done for
    # every hour of a compacted day, whose partials are all gone.
    missing = sorted(set(check["missing"]) | {h for h in wanted if h not in done})
    if missing:
        log(f"missing ({len(missing)}): {', '.join(missing[:8])}"
            f"{' ...' if len(missing) > 8 else ''}")
        if not args.allow_incomplete:
            log("refusing to compact an incomplete day: a partial day compacted into")
            log("one object looks finished and is not. Pass --allow-incomplete if you")
            log("mean it, and hours_present will record the truth.")
            return 2

    partials = [o for o in listed if "/part-" in o["Key"]]
    if not partials:
        log(f"no partials under s3://{args.bucket}/{prefix}")
        return 2
    partial_bytes = sum(o["Size"] for o in partials)
    log(f"{len(partials)} partials, {human(partial_bytes)} total")

    # Fetched lazily: the incremental engine pulls each partial only when it
    # folds it in, so the 24 bodies are never in memory together.
    def fetch():
        for o in partials:
            yield s3.get_object(Bucket=args.bucket, Key=o["Key"])["Body"].read()

    started = time.time()
    bodies = fetch() if args.engine == "incremental" else list(fetch())
    compacted = ENGINES[args.engine](bodies, day)
    log(f"{args.engine} aggregate (download included): {time.time() - started:6.2f}s, "
        f"{compacted.num_rows:,} rows, peak RSS so far {peak_rss_mib():,.0f} MiB")
    unfloored_rows = compacted.num_rows
    unfloored_views = pc.sum(compacted.column("views")).as_py() or 0

    # The double-count check. Before any floor, a rebuilt day must reproduce the
    # day it replaced: same pages, same views. More means an hour was counted
    # twice; fewer means one was lost. Only comparable when the invalidated day
    # was itself unfloored.
    if status == "invalidated" and int(row.get("floor_applied", 0)) == 0:
        before_rows = int(row.get("rows_previous", 0))
        before_views = int(row.get("views_previous", 0))
        same = before_rows == unfloored_rows and before_views == unfloored_views
        log(f"rebuild vs invalidated day: rows {before_rows:,} -> {unfloored_rows:,}, "
            f"views {before_views:,} -> {unfloored_views:,} -- "
            + ("UNCHANGED" if same else "CHANGED"))

    compacted = apply_floor(compacted, args.floor)
    report_size(compacted, partial_bytes, f"across {len(partials)} partials")

    if args.dry_run:
        print()
        log("dry run: nothing written, nothing deleted")
        return 0

    place(s3, manifest, args, day, compacted, {
        "hours_done": len(done),
        "bytes_partials_before": partial_bytes,
        "partials_total": len(partials),
        "rows_unfloored": unfloored_rows,
        "views_unfloored": unfloored_views,
        **({"rebuilt_after": row.get("invalidated_by"),
            "rows_previous": row.get("rows_previous"),
            "views_previous": row.get("views_previous")}
           if status == "invalidated" else {}),
    }, partials)

    # The quarantined copy was kept only until a rebuild succeeded.
    quarantined = row.get("key_quarantine")
    if quarantined and s3_exists(s3, args.bucket, quarantined):
        s3.delete_object(Bucket=args.bucket, Key=quarantined)
        log(f"removed the quarantined copy s3://{args.bucket}/{quarantined}")
    return 0


def main():
    """Runs a compaction and ends with one machine-readable RESULT line, which
    backfill.py reads: it runs this as a subprocess so the ~5.4 GiB a day's
    compaction peaks at goes back to the OS the moment it exits."""
    # backfill.py sets this so that, under memory pressure, the kernel kills a
    # compaction before the runner or the wrapper (see OOM_SCORE_* there).
    if os.environ.get("HYPE_DECAY_OOM_SCORE"):
        try:
            with open("/proc/self/oom_score_adj", "w") as f:
                f.write(os.environ["HYPE_DECAY_OOM_SCORE"])
        except OSError:
            pass
    started = time.time()
    code = _main()
    print("RESULT " + json.dumps({
        "exit": code, "secs": round(time.time() - started, 1),
        "peak_rss_mib": round(peak_rss_mib(), 1),
        "s3_requests": dict(S3_REQUESTS)}), flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
