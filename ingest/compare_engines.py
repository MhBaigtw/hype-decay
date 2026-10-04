#!/usr/bin/env python3
"""
Prove the incremental compaction engine matches the all-at-once one on a real
day, without compacting that day.

Read-only on S3 and the manifest: downloads the day's 24 partials to a local
work directory, runs each engine in its own fresh process -- so each peak is
its own and not inherited -- writes each result locally, and compares:

  * rows and total views
  * the two tables equal row for row (same schema, same order, same values)
  * peak memory of each process

Nothing is written to S3, the day row is not touched, and the partials stay
exactly where they are for the real compaction.

The engine processes run with oom_score_adj 1000, so if the all-at-once engine
exhausts memory the kernel kills it rather than anything else on the box.

    python3 compare_engines.py --dt 2025-08-28
"""

import argparse
import concurrent.futures as futures
import datetime as dt
import io
import multiprocessing
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))


def _oom_first():
    try:
        Path("/proc/self/oom_score_adj").write_text("1000")
    except OSError:
        pass


def _peak_mib():
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        return 0.0


def run_engine(name, paths, day, out):
    import pyarrow.parquet as pq

    import compact_day

    started = time.time()
    if name == "incremental":
        bodies = (Path(p).read_bytes() for p in paths)       # one at a time
    else:
        bodies = [Path(p).read_bytes() for p in paths]       # all 24, as before
    table = compact_day.ENGINES[name](bodies, dt.date.fromisoformat(day))
    secs = time.time() - started
    pq.write_table(table, out)
    return {"engine": name, "secs": round(secs, 1), "rows": table.num_rows,
            "peak_rss_mib": round(_peak_mib(), 1)}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--dt", required=True)
    ap.add_argument("--workdir", default="/var/tmp/hype-decay-compare")
    ap.add_argument("--bucket", default="hype-decay-curated-820697996849")
    ap.add_argument("--region", default="us-east-1")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    args = ap.parse_args()

    import boto3
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    from compact_day import check_partition, source_hours_for

    day = dt.date.fromisoformat(args.dt)
    work = Path(args.workdir)
    work.mkdir(parents=True, exist_ok=True)
    s3 = boto3.Session(profile_name=args.profile, region_name=args.region).client("s3")

    prefix = f"curated/page_daily/dt={day}/"
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=args.bucket, Prefix=prefix)
            .get("Contents", [])]
    check = check_partition(keys, day)
    print(f"{day}: {24 - len(check['missing'])}/24 partials, "
          f"day.parquet present: {check['has_day_object']}")
    if check["missing"] or check["has_day_object"]:
        print("not a clean uncompacted day; refusing to compare")
        return 2

    paths = []
    for hour in source_hours_for(day):
        local = work / f"part-{hour}.parquet"
        s3.download_file(args.bucket, f"{prefix}part-{hour}.parquet", str(local))
        paths.append(str(local))
    print(f"downloaded {sum(Path(p).stat().st_size for p in paths) / 2**20:,.0f} MiB "
          f"to {work} (read-only)")

    ctx = multiprocessing.get_context("spawn")
    results = {}
    for name in ("incremental", "arrow"):
        out = work / f"{name}.parquet"
        with futures.ProcessPoolExecutor(1, mp_context=ctx, initializer=_oom_first) as pool:
            try:
                results[name] = pool.submit(run_engine, name, paths, str(day), str(out)).result()
            except futures.process.BrokenProcessPool:
                print(f"{name}: process died (killed by the kernel, most likely OOM)")
                return 3
        r = results[name]
        print(f"{name:<12} {r['secs']:6.1f}s  {r['rows']:,} rows  peak RSS {r['peak_rss_mib']:,.0f} MiB")

    a = pq.read_table(work / "incremental.parquet")
    b = pq.read_table(work / "arrow.parquet")
    checks = {
        "same schema": a.schema == b.schema,
        "same rows": a.num_rows == b.num_rows,
        "same views": pc.sum(a["views"]).as_py() == pc.sum(b["views"]).as_py(),
        "same hours_present": (pc.sum(a["hours_present"]).as_py()
                               == pc.sum(b["hours_present"]).as_py()),
        "tables equal row for row": a.equals(b),
    }
    print(f"rows {a.num_rows:,} vs {b.num_rows:,}; views "
          f"{pc.sum(a['views']).as_py():,} vs {pc.sum(b['views']).as_py():,}")
    for name, ok in checks.items():
        print(f"  [{'ok' if ok else 'FAIL'}] {name}")
    for p in list(work.glob("*.parquet")):
        p.unlink()
    verdict = all(checks.values())
    print("IDENTICAL" if verdict else "DIFFERENT")
    return 0 if verdict else 1


if __name__ == "__main__":
    sys.exit(main())
