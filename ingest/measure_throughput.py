#!/usr/bin/env python3
"""
Size the backfill by measuring a sample of hours, not by guessing.

Answers what Task 3 design needs:

  * wall-clock for all 17,520 hours
  * whether the binding constraint is the connection cap or the processing
  * peak memory per parse worker, which decides how many workers fit in RAM
  * instance cost at that duration

MEASURED IN TWO STAGES, ON PURPOSE. Download and parse are timed separately
rather than as one pipeline, because a single end-to-end number cannot tell you
which stage to fix. The overlapped wall clock is then derived as

    max(total_download_time, total_parse_time / parse_workers)

which is what a pipelined runner would actually achieve.

Downloads run in THREADS capped at 3, because the limit is network politeness:
Wikimedia allows 3 connections per IP and blocks clients that evade it, and a
mirror run by a university deserves the same restraint.

Parsing runs in PROCESSES, because it is CPU-bound and Python threads would
serialise on the GIL. Each process reports its own peak RSS, which is the only
way to get per-worker memory -- threads share an address space and cannot be
measured apart.

Laptop reference figures (2026-09-27):
    origin dumps.wikimedia.org   4.91 MiB/s   one connection
    your.org mirror             53.22 MiB/s   one connection, bytes identical
    pyarrow parse                4.3 s/file, python parse 21.9 s/file

Nothing is written to S3 and the manifest is never touched: this measures, it
does not ingest.

    python3 measure_throughput.py --hours 20 --download-workers 3 --parse-workers 4
"""

import argparse
import concurrent.futures as futures
import datetime as dt
import gzip
import hashlib
import os
import statistics
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path

SOURCES = {
    "your.org": "https://dumps.wikimedia.your.org/other/pageviews",
    "umu": "https://ftp.acc.umu.se/mirror/wikimedia.org/other/pageviews",
    "origin": "https://dumps.wikimedia.org/other/pageviews",
}

USER_AGENT = "hype-decay-measure/0.1 (MhBaig971@gmail.com)"

# Wikimedia allows 3 per IP. Mirrors publish no number, so the same cap applies
# out of courtesy. This is a hard ceiling in this script, not a default.
MAX_CONNECTIONS = 3

TOTAL_HOURS = 17520
MEASURED_WINDOW_GIB = 939.4 # Task 0, measured from the dump listings

# us-east-1 on-demand, read from the AWS Pricing API on 2026-09-29.
INSTANCE_HOURLY_USD = {
    "c7g.medium": 0.0363, "c7g.large": 0.0725, "c7g.xlarge": 0.1450,
    "c8g.large": 0.0798, "c8g.xlarge": 0.1595, "t4g.small": 0.0168,
}
EBS_GP3_USD_PER_GIB_MONTH = 0.08


def peak_rss_mib():
    """Peak resident set of THIS process, in MiB.

    resource is Unix-only and ru_maxrss is KiB on Linux; the instance runs
    Linux, so that is the path that matters. tracemalloc is the fallback for
    development on Windows and undercounts, because it sees Python allocations
    only and not pyarrow buffers.
    """
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    except ImportError:
        import tracemalloc
        if tracemalloc.is_tracing():
            return tracemalloc.get_traced_memory()[1] / 2 ** 20
        return 0.0


def source_url(base, when):
    return (f"{base}/{when:%Y}/{when:%Y-%m}/"
            f"pageviews-{when:%Y%m%d}-{when:%H}0000.gz")


def hours_ending(last, count):
    return [last - dt.timedelta(hours=offset) for offset in range(count - 1, -1, -1)]


def download_one(args):
    """Fetch one hour to disk, hashing as it goes. Runs in a thread."""
    when, base, workdir = args
    url = source_url(base, when)
    path = Path(workdir) / f"pageviews-{when:%Y%m%d}-{when:%H}0000.gz"
    digest = hashlib.sha256()
    started = time.time()
    try:
        request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
        with urllib.request.urlopen(request, timeout=180) as response, \
                open(path, "wb") as out:
            while True:
                chunk = response.read(1024 * 256)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
    except (urllib.error.HTTPError, urllib.error.URLError, OSError) as e:
        return {"hour": f"{when:%Y-%m-%dT%H}", "error": str(e)[:120]}
    secs = time.time() - started
    size = path.stat().st_size
    return {"hour": f"{when:%Y-%m-%dT%H}", "path": str(path), "bytes": size,
            "sha256": digest.hexdigest(), "secs": round(secs, 2),
            "mib_per_s": round(size / max(secs, 0.01) / 2 ** 20, 2)}


def parse_one(path):
    """Parse one hour with pyarrow. Runs in its own PROCESS, reports own RSS."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.csv as pacsv

    skipped = Counter()

    def on_invalid_row(row):
        skipped["invalid"] += 1
        return "skip"

    started = time.time()
    table = pacsv.read_csv(
        path,
        read_options=pacsv.ReadOptions(
            column_names=["domain", "title", "views", "bytes"]),
        parse_options=pacsv.ParseOptions(
            delimiter=" ", quote_char=False, invalid_row_handler=on_invalid_row),
        convert_options=pacsv.ConvertOptions(column_types={
            "domain": pa.binary(), "title": pa.binary(),
            "views": pa.binary(), "bytes": pa.binary()}),
    )
    english = table.filter(pc.is_in(
        table.column("domain"),
        value_set=pa.array([b"en", b"en.m"], pa.binary())))
    numeric = pc.match_substring_regex(
        pc.cast(english.column("views"), pa.string()), r"^[0-9]+$")
    english = english.filter(numeric)
    views = pc.cast(pc.cast(english.column("views"), pa.string()), pa.int64())
    grouped = (pa.table({"title": english.column("title"), "views": views})
               .group_by("title").aggregate([("views", "sum")]))
    secs = time.time() - started

    return {"path": path, "secs": round(secs, 2), "titles": grouped.num_rows,
            "views": pc.sum(grouped.column("views_sum")).as_py(),
            "peak_rss_mib": round(peak_rss_mib(), 1), "pid": os.getpid(),
            "skipped_invalid": skipped["invalid"]}


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--hours", type=int, default=20)
    ap.add_argument("--download-workers", type=int, default=3,
                    help=f"concurrent connections, hard cap {MAX_CONNECTIONS}")
    ap.add_argument("--parse-workers", type=int, default=4,
                    help="parse processes; set to the vCPU count")
    ap.add_argument("--source", choices=sorted(SOURCES), default="your.org")
    ap.add_argument("--last-hour", default="2026-09-10T18")
    ap.add_argument("--instance-type", default="c7g.xlarge")
    ap.add_argument("--keep-files", action="store_true")
    args = ap.parse_args()

    if args.download_workers > MAX_CONNECTIONS:
        print(f"refusing {args.download_workers} connections: the cap is "
              f"{MAX_CONNECTIONS}, and evading it gets the IP blocked")
        return 2

    base = SOURCES[args.source]
    last = dt.datetime.strptime(args.last_hour, "%Y-%m-%dT%H").replace(
        tzinfo=dt.timezone.utc)
    schedule = hours_ending(last, args.hours)
    workdir = tempfile.mkdtemp(prefix="hype-decay-measure-")

    print("=" * 74)
    print(f"THROUGHPUT: {args.hours} hours, {args.source}, "
          f"{args.download_workers} connections, {args.parse_workers} parse processes")
    print("=" * 74)
    print(f"  base    : {base}")
    print(f"  sample  : {schedule[0]:%Y-%m-%dT%H} .. {schedule[-1]:%Y-%m-%dT%H}")
    print(f"  workdir : {workdir}")

    # --- stage 1: download -------------------------------------------------
    print(f"\n  STAGE 1: download at {args.download_workers} connections")
    started = time.time()
    downloaded = []
    with futures.ThreadPoolExecutor(max_workers=args.download_workers) as pool:
        for row in pool.map(download_one,
                            [(when, base, workdir) for when in schedule]):
            downloaded.append(row)
            if "error" in row:
                print(f"    {row['hour']}  FAILED {row['error']}")
            else:
                print(f"    {row['hour']}  {row['bytes'] / 2 ** 20:6.1f} MiB  "
                      f"{row['secs']:6.2f}s  {row['mib_per_s']:6.2f} MiB/s")
    download_wall = time.time() - started
    good = [r for r in downloaded if "error" not in r]
    if not good:
        print("\n  every download failed; nothing to measure")
        return 1
    total_bytes = sum(r["bytes"] for r in good)
    aggregate_mib_s = total_bytes / download_wall / 2 ** 20
    print(f"    {len(good)} files, {total_bytes / 2 ** 20:,.0f} MiB in "
          f"{download_wall:.1f}s = {aggregate_mib_s:.2f} MiB/s aggregate")

    # --- stage 2: parse ----------------------------------------------------
    print(f"\n  STAGE 2: parse in {args.parse_workers} processes")
    started = time.time()
    parsed = []
    with futures.ProcessPoolExecutor(max_workers=args.parse_workers) as pool:
        for row in pool.map(parse_one, [r["path"] for r in good]):
            parsed.append(row)
            print(f"    pid {row['pid']:>7}  {row['secs']:6.2f}s  "
                  f"{row['titles']:>9,} titles  {row['views']:>12,} views  "
                  f"peak RSS {row['peak_rss_mib']:7.1f} MiB")
    parse_wall = time.time() - started
    parse_cpu = sum(r["secs"] for r in parsed)
    per_file = statistics.mean(r["secs"] for r in parsed)
    peaks = [r["peak_rss_mib"] for r in parsed]
    by_pid = {}
    for row in parsed:
        by_pid[row["pid"]] = max(by_pid.get(row["pid"], 0), row["peak_rss_mib"])

    print(f"    wall {parse_wall:.1f}s, summed CPU {parse_cpu:.1f}s, "
          f"mean {per_file:.2f}s per file")
    print(f"    PEAK MEMORY PER WORKER: max {max(peaks):,.1f} MiB, "
          f"mean {statistics.mean(peaks):,.1f} MiB, over {len(by_pid)} processes")
    for pid, peak in sorted(by_pid.items()):
        print(f"      pid {pid:>7}  peak {peak:7.1f} MiB")
    print(f"    {args.parse_workers} workers at the max => "
          f"{max(peaks) * args.parse_workers / 1024:.2f} GiB resident")

    # --- extrapolate -------------------------------------------------------
    scale = TOTAL_HOURS / len(good)
    download_full_h = download_wall * scale / 3600
    parse_full_h = parse_cpu * scale / args.parse_workers / 3600
    overlapped_h = max(download_full_h, parse_full_h)
    hourly = INSTANCE_HOURLY_USD.get(args.instance_type, 0.02)

    print(f"\n  EXTRAPOLATION to {TOTAL_HOURS:,} hours "
          f"({MEASURED_WINDOW_GIB:,.1f} GiB measured in Task 0)")
    print(f"    download, {args.download_workers} connections : "
          f"{download_full_h:7.1f} h")
    print(f"    parse, {args.parse_workers} processes          : "
          f"{parse_full_h:7.1f} h")
    print(f"    overlapped wall clock              : {overlapped_h:7.1f} h "
          f"({overlapped_h / 24:.1f} days)")
    print(f"    {args.instance_type} at ${hourly}/h            : "
          f"${overlapped_h * hourly:7.2f}  (ESTIMATE)")
    print(f"    20 GiB gp3 for that duration       : "
          f"${20 * EBS_GP3_USD_PER_GIB_MONTH * overlapped_h / 730:7.2f}  (ESTIMATE)")

    print()
    if parse_full_h > download_full_h:
        print(f"  BINDING CONSTRAINT: PROCESSING. Parse needs {parse_full_h:.1f} h "
              f"against {download_full_h:.1f} h of download, so the")
        print(f"  {MAX_CONNECTIONS}-connection cap is not the limit -- more parse "
              f"processes or a faster parse is.")
    else:
        print(f"  BINDING CONSTRAINT: TRANSFER. Download needs "
              f"{download_full_h:.1f} h against {parse_full_h:.1f} h of parse,")
        print(f"  and {MAX_CONNECTIONS} connections is the ceiling, so the parse "
              f"has headroom to spare.")

    print("\n  Estimates assume this sample is representative. Task 0 measured")
    print("  hourly files between 45 and 77 MiB depending on hour of day, so a")
    print("  sample from one part of the clock will be optimistic or pessimistic.")

    if not args.keep_files:
        for row in good:
            Path(row["path"]).unlink(missing_ok=True)
        Path(workdir).rmdir()
        print(f"\n  cleaned up {workdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
