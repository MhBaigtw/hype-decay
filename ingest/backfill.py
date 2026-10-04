#!/usr/bin/env python3
"""
Task 3 backfill: every hour of the two-year window, a day at a time.

    for each day D in the window:
        ingest D's 24 source hours   4 parse processes, at most 3 connections
        stop every parse process     so their memory is given back
        compact D                    in a fresh process, nothing else running
        publish progress             CloudWatch, namespace hype-decay/backfill

WHY PARSING AND COMPACTION ALTERNATE. Measured on the c7g.xlarge (NOTES, Task
3): a parse worker peaks at 1,582 MiB and compacting a day at 5,435 MiB. Four
workers beside a compaction need 11.5 GiB on a box with 7.6 GiB usable. So the
pool is shut down before every compaction -- not merely idle, because an idle
worker still holds its peak -- and compaction runs as its own process so its
5.4 GiB goes back to the OS when it exits.

THE MANIFEST IS THE ONLY STATE. The runner keeps nothing of its own. For each
day it reads the manifest and decides (plan_day):

    day row compacted                     skip
    day row compacting                    finish the compaction (crash recovery)
    day row invalidated / failed          blocked: needs a person
    any hour absent, pending, failed with
      attempts left, or in-flight with an
      expired lease                       ingest those hours
    only live leases outstanding          wait for them to lapse, then re-plan
    an hour failed MAX_ATTEMPTS times     incomplete: left uncompacted, reported
    all 24 done                           compact

Killing the runner at any line and starting it again therefore loses nothing
and repeats nothing: a finished hour is `done` and is never planned again, an
unfinished one is in-flight with a lease that lapses, and a half-finished
compaction is `compacting` with its staging key recorded. test_backfill_resume
proves this against moto with the real conditional writes.

A DAY WITH A DEAD HOUR IS NOT COMPACTED. TASKS asks for gaps to be visible and
never silently filled. Compacting around a missing hour would produce a day that
looks whole, and compaction closes a day to partials (SPEC), so a later retry
would need a forced invalidation. The day stays as partials and is reported.

CONNECTIONS. Workers share one semaphore of 3, held around every request to the
mirror or to the origin (the content-length HEAD), so there are never more than
3 connections out, whichever worker holds them.

    python3 backfill.py --start 2024-09-13 --end 2024-09-15        # trial
    python3 backfill.py                                             # whole window
"""

import argparse
import concurrent.futures as futures
import contextlib
import datetime as dt
import json
import multiprocessing
import os
import re
import subprocess
import sys
import threading
import time
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

from compact_day import source_hours_for  # noqa: E402
from ingest_hour import (  # noqa: E402
    DEFAULT_BUCKET,
    DEFAULT_REGION,
    DEFAULT_TABLE,
    MAX_CONNECTIONS,
    Manifest,
)

# Days by hour_start, so 17,520 source hours: 2024-09-13T01 .. 2026-09-13T00.
# The window Task 0 measured as complete.
WINDOW_START = dt.date(2024, 9, 13)
WINDOW_END = dt.date(2026, 9, 12)

MAX_ATTEMPTS = 3          # per hour, counted by the manifest's attempt field
MAX_PASSES = 8            # plan/act rounds per day before giving up on it
# Waits before the 2nd and 3rd attempt at a failed hour. Without them a
# minute-long mirror blip would burn all three attempts in seconds and mark a
# whole day of hours permanently failed.
RETRY_BACKOFF_SECONDS = (30, 120)
PARSE_WORKERS = 4         # c7g.xlarge: 4 vCPUs, 4 x 1.6 GiB fits in 7.6 GiB
METRIC_NAMESPACE = "hype-decay/backfill"


def log(msg):
    print(f"{dt.datetime.now(dt.timezone.utc):%H:%M:%S}  {msg}", flush=True)


def window_days(start, end):
    return [start + dt.timedelta(days=n) for n in range((end - start).days + 1)]


# --- deciding, from the manifest alone --------------------------------------

@dataclass
class DayPlan:
    day: dt.date
    action: str
    todo: list = field(default_factory=list)
    done: list = field(default_factory=list)
    held: list = field(default_factory=list)
    exhausted: list = field(default_factory=list)
    retrying: list = field(default_factory=list)
    lease_until: int = 0


def plan_day(manifest, day, now):
    row = manifest.get(f"day#{day}") or {}
    status = row.get("status")
    if status == "compacted":
        return DayPlan(day, "skip")
    if status == "compacting":
        return DayPlan(day, "resume-compaction")
    if status in ("invalidated", "failed"):
        return DayPlan(day, "blocked")

    plan = DayPlan(day, "")
    for hour in source_hours_for(day):
        item = manifest.get(hour) or {}
        st = item.get("status")
        if st == "done":
            plan.done.append(hour)
        elif st == "in-flight" and int(item.get("lease_expires", 0)) >= now:
            plan.held.append(hour)
            plan.lease_until = max(plan.lease_until, int(item["lease_expires"]))
        elif st == "failed" and int(item.get("attempt", 0)) >= MAX_ATTEMPTS:
            plan.exhausted.append(hour)
        else:
            plan.todo.append(hour)
            if st == "failed":
                plan.retrying.append(hour)

    plan.action = ("ingest" if plan.todo else "wait" if plan.held
                   else "incomplete" if plan.exhausted else "compact")
    return plan


def run_day(manifest, day, process_hours, compact, clock=time.time, sleep=time.sleep):
    """Plans and acts on one day until it reaches an end state.

    process_hours(hours) -> list of {"hour", "code", ...}; compact(day) -> dict
    with "exit". Both are injected so the tests can drive this with fakes.
    """
    started = clock()
    report = {"day": str(day), "passes": 0, "hours_ingested": 0, "hours_failed": 0,
              "hour_results": [], "compaction": None, "ingest_secs": 0.0,
              "compact_secs": 0.0, "exhausted": [], "held": []}

    retry_round = 0
    for _ in range(MAX_PASSES):
        report["passes"] += 1
        plan = plan_day(manifest, day, clock())

        if plan.action == "skip":
            report["outcome"] = "skipped" if report["passes"] == 1 else "compacted"
            break
        if plan.action == "blocked":
            report["outcome"] = "blocked"
            break
        if plan.action == "incomplete":
            report["outcome"] = "incomplete"
            report["exhausted"] = plan.exhausted
            break
        if plan.action == "wait":
            # Someone -- almost always this runner's previous life -- holds a
            # lease. Wait for it to lapse rather than race it.
            wait = max(1, plan.lease_until - int(clock()) + 1)
            log(f"{day}: {len(plan.held)} hour(s) leased elsewhere, waiting {wait}s")
            sleep(wait)
            continue
        if plan.action == "ingest":
            if plan.retrying:
                wait = RETRY_BACKOFF_SECONDS[min(retry_round, len(RETRY_BACKOFF_SECONDS) - 1)]
                retry_round += 1
                log(f"{day}: retrying {len(plan.retrying)} failed hour(s) after {wait}s")
                sleep(wait)
            t = clock()
            results = process_hours(plan.todo)
            report["ingest_secs"] += clock() - t
            report["hour_results"] += results
            report["hours_ingested"] += sum(1 for r in results if r["code"] == 0)
            report["hours_failed"] += sum(1 for r in results if r["code"] not in (0, 3))
            continue
        if plan.action in ("compact", "resume-compaction"):
            t = clock()
            report["compaction"] = compact(day)
            report["compact_secs"] += clock() - t
            if report["compaction"].get("exit") != 0:
                report["outcome"] = "compaction-failed"
                break
            continue      # re-plan: the day row must now read compacted
    else:
        report["outcome"] = "stalled"
        report["held"] = plan.held

    report["day_secs"] = clock() - started
    return report


def final_pass(manifest, days, compact, clock=time.time):
    """The second pass: compact every day that can be, explain every one that
    cannot. Runs after the main loop with nothing else running, so it catches a
    compaction that failed (out of memory, say) without re-ingesting anything.

    Returns {day: reason} for every day still not compacted afterwards.
    """
    left = {}
    for day in days:
        plan = plan_day(manifest, day, clock())
        result = None
        if plan.action in ("compact", "resume-compaction"):
            result = compact(day)
            plan = plan_day(manifest, day, clock())
        if plan.action == "skip":
            continue
        if plan.action == "incomplete":
            why = []
            for hour in plan.exhausted:
                item = manifest.get(hour) or {}
                why.append(f"{hour} failed {int(item.get('attempt', 0))}x: "
                           f"{item.get('error', 'no reason recorded')}")
            left[str(day)] = "hours failed permanently: " + "; ".join(why)
        elif plan.action == "blocked":
            row = manifest.get(f"day#{day}") or {}
            left[str(day)] = (f"day row is {row.get('status')}: "
                              f"{row.get('error', 'no reason recorded')}")
        elif result is not None:
            left[str(day)] = f"compaction did not complete: {result}"
        else:
            left[str(day)] = (f"{plan.action}: {len(plan.todo)} hour(s) not done, "
                              f"{len(plan.held)} leased: {(plan.todo + plan.held)[:4]}")
    return left


# --- the real workers --------------------------------------------------------

_GATE = None
_WORKER_OPTS = None


def _init_worker(gate, opts):
    global _GATE, _WORKER_OPTS
    _GATE, _WORKER_OPTS = gate, opts


def _ingest_one(hour):
    """One hour, in a pool process. Output goes to that hour's own log."""
    import resource

    import ingest_hour

    opts = _WORKER_OPTS
    args = argparse.Namespace(
        source_hour=hour, source="your.org", verify_origin_length=True,
        bucket=opts["bucket"], table=opts["table"], region=opts["region"],
        profile=opts["profile"], force=False, dry_run=False, no_fixture=True)
    before = Counter(ingest_hour.S3_REQUESTS)
    started = time.time()
    log_path = Path(opts["log_dir"]) / "hours" / f"{hour}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(log_path, "w", encoding="utf-8") as out, \
                contextlib.redirect_stdout(out):
            code = ingest_hour.run(args, download_gate=_GATE)
        error = None
    except Exception as e:  # noqa: BLE001 - run() already marked the hour failed
        code, error = "failed", str(e)[:300]
    return {"hour": hour, "code": code, "error": error,
            "secs": round(time.time() - started, 2),
            "peak_rss_mib": round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1),
            "s3_requests": dict(Counter(ingest_hour.S3_REQUESTS) - before)}


def make_process_hours(opts, workers, connections):
    def process_hours(hours):
        # forkserver, not fork: the parent has a memory-sampling thread, and
        # forking a threaded process is how locks end up held forever.
        ctx = multiprocessing.get_context("forkserver")
        gate = ctx.BoundedSemaphore(connections)
        # A fresh pool per call, shut down on exit, so no parse worker is
        # alive -- holding its 1.6 GiB peak -- when compaction starts.
        with futures.ProcessPoolExecutor(max_workers=workers, mp_context=ctx,
                                         initializer=_init_worker,
                                         initargs=(gate, opts)) as pool:
            return list(pool.map(_ingest_one, hours))
    return process_hours


def make_compact(opts, verify_copy):
    def compact(day):
        if verify_copy:
            copy_for_verification(opts, day)
        log_path = Path(opts["log_dir"]) / f"compact-{day}.log"
        cmd = [sys.executable, str(HERE / "compact_day.py"), "--dt", str(day),
               "--bucket", opts["bucket"], "--table", opts["table"],
               "--region", opts["region"]]
        if opts["profile"]:
            cmd += ["--profile", opts["profile"]]
        with open(log_path, "w", encoding="utf-8") as out:
            proc = subprocess.run(cmd, stdout=out, stderr=subprocess.STDOUT)
        text = log_path.read_text(encoding="utf-8")
        found = re.findall(r"^RESULT (\{.*\})$", text, flags=re.M)
        result = json.loads(found[-1]) if found else {}
        result.setdefault("exit", proc.returncode)
        return result
    return compact


def copy_for_verification(opts, day):
    """TRIAL ONLY. Copies the day's partials aside before compaction deletes
    them, so verify_compaction.py can rebuild the day independently and
    compare. Outside curated/page_daily/, so no reader sees the copies."""
    import boto3
    s3 = boto3.Session(profile_name=opts["profile"],
                       region_name=opts["region"]).client("s3")
    for hour in source_hours_for(day):
        key = f"curated/page_daily/dt={day}/part-{hour}.parquet"
        s3.copy_object(Bucket=opts["bucket"],
                       Key=f"curated/_verify/page_daily/dt={day}/part-{hour}.parquet",
                       CopySource={"Bucket": opts["bucket"], "Key": key})


# --- observing ---------------------------------------------------------------

class MemoryWatch:
    """Samples MemAvailable twice a second; reports the lowest seen per phase.

    Per-process peak RSS misses what the box as a whole went through -- page
    cache, tmpfs, the parent -- and the question that matters is whether the
    machine ran short, which is what MemAvailable answers.
    """

    def __init__(self):
        self.low = None
        self.ok = Path("/proc/meminfo").exists()
        if self.ok:
            threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        while True:
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemAvailable:"):
                    mib = int(line.split()[1]) / 1024
                    self.low = mib if self.low is None else min(self.low, mib)
            time.sleep(0.5)

    def take(self):
        low, self.low = self.low, None
        return round(low, 0) if low is not None else None


def publish_progress(cloudwatch, report, days_remaining):
    data = [
        ("HoursIngested", report.get("hours_ingested", 0), "Count"),
        ("HoursFailed", report.get("hours_failed", 0), "Count"),
        ("DaysCompacted", 1 if report.get("outcome") == "compacted" else 0, "Count"),
        ("DaySeconds", report.get("day_secs", 0.0), "Seconds"),
        ("DaysRemaining", days_remaining, "Count"),
    ]
    for name, key in (("ParsePeakRssMiB", "parse_peak_rss_mib"),
                      ("CompactPeakRssMiB", "compact_peak_rss_mib"),
                      ("MinMemAvailableMiB", "min_mem_available_mib")):
        if report.get(key) is not None:
            data.append((name, report[key], "None"))
    cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=[
        {"MetricName": n, "Value": float(v), "Unit": u} for n, v, u in data])


def summarise(report, mem_ingest, mem_compact):
    """Adds the per-day figures the operator and the metrics need."""
    results = report["hour_results"]
    report["parse_peak_rss_mib"] = max((r.get("peak_rss_mib", 0) for r in results),
                                       default=None)
    comp = report["compaction"] or {}
    report["compact_peak_rss_mib"] = comp.get("peak_rss_mib")
    lows = [m for m in (mem_ingest, mem_compact) if m is not None]
    report["min_mem_available_mib"] = min(lows) if lows else None
    report["min_mem_available_ingest_mib"] = mem_ingest
    report["min_mem_available_compact_mib"] = mem_compact
    s3_ingest = Counter()
    for r in results:
        s3_ingest.update(r.get("s3_requests", {}))
    report["s3_requests_ingest"] = dict(s3_ingest)
    report["s3_requests_compact"] = comp.get("s3_requests", {})
    report["s3_requests_total"] = sum(s3_ingest.values()) + sum(
        report["s3_requests_compact"].values())
    return report


# --- main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("--start", type=dt.date.fromisoformat, default=WINDOW_START)
    ap.add_argument("--end", type=dt.date.fromisoformat, default=WINDOW_END)
    ap.add_argument("--workers", type=int, default=PARSE_WORKERS)
    ap.add_argument("--connections", type=int, default=MAX_CONNECTIONS)
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--table", default=DEFAULT_TABLE)
    ap.add_argument("--region", default=DEFAULT_REGION)
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    ap.add_argument("--log-dir", default="/var/log/hype-decay")
    ap.add_argument("--tmpdir", default="/var/tmp/hype-decay",
                    help="where hour files are staged. NOT /tmp: on AL2023 /tmp "
                         "is tmpfs, i.e. RAM, which the parse workers need")
    ap.add_argument("--verify-copy", action="store_true",
                    help="trial only: copy partials aside for verify_compaction.py")
    ap.add_argument("--no-metrics", action="store_true")
    args = ap.parse_args()

    if args.connections > MAX_CONNECTIONS:
        log(f"refusing {args.connections} connections: the cap is {MAX_CONNECTIONS}")
        return 2
    if not WINDOW_START <= args.start <= args.end <= WINDOW_END:
        log(f"--start/--end must lie inside {WINDOW_START} .. {WINDOW_END}")
        return 2

    Path(args.log_dir).mkdir(parents=True, exist_ok=True)
    Path(args.tmpdir).mkdir(parents=True, exist_ok=True)
    os.environ["TMPDIR"] = args.tmpdir      # inherited by every worker

    import boto3
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    manifest = Manifest(session.resource("dynamodb").Table(args.table), "backfill-planner")
    cloudwatch = session.client("cloudwatch")
    opts = {"bucket": args.bucket, "table": args.table, "region": args.region,
            "profile": args.profile, "log_dir": args.log_dir}
    process_hours = make_process_hours(opts, args.workers, args.connections)
    compact = make_compact(opts, args.verify_copy)
    watch = MemoryWatch()
    days = window_days(args.start, args.end)
    journal = Path(args.log_dir) / "days.jsonl"

    log(f"backfill {days[0]} .. {days[-1]}: {len(days)} days, {len(days) * 24:,} "
        f"source hours, {args.workers} workers, {args.connections} connections")
    totals = Counter()
    run_started = time.time()

    for n, day in enumerate(days):
        mem = {"ingest": None, "compact": None}

        def timed_ingest(hours, _mem=mem):
            watch.take()
            out = process_hours(hours)
            _mem["ingest"] = watch.take()
            return out

        def timed_compact(d, _mem=mem):
            watch.take()
            out = compact(d)
            _mem["compact"] = watch.take()
            return out

        report = summarise(run_day(manifest, day, timed_ingest, timed_compact),
                           mem["ingest"], mem["compact"])
        totals[report["outcome"]] += 1
        totals["hours_ingested"] += report["hours_ingested"]
        totals["hours_failed"] += report["hours_failed"]
        totals["s3_requests"] += report["s3_requests_total"]

        log(f"{day} {report['outcome']:<17} {report['hours_ingested']:>2} h  "
            f"ingest {report['ingest_secs']:6.1f}s  compact {report['compact_secs']:5.1f}s  "
            f"day {report['day_secs']:6.1f}s  parse peak {report['parse_peak_rss_mib']} MiB  "
            f"compact peak {report['compact_peak_rss_mib']} MiB  "
            f"min avail {report['min_mem_available_mib']} MiB  "
            f"S3 req {report['s3_requests_total']}")
        if report["outcome"] in ("incomplete", "blocked", "compaction-failed", "stalled"):
            log(f"  ATTENTION {day}: {report['outcome']} "
                f"exhausted={report['exhausted']} held={report['held']}")

        slim = {k: v for k, v in report.items() if k != "hour_results"}
        slim["failed_hours"] = [r for r in report["hour_results"] if r["code"] not in (0, 3)]
        with open(journal, "a", encoding="utf-8") as f:
            f.write(json.dumps(slim, default=str) + "\n")
        if not args.no_metrics:
            publish_progress(cloudwatch, report, days_remaining=len(days) - n - 1)

    log(f"main loop done: {dict(totals)} in {(time.time() - run_started) / 60:.1f} min")

    # --- second pass: compact anything left, explain the rest ----------------
    log("SECOND PASS: compacting any day not yet compacted, nothing else running")
    left = final_pass(manifest, days, compact)
    for day, why in left.items():
        log(f"  NOT COMPACTED {day}: {why}")
    log(f"second pass: {len(left)} day(s) not compacted")

    elapsed = time.time() - run_started
    summary = {"window": [str(days[0]), str(days[-1])], "days": len(days),
               "totals": dict(totals), "not_compacted": left,
               "elapsed_hours": round(elapsed / 3600, 2),
               "finished_at": dt.datetime.now(dt.timezone.utc).isoformat()}
    (Path(args.log_dir) / "summary.json").write_text(json.dumps(summary, indent=2))
    if not args.no_metrics:
        cloudwatch.put_metric_data(Namespace=METRIC_NAMESPACE, MetricData=[
            {"MetricName": "DaysNotCompacted", "Value": float(len(left)), "Unit": "Count"},
            {"MetricName": "RunComplete", "Value": 1.0, "Unit": "Count"}])
    log(f"done: {dict(totals)} in {elapsed / 60:.1f} min")
    if left:
        return 1
    return 0 if totals["hours_failed"] == 0 and set(totals) <= {
        "compacted", "skipped", "hours_ingested", "hours_failed", "s3_requests"} else 1


if __name__ == "__main__":
    sys.exit(main())
