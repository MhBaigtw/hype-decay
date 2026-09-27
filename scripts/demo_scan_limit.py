#!/usr/bin/env python3
"""
Prove the Athena workgroup scan limit actually rejects a query.

TASKS.md Task 1: "an intentionally unpartitioned Athena query is rejected by the
workgroup limit. Demonstrate that rejection -- the guardrail is worthless until
it has been seen to fire."

At Task 1 the project has no data of its own, so this points a deliberately
unpartitioned external table at a large public dataset (NOAA GHCN-Daily, open
data, us-east-1, no requester-pays) and runs a full-table COUNT(*). The
workgroup should cancel it at 5 GiB.

Cost: bytes scanned before cancellation are billable at roughly 5 USD/TB, so
about 0.03 USD per run. Nothing is written to S3 except the query result stub,
which expires in 7 days by lifecycle rule.

Exit code 0 means the guardrail fired (the good outcome), which Athena reports
as a CANCELLED query whose reason is "Bytes scanned limit was exceeded". Exit 1
means the query succeeded, or stopped for some other reason -- either way the
limit is unproven.

    python3 demo_scan_limit.py              # run the demonstration
    python3 demo_scan_limit.py --cleanup    # drop the demo database and table

Requires the AWS CLI on PATH and a profile with Athena and Glue access
(AWS_PROFILE, or --profile).
"""

import argparse
import json
import os
import subprocess
import sys
import time

WORKGROUP = "hype-decay"
REGION = "us-east-1"
DATABASE = "hype_decay_demo"
TABLE = "ghcn_unpartitioned"

# NOAA Global Historical Climatology Network Daily, an AWS Open Data set.
# Tens of GB, unpartitioned, public. Reading it is free; scanning it is not.
PUBLIC_DATA = "s3://noaa-ghcn-pds/csv/by_year/"

SCAN_LIMIT_BYTES = 5 * 1024 ** 3
POLL_SECONDS = 5
POLL_TIMEOUT = 600


def aws(args, profile):
    cmd = ["aws"] + args + ["--region", REGION, "--output", "json"]
    if profile:
        cmd += ["--profile", profile]
    done = subprocess.run(cmd, capture_output=True, text=True)
    if done.returncode != 0:
        print(f"  aws call failed: {' '.join(args[:3])}\n  {done.stderr.strip()}")
        return None
    return json.loads(done.stdout) if done.stdout.strip() else {}


def run_query(sql, profile, label):
    """Starts a query and waits for a terminal state. Returns the execution dict."""
    print(f"\n  {label}")
    started = aws(["athena", "start-query-execution",
                   "--work-group", WORKGROUP,
                   "--query-string", sql], profile)
    if not started:
        return None
    qid = started["QueryExecutionId"]
    print(f"    query id: {qid}")

    waited = 0
    while waited < POLL_TIMEOUT:
        got = aws(["athena", "get-query-execution", "--query-execution-id", qid], profile)
        if not got:
            return None
        execution = got["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return execution
        time.sleep(POLL_SECONDS)
        waited += POLL_SECONDS
    print(f"    still running after {POLL_TIMEOUT}s, giving up")
    return None


def human_gib(n):
    return f"{n / 1024 ** 3:.2f} GiB"


def demonstrate(profile):
    print("=" * 68)
    print("ATHENA SCAN-LIMIT DEMONSTRATION")
    print("=" * 68)
    print(f"  workgroup  : {WORKGROUP} (limit {human_gib(SCAN_LIMIT_BYTES)} per query)")
    print(f"  public data: {PUBLIC_DATA}")
    print("  the table is UNPARTITIONED on purpose -- that is what is being caught")

    for sql, label in (
        (f"CREATE DATABASE IF NOT EXISTS {DATABASE}", "creating demo database"),
        (f"""CREATE EXTERNAL TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
                id string, observation_date string, element string,
                data_value string, m_flag string, q_flag string,
                s_flag string, observation_time string)
             ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             LOCATION '{PUBLIC_DATA}'""", "creating unpartitioned external table"),
    ):
        execution = run_query(sql, profile, label)
        if execution is None or execution["Status"]["State"] != "SUCCEEDED":
            print("    setup failed, cannot run the demonstration")
            return 1
        print("    ok")

    execution = run_query(
        f"SELECT count(*) FROM {DATABASE}.{TABLE}",
        profile,
        "running a full-table COUNT(*), which must scan far more than the limit",
    )
    if execution is None:
        return 1

    status = execution["Status"]
    scanned = execution.get("Statistics", {}).get("DataScannedInBytes", 0)
    reason = status.get("StateChangeReason", "")
    print(f"\n    state        : {status['State']}")
    print(f"    bytes scanned: {scanned:,} ({human_gib(scanned)})")
    print(f"    reason       : {reason or '(none)'}")

    # Athena reports a scan-limit kill as CANCELLED, not FAILED. Accepting only
    # FAILED here made the first run of this script report a passing guardrail
    # as broken.
    rejected = status["State"] in ("CANCELLED", "FAILED") and "limit" in reason.lower()
    print()
    if rejected:
        print("  [ok]    GUARDRAIL FIRED -- the workgroup cancelled the query.")
        print(f"          Billable scan stopped at {human_gib(scanned)} instead of")
        print("          running to completion over the whole dataset.")
        print("          CLAUDE.md: if a real query trips this, fix the query.")
        print("          Do not raise the limit.")
        return 0
    if status["State"] == "SUCCEEDED":
        print("  [FAIL]  the query SUCCEEDED. Either the dataset now scans under")
        print("          the limit, or enforce_workgroup_configuration is off.")
        print("          The guardrail is not proven.")
    else:
        print("  [FAIL]  the query failed for a reason other than the scan limit,")
        print("          so the limit itself is still unproven.")
    return 1


def cleanup(profile):
    print("dropping the demo table and database (the public data is untouched)")
    for sql, label in (
        (f"DROP TABLE IF EXISTS {DATABASE}.{TABLE}", "dropping table"),
        (f"DROP DATABASE IF EXISTS {DATABASE}", "dropping database"),
    ):
        execution = run_query(sql, profile, label)
        state = execution["Status"]["State"] if execution else "UNKNOWN"
        print(f"    {state}")
    return 0


def main():
    ap = argparse.ArgumentParser(description="Demonstrate the Athena scan limit rejecting a query")
    ap.add_argument("--profile", default=os.environ.get("AWS_PROFILE"),
                    help="AWS profile (default: AWS_PROFILE)")
    ap.add_argument("--cleanup", action="store_true",
                    help="drop the demo database and table, then exit")
    args = ap.parse_args()

    if not args.profile:
        print("No AWS profile. Pass --profile or set AWS_PROFILE.")
        return 2
    return cleanup(args.profile) if args.cleanup else demonstrate(args.profile)


if __name__ == "__main__":
    sys.exit(main())
