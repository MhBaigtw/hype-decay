"""
Minimal Athena runner shared by the Task 4 scripts.

Always runs in the scan-limited workgroup, so a query that would read too much
is cancelled by the workgroup -- the guardrail -- rather than by this code.
Every result carries the bytes scanned, because that number is the bill.
"""

import os
import time

import boto3

WORKGROUP = "hype-decay"
DATABASE = "hype_decay"
REGION = "us-east-1"


class Athena:
    def __init__(self, profile=None, workgroup=WORKGROUP, database=DATABASE):
        session = boto3.Session(profile_name=profile or os.environ.get("AWS_PROFILE"),
                                region_name=REGION)
        self.client = session.client("athena")
        self.workgroup, self.database = workgroup, database
        self.scanned_total = 0

    def run(self, sql, fetch=False, check=True):
        qid = self.client.start_query_execution(
            QueryString=sql, WorkGroup=self.workgroup,
            QueryExecutionContext={"Database": self.database})["QueryExecutionId"]
        while True:
            q = self.client.get_query_execution(QueryExecutionId=qid)["QueryExecution"]
            state = q["Status"]["State"]
            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        stats = q.get("Statistics", {})
        result = {"id": qid, "state": state,
                  "scanned": stats.get("DataScannedInBytes", 0),
                  "secs": stats.get("EngineExecutionTimeInMillis", 0) / 1000,
                  "reason": q["Status"].get("StateChangeReason", "")}
        self.scanned_total += result["scanned"]
        if check and state != "SUCCEEDED":
            raise RuntimeError(f"{state}: {result['reason']}\n{sql[:400]}")
        if fetch and state == "SUCCEEDED":
            result["rows"] = self.rows(qid)
        return result

    def rows(self, qid):
        out, kwargs = [], {"QueryExecutionId": qid}
        while True:
            page = self.client.get_query_results(**kwargs)
            for r in page["ResultSet"]["Rows"]:
                out.append([c.get("VarCharValue") for c in r["Data"]])
            if "NextToken" not in page:
                break
            kwargs["NextToken"] = page["NextToken"]
        return out[1:]          # first row is the header


def mib(n):
    return f"{n / 2**20:,.1f} MiB"
