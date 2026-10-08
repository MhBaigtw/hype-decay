#!/usr/bin/env python3
"""
The double-count guard, against moto's DynamoDB and S3.

A compacted day.parquet already contains every hour of its day. If the ingester
then drops a fresh partial into the same partition, every reader that lists the
prefix counts that hour twice, and nothing about the output looks wrong. So:

  * a partial may NOT be written into a day whose day# row is compacted or
    compacting
  * --force on such an hour invalidates the day first: the row says so, and
    day.parquet leaves the readable partition (quarantined, not deleted), so the
    partition reads as a visible gap rather than a silent double count
  * a compacting day is never touched, force or not -- compaction holds it
  * the compactor rebuilds a day only from all 24 expected partials, and refuses
    if a compacted object is still sitting next to them

moto is used rather than a hand-written fake because the guard rests on
DynamoDB conditional writes, and a fake would encode my assumptions about them
instead of checking them.

    python3 test_day_guard.py
"""

import datetime as dt
import io
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

import boto3  # noqa: E402
import pyarrow as pa  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from moto import mock_aws  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import compact_day  # noqa: E402
from ingest_hour import DayGuardRefused, Manifest, guard_day  # noqa: E402

BUCKET = "test-curated"
TABLE = "test-manifest"
DAY = dt.date(2026, 9, 10)
DAY_PREFIX = f"curated/page_daily/dt={DAY}/"
DAY_OBJECT = f"{DAY_PREFIX}day.parquet"


def day_parquet_bytes(views):
    table = pa.Table.from_pydict({
        "project": ["en.wikipedia"] * len(views),
        "page_title": [f"P{i}" for i in range(len(views))],
        "dt": [DAY] * len(views),
        "views": views,
        "hours_present": [24] * len(views),
    }, schema=compact_day.PAGE_DAILY_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


@mock_aws
class GuardTest(unittest.TestCase):

    def setUp(self):
        self.s3 = boto3.client("s3")
        self.s3.create_bucket(Bucket=BUCKET)
        ddb = boto3.resource("dynamodb")
        self.table = ddb.create_table(
            TableName=TABLE, BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "source_hour", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_hour",
                                   "AttributeType": "S"}])
        self.manifest = Manifest(self.table, "test-worker")

    def set_day(self, status, **extra):
        self.table.put_item(Item={"source_hour": f"day#{DAY}", "status": status,
                                  "views": 1000, "rows": 3, "floor_applied": 0,
                                  "key_compacted": DAY_OBJECT, **extra})
        self.s3.put_object(Bucket=BUCKET, Key=DAY_OBJECT,
                           Body=day_parquet_bytes([500, 300, 200]))

    def day_status(self):
        row = self.table.get_item(Key={"source_hour": f"day#{DAY}"}).get("Item")
        return row and row["status"]

    def readable_keys(self):
        listed = self.s3.list_objects_v2(Bucket=BUCKET, Prefix=DAY_PREFIX)
        return sorted(o["Key"] for o in listed.get("Contents", []))

    # --- the ingester side --------------------------------------------------

    def test_open_day_is_writable(self):
        self.assertEqual(guard_day(self.manifest, self.s3, BUCKET, DAY,
                                   "2026-09-10T18", force=False), "open")

    def test_compacted_day_refuses_a_partial(self):
        self.set_day("compacted")
        with self.assertRaises(DayGuardRefused):
            guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T18",
                      force=False)
        self.assertEqual(self.day_status(), "compacted")
        self.assertEqual(self.readable_keys(), [DAY_OBJECT])

    def test_compacting_day_refuses_even_with_force(self):
        self.set_day("compacting", key_staging="curated/_staging/x")
        with self.assertRaises(DayGuardRefused):
            guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T18",
                      force=True)
        self.assertEqual(self.day_status(), "compacting")
        self.assertEqual(self.readable_keys(), [DAY_OBJECT])

    def test_force_invalidates_and_quarantines(self):
        self.set_day("compacted")
        action = guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T18",
                           force=True)
        self.assertEqual(action, "invalidated")
        row = self.table.get_item(Key={"source_hour": f"day#{DAY}"})["Item"]
        self.assertEqual(row["status"], "invalidated")
        self.assertEqual(row["invalidated_by"], "2026-09-10T18")
        self.assertEqual(int(row["views_previous"]), 1000)
        # Not recorded on this row, so marked -1 rather than failing the write.
        self.assertEqual(int(row["views_unfloored_previous"]), -1)
        # day.parquet is gone from the readable partition ...
        self.assertEqual(self.readable_keys(), [])
        # ... but kept, out of the reader's path, until a recompaction succeeds.
        self.s3.head_object(Bucket=BUCKET, Key=row["key_quarantine"])
        self.assertFalse(row["key_quarantine"].startswith("curated/page_daily/"))

    def test_invalidated_day_finishes_an_interrupted_quarantine(self):
        # A crash after the row flipped but before day.parquet moved leaves the
        # compacted object in the partition. The next ingest must finish the
        # move before it writes, or the partial lands next to it.
        self.set_day("invalidated",
                     key_quarantine=f"curated/_invalidated/page_daily/dt={DAY}/day.parquet")
        action = guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T19",
                           force=False)
        self.assertEqual(action, "open")
        self.assertEqual(self.readable_keys(), [])

    def test_invalidation_copies_unfloored_totals(self):
        self.set_day("compacted", views_unfloored=5000, rows_unfloored=40)
        guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T18", force=True)
        row = self.table.get_item(Key={"source_hour": f"day#{DAY}"})["Item"]
        self.assertEqual((int(row["rows_unfloored_previous"]),
                          int(row["views_unfloored_previous"])), (40, 5000))

    def test_second_force_does_not_reinvalidate(self):
        self.set_day("compacted")
        guard_day(self.manifest, self.s3, BUCKET, DAY, "2026-09-10T18", force=True)
        self.assertEqual(guard_day(self.manifest, self.s3, BUCKET, DAY,
                                   "2026-09-10T19", force=True), "open")
        row = self.table.get_item(Key={"source_hour": f"day#{DAY}"})["Item"]
        self.assertEqual(row["invalidated_by"], "2026-09-10T18")

    # --- the compactor side -------------------------------------------------

    def test_partition_check_wants_exactly_the_24_expected_partials(self):
        hours = compact_day.source_hours_for(DAY)
        keys = [compact_day.partial_key(DAY, h) for h in hours]
        self.assertEqual(hours[-1], "2026-09-11T00")
        self.assertEqual(compact_day.check_partition(keys, DAY),
                         {"missing": [], "unexpected": [], "has_day_object": False})

        one_short = compact_day.check_partition(keys[:-1], DAY)
        self.assertEqual(one_short["missing"], ["2026-09-11T00"])

        stray = keys + [f"{DAY_PREFIX}part-2026-09-12T05.parquet"]
        self.assertEqual(compact_day.check_partition(stray, DAY)["unexpected"],
                         [f"{DAY_PREFIX}part-2026-09-12T05.parquet"])

        beside = compact_day.check_partition(keys + [DAY_OBJECT], DAY)
        self.assertTrue(beside["has_day_object"])

    def test_refloor_only_raises_the_floor(self):
        self.assertTrue(compact_day.refloor_allowed(0, 10))
        self.assertTrue(compact_day.refloor_allowed(10, 10))
        # Rows under the old floor are already gone; lowering it needs the
        # partials, which means a re-ingest.
        self.assertFalse(compact_day.refloor_allowed(10, 5))

    def test_recovery_overwrites_an_old_final_from_staging(self):
        # On a refloor the final key already holds the OLD day when the crash
        # happens. Recovery must copy staging over it, not conclude from the
        # final key existing that the copy had completed.
        staging = f"curated/_staging/page_daily/dt={DAY}/day.parquet"
        self.set_day("compacting", key_staging=staging)
        new_body = day_parquet_bytes([500, 300])
        self.s3.put_object(Bucket=BUCKET, Key=staging, Body=new_body)
        row = self.table.get_item(Key={"source_hour": f"day#{DAY}"})["Item"]

        self.assertEqual(compact_day.finish_placement(
            self.s3, self.table, BUCKET, row, DAY), 0)
        placed = self.s3.get_object(Bucket=BUCKET, Key=DAY_OBJECT)["Body"].read()
        self.assertEqual(placed, new_body)
        self.assertEqual(self.day_status(), "compacted")


if __name__ == "__main__":
    unittest.main(verbosity=2)
