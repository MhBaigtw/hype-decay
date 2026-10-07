#!/usr/bin/env python3
"""
Publishing a day to Iceberg and retiring its staging copy, against moto.

The manifest must never claim a file that does not exist, and staging must never
be deleted for a day Iceberg does not hold. Tested here on the S3 and DynamoDB
side; the Athena DELETE + INSERT is exercised for real by republish_day.py.

    python3 test_publish.py
"""

import datetime as dt
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import publish  # noqa: E402
from compact_day import source_hours_for  # noqa: E402

BUCKET = "test-curated"
DAY = dt.date(2025, 3, 15)
NEIGHBOUR = DAY + dt.timedelta(days=1)


@mock_aws
class PublishTest(unittest.TestCase):

    def setUp(self):
        self.s3 = boto3.client("s3")
        self.s3.create_bucket(Bucket=BUCKET)
        self.table = boto3.resource("dynamodb").create_table(
            TableName="m", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "source_hour", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_hour", "AttributeType": "S"}])
        for day in (DAY, NEIGHBOUR):
            keys = publish.staging_keys(day)
            for k in keys:
                self.s3.put_object(Bucket=BUCKET, Key=k, Body=b"x")
            self.table.put_item(Item={"source_hour": f"day#{day}", "status": "compacted",
                                      "key_compacted": keys[0]})
            for h, k in zip(source_hours_for(day), keys[1:]):
                self.table.put_item(Item={"source_hour": h, "status": "done",
                                          "key_page_hour": k,
                                          "key_page_daily": f"curated/page_daily/dt={day}/part-{h}.parquet"})

    def row(self, key):
        return self.table.get_item(Key={"source_hour": key})["Item"]

    def present(self, day):
        return [k for k in publish.staging_keys(day)
                if self.s3.list_objects_v2(Bucket=BUCKET, Prefix=k).get("KeyCount")]

    def test_staging_keys_are_the_day_object_and_its_24_hours(self):
        keys = publish.staging_keys(DAY)
        self.assertEqual(keys[0], "curated/page_daily/dt=2025-03-15/day.parquet")
        self.assertEqual(len(keys), 25)
        # hour_start = source hour - 1, so the (D+1)T00 file is D hour 23.
        self.assertEqual(keys[-1], "curated/page_hour/dt=2025-03-15/hour=23/"
                                   "part-2025-03-16T00.parquet")
        self.assertEqual(keys[1], "curated/page_hour/dt=2025-03-15/hour=00/"
                                  "part-2025-03-15T01.parquet")

    def test_refuses_to_retire_an_unpublished_day(self):
        with self.assertRaises(publish.NotPublished):
            publish.retire_staging(self.s3, self.table, BUCKET, DAY)
        self.assertEqual(len(self.present(DAY)), 25)

    def test_publish_then_retire_touches_only_that_day(self):
        publish.mark_published(self.table, DAY)
        self.assertEqual(self.row(f"day#{DAY}")["iceberg_state"], "published")
        deleted = publish.retire_staging(self.s3, self.table, BUCKET, DAY)
        self.assertEqual(deleted, 25)
        self.assertEqual(self.present(DAY), [])
        self.assertEqual(len(self.present(NEIGHBOUR)), 25)

        day = self.row(f"day#{DAY}")
        self.assertIn("staging_removed_at", day)
        self.assertNotIn("key_compacted", day)           # no claim to a gone file
        for h in source_hours_for(DAY):
            hour = self.row(h)
            self.assertIn("staging_removed_at", hour)
            self.assertNotIn("key_page_hour", hour)
            self.assertNotIn("key_page_daily", hour)
        self.assertIn("key_page_hour", self.row(source_hours_for(NEIGHBOUR)[0]))

    def test_retire_is_idempotent(self):
        publish.mark_published(self.table, DAY)
        publish.retire_staging(self.s3, self.table, BUCKET, DAY)
        self.assertEqual(publish.retire_staging(self.s3, self.table, BUCKET, DAY), 0)

    def test_cannot_mark_a_day_that_is_not_compacted(self):
        self.table.put_item(Item={"source_hour": f"day#{DAY}", "status": "invalidated"})
        with self.assertRaises(publish.NotPublished):
            publish.mark_published(self.table, DAY)


if __name__ == "__main__":
    unittest.main(verbosity=2)
