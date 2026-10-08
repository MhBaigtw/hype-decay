#!/usr/bin/env python3
"""
The public API's Lambda handler, against moto's DynamoDB.

    python3 api/test_handler.py
"""

import json
import os
import sys
import unittest
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ["TABLE_NAME"] = "serving-test"

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import handler  # noqa: E402


def event(path, **query):
    return {"rawPath": path, "requestContext": {"http": {"method": "GET"}},
            "queryStringParameters": query or None}


def body(resp):
    return json.loads(resp["body"])


@mock_aws
class HandlerTest(unittest.TestCase):

    def setUp(self):
        ddb = boto3.resource("dynamodb")
        self.table = ddb.create_table(
            TableName="serving-test", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"},
                       {"AttributeName": "sk", "KeyType": "RANGE"}],
            AttributeDefinitions=[{"AttributeName": n, "AttributeType": "S"}
                                  for n in ("pk", "sk", "gsi1pk", "gsi1sk")],
            GlobalSecondaryIndexes=[{
                "IndexName": "by_title",
                "KeySchema": [{"AttributeName": "gsi1pk", "KeyType": "HASH"},
                              {"AttributeName": "gsi1sk", "KeyType": "RANGE"}],
                "Projection": {"ProjectionType": "ALL"}}])
        handler._table = None          # re-bind to the moto table
        for title, start, hl in (("Pope_Leo_XIV", "2025-05-08", 6),
                                 ("Pope_Francis", "2025-04-21", 40),
                                 ("Liam_Payne", "2024-10-16", 20),
                                 ("AC/DC", "2025-01-01", 30)):
            self.table.put_item(Item=handler.spike_item({
                "spike_id": f"{title}|{start}", "page_title": title, "spike_start": start,
                "onset_hour": f"{start} 17:00:00", "peak_hour": f"{start} 17:00:00",
                "peak_views": 1000, "peak_day_views": 30000, "baseline_hourly": 10.0,
                "attention_half_life_hours": hl, "long_tail_share": 0.1,
                "peak_hour_half_life_hours": 1, "total_excess_720h": 5000, "window_end": False,
            }, views=[1000, 600, 300] + [10] * 717))
        self.table.put_item(Item={"pk": "STATS", "sk": "STATS",
                                  "data": json.dumps({"qualifying_spikes": 3, "median_hours": 27})})
        for board in ("fastest", "slowest"):
            self.table.put_item(Item={"pk": f"LEADERBOARD#{board}", "sk": "LEADERBOARD",
                                      "data": json.dumps([{"page_title": "Liam_Payne"}])})

    def test_curve_round_trips_compactly(self):
        views = [3858371, 1253701, 0, 7] + [123] * 716
        blob = handler.encode_curve(views)
        self.assertEqual(handler.decode_curve(blob), views)
        self.assertLess(len(blob), 4 * 720)     # smaller than the raw uint32 array

    def test_search_is_a_case_and_space_insensitive_prefix_match(self):
        got = body(handler.handler(event("/api/search", q="pope "), None))["results"]
        self.assertEqual(sorted(r["page_title"] for r in got), ["Pope_Francis", "Pope_Leo_XIV"])
        got = body(handler.handler(event("/api/search", q="pope leo"), None))["results"]
        self.assertEqual([r["page_title"] for r in got], ["Pope_Leo_XIV"])
        self.assertNotIn("curve", got[0])           # search results never carry curves

    def test_search_needs_a_query(self):
        self.assertEqual(handler.handler(event("/api/search"), None)["statusCode"], 400)

    def test_spike_returns_summary_and_excess_curve(self):
        r = handler.handler(event("/api/spike", id="AC/DC|2025-01-01"), None)
        self.assertEqual(r["statusCode"], 200)
        b = body(r)
        self.assertEqual(b["page_title"], "AC/DC")
        self.assertEqual(len(b["excess"]), 720)
        self.assertEqual(b["excess"][:3], [990, 590, 290])   # views - baseline 10
        self.assertEqual(b["excess"][3], 0)                     # floored at 0
        self.assertIn("max-age", r["headers"]["cache-control"])

    def test_unknown_spike_is_404(self):
        r = handler.handler(event("/api/spike", id="Nope|2025-01-01"), None)
        self.assertEqual(r["statusCode"], 404)

    def test_summary_returns_stats_and_both_leaderboards(self):
        b = body(handler.handler(event("/api/summary"), None))
        self.assertEqual(b["stats"]["median_hours"], 27)
        self.assertEqual(b["fastest"][0]["page_title"], "Liam_Payne")
        self.assertIn("slowest", b)
        r = handler.handler(event("/api/summary"), None)
        self.assertIn("s-maxage=86400", r["headers"]["netlify-cdn-cache-control"])

    def test_unknown_route_is_404(self):
        self.assertEqual(handler.handler(event("/api/other"), None)["statusCode"], 404)


if __name__ == "__main__":
    unittest.main(verbosity=2)
