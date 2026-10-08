"""
Public read API for the hype-decay page (Task 7).

    GET /api/summary              headline stats and both leaderboards
    GET /api/search?q=pope        spikes whose page title starts with q
    GET /api/spike?id=<spike_id>  one spike: its summary and 720-hour curve

Reads only the DynamoDB serving table, which scripts/load_serving.py fills from
fct_half_life. Never Athena: a single-spike Athena query costs about $0.10
(Task 5), and a page view here costs a few hundred-thousandths of that.

The spike id goes in a query parameter, not the path: titles contain "/"
(AC/DC), which a path parameter cannot carry reliably.
"""

import json
import os
import struct
import zlib
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key

MAX_RESULTS = 20
MAX_QUERY_LENGTH = 100

_table = None


def table():
    """Bound lazily so the loader can import this module without AWS."""
    global _table
    if _table is None:
        _table = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"])
    return _table


# --- the curve codec, shared with the loader ---------------------------------

def encode_curve(views):
    """Hourly views from onset as little-endian uint32, zlib-compressed. 720
    hours raw is 2,880 bytes; most of a curve is a long low tail, which zlib
    shrinks to a fraction of that."""
    return zlib.compress(struct.pack(f"<{len(views)}I", *views), 9)


def decode_curve(blob):
    raw = zlib.decompress(bytes(blob))
    return list(struct.unpack(f"<{len(raw) // 4}I", raw))


def normalise(title):
    """Search key: spaces and underscores are the same thing on Wikipedia."""
    return title.strip().replace(" ", "_").lower()


SUMMARY_FIELDS = ("page_title", "spike_start", "onset_hour", "peak_hour", "peak_views",
                  "peak_day_views", "baseline_hourly", "attention_half_life_hours",
                  "long_tail_share", "peak_hour_half_life_hours", "total_excess_720h",
                  "window_end")


def spike_item(summary, views):
    """The DynamoDB item for one spike (used by the loader, and by the tests)."""
    norm = normalise(summary["page_title"])
    item = {"pk": f"SPIKE#{summary['spike_id']}", "sk": "SPIKE",
            "gsi1pk": f"T#{norm[:1]}", "gsi1sk": f"{norm}#{summary['spike_start']}",
            "spike_id": summary["spike_id"], "curve": encode_curve(views)}
    for field in SUMMARY_FIELDS:
        value = summary.get(field)
        if value is None:
            continue
        item[field] = Decimal(str(value)) if isinstance(value, float) else value
    return item


# --- HTTP plumbing -----------------------------------------------------------

def plain(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, dict):
        return {k: plain(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain(v) for v in value]
    return value


def respond(status, payload, max_age=0, cdn_max_age=0):
    headers = {"content-type": "application/json",
               # The data changes at most once a day (Task 6), so browsers may
               # cache it; that is page views that never reach AWS.
               "cache-control": f"public, max-age={max_age}" if max_age else "no-store"}
    if cdn_max_age:
        # For Netlify's edge, which proxies /summary.json to this endpoint
        # (web/_redirects): cache it for the day, so the API is asked a few
        # times a day rather than once per page view.
        headers["netlify-cdn-cache-control"] = f"public, durable, s-maxage={cdn_max_age}"
    return {"statusCode": status, "headers": headers,
            "body": json.dumps(plain(payload), separators=(",", ":"))}


def summary():
    keys = [{"pk": "STATS", "sk": "STATS"},
            {"pk": "LEADERBOARD#fastest", "sk": "LEADERBOARD"},
            {"pk": "LEADERBOARD#slowest", "sk": "LEADERBOARD"}]
    got = boto3.resource("dynamodb").batch_get_item(
        RequestItems={table().name: {"Keys": keys}})["Responses"][table().name]
    by_pk = {i["pk"]: json.loads(i["data"]) for i in got}
    if "STATS" not in by_pk:
        return respond(503, {"error": "serving table not loaded"})
    return respond(200, {"stats": by_pk["STATS"],
                         "fastest": by_pk.get("LEADERBOARD#fastest", []),
                         "slowest": by_pk.get("LEADERBOARD#slowest", [])},
                   max_age=3600, cdn_max_age=86400)


def search(q):
    q = (q or "").strip()
    if not q or len(q) > MAX_QUERY_LENGTH:
        return respond(400, {"error": f"q must be 1-{MAX_QUERY_LENGTH} characters"})
    norm = normalise(q)
    got = table().query(
        IndexName="by_title",
        KeyConditionExpression=Key("gsi1pk").eq(f"T#{norm[:1]}") & Key("gsi1sk").begins_with(norm),
        Limit=MAX_RESULTS)["Items"]
    results = [{"spike_id": f"{i['page_title']}|{i['spike_start']}",
                "page_title": i["page_title"], "spike_start": i["spike_start"],
                "attention_half_life_hours": i.get("attention_half_life_hours"),
                "peak_day_views": i.get("peak_day_views"),
                "window_end": i.get("window_end", False)} for i in got]
    return respond(200, {"query": q, "results": results}, max_age=3600)


def spike(spike_id):
    if not spike_id or len(spike_id) > 400:
        return respond(400, {"error": "id is required"})
    item = table().get_item(Key={"pk": f"SPIKE#{spike_id}", "sk": "SPIKE"}).get("Item")
    if not item:
        return respond(404, {"error": "no such spike"})
    baseline = float(item.get("baseline_hourly", 0))
    views = decode_curve(item["curve"])
    out = {k: item[k] for k in SUMMARY_FIELDS if k in item}
    out["spike_id"] = spike_id
    out["views"] = views
    out["excess"] = [max(round(v - baseline), 0) for v in views]
    return respond(200, out, max_age=86400)


def handler(event, context):
    path = event.get("rawPath", "")
    params = event.get("queryStringParameters") or {}
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method != "GET":
        return respond(405, {"error": "GET only"})
    if path == "/api/summary":
        return summary()
    if path == "/api/search":
        return search(params.get("q"))
    if path == "/api/spike":
        return spike(params.get("id"))
    return respond(404, {"error": "not found"})
