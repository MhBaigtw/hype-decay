#!/usr/bin/env python3
"""
Load the DynamoDB serving table from fct_half_life (Task 7).

Writes:
  SPIKE#<spike_id>        one per qualifying spike: summary fields + the hourly
                          views from onset for 720 hours (fewer if the data ends
                          first), zlib-compressed (api/handler.py encode_curve)
  STATS                   the headline numbers the page states in words
  LEADERBOARD#fastest     the 20 fastest-fading qualifying spikes
  LEADERBOARD#slowest     the 20 slowest

Idempotent: re-running overwrites every item by key and deletes spike items
that no longer qualify, so the table always equals the current model.

Athena reads go through the dbt workgroup (25 GiB cap); the curve query reads
the 720 hours per spike from int_spike_hours in one pass, and its CSV result is
fetched from S3 directly rather than paged through GetQueryResults.

    python3 scripts/load_serving.py
"""

import csv
import io
import json
import math
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "ingest"))
sys.path.insert(0, str(ROOT / "api"))
from athena import Athena, mib  # noqa: E402
from handler import spike_item  # noqa: E402

TABLE = "hype-decay-serving"
LEADERBOARD_SIZE = 20
# us-east-1 on-demand list prices (after the November 2024 reduction).
USD_PER_MILLION_WRU = 0.625
USD_PER_GB_MONTH = 0.25

SUMMARY_SQL = """
select f.spike_id, f.page_title, cast(f.spike_start as varchar) as spike_start,
       cast(f.onset_hour as varchar) as onset_hour, cast(f.peak_hour as varchar) as peak_hour,
       f.peak_views, f.peak_day_views, f.baseline_hourly, f.attention_half_life_hours,
       f.long_tail_share, f.peak_hour_half_life_hours, f.total_excess_720h, f.window_end,
       array_join(array_agg(h.views order by h.hour_start), ',') as curve
from fct_half_life f
join int_spike_hours h
  on h.spike_id = f.spike_id
 and h.hour_start >= f.onset_hour
 and h.hour_start <  f.onset_hour + interval '720' hour
where f.leaderboard_eligible
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
"""


def quartiles(a, col, where):
    r = a.run(f"""with v as (select array_sort(array_agg({col})) s from fct_half_life
        where {where} and {col} is not null)
        select cardinality(s),
               element_at(s, cast(ceil(cardinality(s) * 0.25) as integer)),
               element_at(s, cast(ceil(cardinality(s) * 0.50) as integer)),
               element_at(s, cast(ceil(cardinality(s) * 0.75) as integer)) from v""", fetch=True)["rows"][0]
    return {"n": int(r[0]), "q1": float(r[1]), "median": float(r[2]), "q3": float(r[3])}


def stats(a):
    t = a.run("""select count(*),
        sum(case when leaderboard_eligible then 1 else 0 end),
        sum(case when burst then 1 else 0 end),
        sum(case when rekindled then 1 else 0 end),
        sum(case when calendar_page then 1 else 0 end),
        sum(case when window_end then 1 else 0 end),
        sum(case when leaderboard_eligible and window_end then 1 else 0 end),
        count(distinct page_title),
        cast(min(spike_start) as varchar), cast(max(spike_start) as varchar)
        from fct_half_life""", fetch=True)["rows"][0]
    total, qual, burst, rek, cal, wend, qual_wend, pages, first, last = t
    big = "peak_day_excess >= 20000 and not burst and not calendar_page"
    big_n, big_rek = a.run(f"""select count(*), sum(case when rekindled then 1 else 0 end)
        from fct_half_life where {big}""", fetch=True)["rows"][0]
    examples = {r[0]: {"page_title": r[0], "spike_start": r[1], "attention_half_life_hours": int(r[2]),
                       "peak_hour_half_life_hours": int(r[3]), "peak_views": int(r[4]),
                       "peak_day_views": int(r[5])}
                for r in a.run("""select page_title, cast(spike_start as varchar), attention_half_life_hours,
                    peak_hour_half_life_hours, peak_views, peak_day_views from fct_half_life
                    where spike_id in ('Pope_Leo_XIV|2025-05-08', 'Liam_Payne|2024-10-16',
                                       'Donald_Trump|2024-11-05')""", fetch=True)["rows"]}
    return {
        "spikes_detected": int(total), "pages_with_spikes": int(pages),
        "qualifying_spikes": int(qual), "qualifying_censored": int(qual_wend),
        "attention_half_life": quartiles(a, "attention_half_life_hours", "leaderboard_eligible"),
        "attention_half_life_including_rekindled": quartiles(
            a, "attention_half_life_hours", big),
        "long_tail_share": quartiles(a, "long_tail_share", "leaderboard_eligible"),
        "peak_hour_half_life": quartiles(a, "peak_hour_half_life_hours", "leaderboard_eligible"),
        "burst": int(burst), "rekindled": int(rek), "calendar_page": int(cal),
        # The rekindled spikes actually left out of the headline: those big
        # enough to qualify otherwise (not the 15.1% share of ALL spikes).
        "big_spikes": int(big_n), "rekindled_among_big": int(big_rek),
        "window_end": int(wend), "window_end_share": int(wend) / int(total),
        "first_spike": first, "last_spike": last,
        "data_window": ["2024-09-13", "2026-09-12"],
        "examples": examples,
        "loaded_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def leaderboard(a, order):
    rows = a.run(f"""select spike_id, page_title, cast(spike_start as varchar), attention_half_life_hours,
        long_tail_share, peak_day_views from fct_half_life
        where leaderboard_eligible and attention_half_life_hours is not null
        order by attention_half_life_hours {order}, peak_day_excess desc limit {LEADERBOARD_SIZE}""",
                 fetch=True)["rows"]
    return [{"spike_id": r[0], "page_title": r[1], "spike_start": r[2],
             "attention_half_life_hours": int(r[3]), "long_tail_share": round(float(r[4]), 3),
             "peak_day_views": int(r[5])} for r in rows]


def item_size(item):
    """DynamoDB's item size: attribute names plus values (numbers ~1 byte per 2
    digits + 1; binary and strings by length)."""
    size = 0
    for k, v in item.items():
        size += len(k.encode())
        if isinstance(v, (bytes, bytearray)):
            size += len(v)
        elif isinstance(v, str):
            size += len(v.encode())
        elif isinstance(v, bool):
            size += 1
        elif isinstance(v, (int, float, Decimal)):
            size += math.ceil(len(str(v).replace("-", "").replace(".", "")) / 2) + 1
    return size


def main():
    a = Athena(database="hype_decay_dbt", workgroup="hype-decay-dbt")
    session = boto3.Session(profile_name=os.environ.get("AWS_PROFILE"), region_name="us-east-1")
    s3 = session.client("s3")
    table = session.resource("dynamodb").Table(TABLE)

    started = time.time()
    r = a.run(SUMMARY_SQL)
    loc = a.client.get_query_execution(QueryExecutionId=r["id"])["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
    bucket, key = loc[5:].split("/", 1)
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
    csv.field_size_limit(10_000_000)
    rows = list(csv.DictReader(io.StringIO(body)))
    print(f"curve query: {len(rows):,} qualifying spikes, scanned {mib(r['scanned'])}, "
          f"result {len(body) / 2**20:.1f} MiB, {time.time() - started:.0f}s")

    items = []
    for row in rows:
        views = [int(v) for v in row.pop("curve").split(",")]
        summary = {k: v for k, v in row.items()}
        for k in ("peak_views", "peak_day_views", "attention_half_life_hours",
                  "peak_hour_half_life_hours"):
            summary[k] = int(summary[k]) if summary[k] not in ("", None) else None
        for k in ("baseline_hourly", "long_tail_share", "total_excess_720h"):
            summary[k] = float(summary[k]) if summary[k] not in ("", None) else None
        summary["window_end"] = summary["window_end"] == "true"
        items.append(spike_item(summary, views))

    st = stats(a)
    fastest, slowest = leaderboard(a, "asc"), leaderboard(a, "desc")
    items += [{"pk": "STATS", "sk": "STATS", "data": json.dumps(st)},
              {"pk": "LEADERBOARD#fastest", "sk": "LEADERBOARD", "data": json.dumps(fastest)},
              {"pk": "LEADERBOARD#slowest", "sk": "LEADERBOARD", "data": json.dumps(slowest)}]

    sizes = [item_size(i) for i in items]
    largest = max(range(len(items)), key=lambda n: sizes[n])
    curve_lengths = [len(i["curve"]) for i in items if "curve" in i]
    # One WRU per started KB of each item, plus one per spike for the GSI copy
    # (its projected attributes are well under 1 KB).
    wru = sum(math.ceil(s / 1024) for s in sizes) + len(rows)

    # Stale spike items (no longer qualifying) are deleted, so reloads converge.
    keep = {(i["pk"], i["sk"]) for i in items}
    existing, kwargs = [], {"ProjectionExpression": "pk, sk"}
    while True:
        page = table.scan(**kwargs)
        existing += [(i["pk"], i["sk"]) for i in page["Items"]]
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    stale = [k for k in existing if k not in keep]

    with table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
        for item in items:
            batch.put_item(Item=item)
        for pk, sk in stale:
            batch.delete_item(Key={"pk": pk, "sk": sk})

    total_bytes = sum(sizes)
    print(f"items written: {len(items):,} ({len(rows):,} spikes + STATS + 2 leaderboards); "
          f"stale deleted: {len(stale):,}")
    print(f"largest item: {items[largest]['pk']} {sizes[largest]:,} bytes "
          f"= {100 * sizes[largest] / (400 * 1024):.2f}% of DynamoDB's 400 KB limit")
    print(f"spike items: mean {sum(sizes[:len(rows)]) / len(rows):,.0f} bytes; compressed curve "
          f"mean {sum(curve_lengths) / len(curve_lengths):,.0f} bytes (raw 2,880); "
          f"table data {total_bytes / 2**20:.1f} MiB")
    print(f"write cost: about {wru:,} WRU = ${wru / 1e6 * USD_PER_MILLION_WRU:.4f}; "
          f"Athena {mib(a.scanned_total)} = ${a.scanned_total / 2**40 * 5:.4f}; "
          f"storage about ${total_bytes / 2**30 * USD_PER_GB_MONTH:.4f}/month")
    print(f"headline: {st['qualifying_spikes']:,} qualifying spikes, median attention half-life "
          f"{st['attention_half_life']['median']:.0f} h")
    return 0


if __name__ == "__main__":
    sys.exit(main())
