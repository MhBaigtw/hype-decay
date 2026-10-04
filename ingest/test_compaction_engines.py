#!/usr/bin/env python3
"""
The incremental compaction engine must produce exactly what the all-at-once
engine produces: same rows, same views, tables equal row for row.

Synthetic partials, built to hit the cases where a running aggregate could
drift: titles present in only some hours, titles in every hour, non-ASCII
titles, an hour with no rows at all, and counts large enough that a narrow
integer would overflow.

The real-data proof is compare_engines.py on 2025-08-28; this is the fast
check that runs with every change.

    python3 test_compaction_engines.py
"""

import datetime as dt
import io
import sys
import unittest
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent))
import compact_day  # noqa: E402

DAY = dt.date(2025, 8, 28)


def partial(rows):
    titles = [t for t, _ in rows]
    table = pa.Table.from_pydict({
        "project": ["en.wikipedia"] * len(rows),
        "page_title": titles,
        "dt": [DAY] * len(rows),
        "views": [v for _, v in rows],
        "hours_present": [1] * len(rows),
    }, schema=compact_day.PAGE_DAILY_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def synthetic_day():
    bodies = []
    for hour in range(24):
        rows = [("Every_Hour", 10 + hour), (f"Only_Hour_{hour}", 3),
                ("Zürich", hour % 3 + 1), ("東京", 7)]
        if hour % 2:
            rows.append(("Odd_Hours", 5))
        if hour == 13:
            rows = []                       # an hour with nothing in it
        if hour == 20:
            rows.append(("Huge", 3_000_000_000))   # beyond int32
        bodies.append(partial(rows))
    return bodies


class EngineTest(unittest.TestCase):

    def test_incremental_equals_all_at_once(self):
        bodies = synthetic_day()
        reference = compact_day.aggregate_arrow(bodies, DAY)
        incremental = compact_day.aggregate_incremental(iter(bodies), DAY)
        self.assertEqual(incremental.schema, reference.schema)
        self.assertTrue(incremental.equals(reference))

    def test_incremental_equals_the_python_baseline(self):
        bodies = synthetic_day()
        self.assertTrue(compact_day.aggregate_incremental(iter(bodies), DAY).equals(
            compact_day.aggregate_python(bodies, DAY)))

    def test_hours_present_and_views_add_up(self):
        out = compact_day.aggregate_incremental(iter(synthetic_day()), DAY).to_pydict()
        row = dict(zip(out["page_title"], zip(out["views"], out["hours_present"])))
        self.assertEqual(row["Every_Hour"], (sum(10 + h for h in range(24) if h != 13), 23))
        self.assertEqual(row["Odd_Hours"], (5 * 11, 11))   # odd hours bar 13
        self.assertEqual(row["Huge"], (3_000_000_000, 1))

    def test_takes_a_generator_so_partials_need_not_all_be_in_memory(self):
        consumed = []

        def lazily():
            for n, body in enumerate(synthetic_day()):
                consumed.append(n)
                yield body
        compact_day.aggregate_incremental(lazily(), DAY)
        self.assertEqual(consumed, list(range(24)))


if __name__ == "__main__":
    unittest.main(verbosity=2)
