#!/usr/bin/env python3
"""
Resume logic of the backfill runner, against moto's DynamoDB and CloudWatch.

The runner keeps no state of its own: every decision is read back from the
manifest. So the property to prove is that the manifest alone is enough --
kill the runner at any point, start it again, and every hour is finished
exactly once and every day is compacted exactly once.

Hours are processed by a fake that claims and finishes through the REAL
Manifest class, so the conditional writes that make a claim safe are the ones
being exercised, not a stand-in for them.

    python3 test_backfill_resume.py
"""

import datetime as dt
import os
import sys
import time
import unittest
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import backfill  # noqa: E402
from ingest_hour import LEASE_SECONDS, Manifest, hour_start_of, parse_source_hour  # noqa: E402

DAY = dt.date(2024, 9, 13)
HOURS = [f"2024-09-13T{h:02d}" for h in range(1, 24)] + ["2024-09-14T00"]


class Killed(Exception):
    """Stands in for the runner process dying mid-day."""


class Clock:
    def __init__(self):
        self.now = time.time()

    def __call__(self):
        return self.now

    def sleep(self, secs):
        self.slept = getattr(self, "slept", []) + [secs]
        self.now += secs


@mock_aws
class ResumeTest(unittest.TestCase):

    def setUp(self):
        ddb = boto3.resource("dynamodb")
        self.table = ddb.create_table(
            TableName="m", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "source_hour", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "source_hour",
                                   "AttributeType": "S"}])
        self.clock = Clock()
        # The same fake clock for claims and plans, so "the lease has lapsed"
        # means the same thing to both.
        self.manifest = Manifest(self.table, "test", clock=self.clock)
        self.finished = []          # every hour a worker carried to done
        self.compactions = []

    # --- fakes ---------------------------------------------------------------

    def ingest(self, die_after=None, fail=()):
        """A process_hours that claims and finishes through the real Manifest."""
        def process_hours(hours):
            results = []
            for n, hour in enumerate(hours):
                when = parse_source_hour(hour)
                if not self.manifest.claim(hour, "url", hour_start_of(when)):
                    results.append({"hour": hour, "code": 3})
                    continue
                if die_after is not None and n == die_after:
                    raise Killed(hour)      # claimed, in-flight, never finished
                if hour in fail:
                    self.manifest.fail(hour, "simulated 404")
                    results.append({"hour": hour, "code": "failed"})
                    continue
                self.manifest.finish(hour, {"views_kept": 1})
                self.finished.append(hour)
                results.append({"hour": hour, "code": 0})
            return results
        return process_hours

    def compact(self, day):
        self.compactions.append(day)
        self.table.put_item(Item={"source_hour": f"day#{day}", "status": "compacted"})
        return {"exit": 0}

    def run_day(self, process_hours):
        return backfill.run_day(self.manifest, DAY, process_hours, self.compact,
                                clock=self.clock, sleep=self.clock.sleep)

    def set_hour(self, hour, **item):
        self.table.put_item(Item={"source_hour": hour, **item})

    # --- the window ------------------------------------------------------------

    def test_window_is_730_days_and_17520_distinct_hours(self):
        days = backfill.window_days(backfill.WINDOW_START, backfill.WINDOW_END)
        self.assertEqual(len(days), 730)
        hours = [h for d in days for h in backfill.source_hours_for(d)]
        self.assertEqual(len(hours), 17_520)
        self.assertEqual(len(set(hours)), 17_520)
        # The trap: day D ends at (D+1)T00, whose file holds D 23:00-24:00.
        self.assertEqual(hours[0], "2024-09-13T01")
        self.assertEqual(hours[-1], "2026-09-13T00")

    # --- planning ----------------------------------------------------------------

    def test_fresh_day_plans_all_24(self):
        plan = backfill.plan_day(self.manifest, DAY, self.clock())
        self.assertEqual(plan.action, "ingest")
        self.assertEqual(plan.todo, HOURS)

    def test_done_hours_are_not_planned(self):
        for h in HOURS[:10]:
            self.set_hour(h, status="done")
        plan = backfill.plan_day(self.manifest, DAY, self.clock())
        self.assertEqual(plan.todo, HOURS[10:])

    def test_expired_lease_is_retaken_live_lease_is_left(self):
        self.set_hour(HOURS[0], status="in-flight", lease_expires=int(self.clock()) - 1)
        self.set_hour(HOURS[1], status="in-flight", lease_expires=int(self.clock()) + 30)
        plan = backfill.plan_day(self.manifest, DAY, self.clock())
        self.assertIn(HOURS[0], plan.todo)
        self.assertNotIn(HOURS[1], plan.todo)
        self.assertEqual(plan.held, [HOURS[1]])

    def test_failed_hours_retry_until_max_attempts(self):
        self.set_hour(HOURS[0], status="failed", attempt=backfill.MAX_ATTEMPTS - 1)
        self.set_hour(HOURS[1], status="failed", attempt=backfill.MAX_ATTEMPTS)
        plan = backfill.plan_day(self.manifest, DAY, self.clock())
        self.assertIn(HOURS[0], plan.todo)
        self.assertEqual(plan.exhausted, [HOURS[1]])

    def test_day_row_states(self):
        for status, action in (("compacted", "skip"), ("compacting", "resume-compaction"),
                               ("invalidated", "blocked"), ("failed", "blocked")):
            self.table.put_item(Item={"source_hour": f"day#{DAY}", "status": status})
            self.assertEqual(backfill.plan_day(self.manifest, DAY, self.clock()).action,
                             action, status)

    def test_all_done_means_compact(self):
        for h in HOURS:
            self.set_hour(h, status="done")
        self.assertEqual(backfill.plan_day(self.manifest, DAY, self.clock()).action,
                         "compact")

    # --- running a day -------------------------------------------------------

    def test_clean_day_ingests_24_then_compacts_once(self):
        report = self.run_day(self.ingest())
        self.assertEqual(sorted(self.finished), HOURS)
        self.assertEqual(self.compactions, [DAY])
        self.assertEqual(report["outcome"], "compacted")
        self.assertEqual(report["hours_ingested"], 24)

    def test_kill_and_restart_repeats_nothing_and_loses_nothing(self):
        # Run 1 dies with 9 hours finished and the 10th claimed but unfinished.
        with self.assertRaises(Killed):
            self.run_day(self.ingest(die_after=9))
        self.assertEqual(len(self.finished), 9)
        self.assertEqual(self.compactions, [])

        # Run 2 starts after the dead worker's lease has lapsed.
        self.clock.now += LEASE_SECONDS + 1
        report = self.run_day(self.ingest())
        self.assertEqual(sorted(self.finished), HOURS)            # nothing lost
        self.assertEqual(len(self.finished), len(set(self.finished)))  # nothing twice
        self.assertEqual(report["hours_ingested"], 15)
        self.assertEqual(self.compactions, [DAY])

        # Run 3 finds the day compacted and touches nothing.
        report = self.run_day(self.ingest())
        self.assertEqual(report["outcome"], "skipped")
        self.assertEqual(len(self.finished), 24)
        self.assertEqual(self.compactions, [DAY])

    def test_restart_inside_the_lease_waits_then_takes_the_hour(self):
        with self.assertRaises(Killed):
            self.run_day(self.ingest(die_after=3))
        # Immediate restart: the dead worker's hour still looks held.
        report = self.run_day(self.ingest())
        self.assertTrue(self.clock.slept, "should have waited out the lease")
        self.assertLessEqual(max(self.clock.slept), LEASE_SECONDS + 5)
        self.assertEqual(sorted(self.finished), HOURS)
        self.assertEqual(report["outcome"], "compacted")

    def test_killed_during_compaction_resumes_compaction_not_ingest(self):
        for h in HOURS:
            self.set_hour(h, status="done")
        self.table.put_item(Item={"source_hour": f"day#{DAY}", "status": "compacting"})
        report = self.run_day(self.ingest())
        self.assertEqual(self.finished, [])
        self.assertEqual(self.compactions, [DAY])
        self.assertEqual(report["outcome"], "compacted")

    def test_transient_failure_is_retried_within_the_run(self):
        flaky = {HOURS[5]}

        def once_then_ok(hours):
            out = self.ingest(fail=set(flaky))(hours)
            flaky.clear()
            return out
        report = self.run_day(once_then_ok)
        self.assertEqual(sorted(self.finished), HOURS)
        self.assertEqual(report["outcome"], "compacted")

    def test_exhausted_hour_leaves_day_uncompacted_and_says_so(self):
        dead = HOURS[7]
        report = self.run_day(self.ingest(fail={dead}))
        self.assertEqual(self.compactions, [])
        self.assertEqual(report["outcome"], "incomplete")
        self.assertEqual(report["exhausted"], [dead])
        row = self.table.get_item(Key={"source_hour": dead})["Item"]
        self.assertEqual(row["status"], "failed")
        self.assertEqual(int(row["attempt"]), backfill.MAX_ATTEMPTS)

    def test_failures_back_off_before_retrying(self):
        # A mirror blip must not burn all three attempts inside a few seconds.
        dead = HOURS[2]
        self.run_day(self.ingest(fail={dead}))
        self.assertEqual(self.clock.slept, list(backfill.RETRY_BACKOFF_SECONDS))

    # --- the second pass -------------------------------------------------------

    def test_final_pass_compacts_leftovers_and_explains_the_rest(self):
        d1, d2, d3, d4 = (DAY + dt.timedelta(days=n) for n in range(4))
        # d1 compacted already; d2 all done but never compacted (say the
        # compaction ran out of memory); d3 has a dead hour; d4 blocked.
        self.table.put_item(Item={"source_hour": f"day#{d1}", "status": "compacted"})
        for h in backfill.source_hours_for(d2) + backfill.source_hours_for(d3):
            self.set_hour(h, status="done")
        dead = backfill.source_hours_for(d3)[4]
        self.set_hour(dead, status="failed", attempt=backfill.MAX_ATTEMPTS,
                      error="HTTP Error 404: Not Found")
        self.table.put_item(Item={"source_hour": f"day#{d4}", "status": "invalidated"})

        left = backfill.final_pass(self.manifest, [d1, d2, d3, d4], self.compact,
                                   clock=self.clock)
        self.assertEqual(self.compactions, [d2])
        self.assertEqual(sorted(left), [str(d3), str(d4)])
        self.assertIn(dead, left[str(d3)])
        self.assertIn("404", left[str(d3)])
        self.assertIn("invalidated", left[str(d4)])

    # --- progress -------------------------------------------------------------

    def test_progress_lands_in_cloudwatch(self):
        cw = boto3.client("cloudwatch")
        backfill.publish_progress(cw, {"hours_ingested": 24, "hours_failed": 0,
                                       "outcome": "compacted", "day_secs": 60.0},
                                  days_remaining=729)
        names = {m["MetricName"] for m in
                 cw.list_metrics(Namespace=backfill.METRIC_NAMESPACE)["Metrics"]}
        self.assertIn("HoursIngested", names)
        self.assertIn("DaysRemaining", names)


if __name__ == "__main__":
    unittest.main(verbosity=2)
