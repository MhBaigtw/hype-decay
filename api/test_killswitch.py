#!/usr/bin/env python3
"""
The API kill switch, against moto's Lambda and SNS.

    python3 api/test_killswitch.py
"""

import io
import os
import sys
import unittest
import zipfile
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")

import boto3  # noqa: E402
from moto import mock_aws  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent))
import killswitch  # noqa: E402


def alarm_event(state, name="hype-decay-api-runaway-traffic"):
    """The shape CloudWatch sends when an alarm invokes a Lambda directly."""
    return {"source": "aws.cloudwatch",
            "alarmData": {"alarmName": name,
                          "state": {"value": state, "reason": "Threshold Crossed: test"},
                          "previousState": {"value": "OK"}}}


@mock_aws
class KillSwitchTest(unittest.TestCase):

    def setUp(self):
        iam = boto3.client("iam")
        role = iam.create_role(RoleName="r", AssumeRolePolicyDocument="{}")["Role"]["Arn"]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("h.py", "def h(e, c): return 1")
        self.lam = boto3.client("lambda")
        self.lam.create_function(FunctionName="api-fn", Runtime="python3.12", Role=role,
                                 Handler="h.h", Code={"ZipFile": buf.getvalue()})
        self.topic = boto3.client("sns").create_topic(Name="alerts")["TopicArn"]
        os.environ["API_FUNCTION"] = "api-fn"
        os.environ["ALERT_TOPIC_ARN"] = self.topic
        os.environ["ALARM_NAME"] = "hype-decay-api-runaway-traffic"
        killswitch._clients.clear()

    def concurrency(self):
        return self.lam.get_function_concurrency(FunctionName="api-fn").get(
            "ReservedConcurrentExecutions")

    def test_alarm_sets_concurrency_to_zero_and_says_how_to_undo_it(self):
        out = killswitch.handler(alarm_event("ALARM"), None)
        self.assertEqual(out["action"], "disabled")
        self.assertEqual(self.concurrency(), 0)
        self.assertIn("delete-function-concurrency", out["message"])
        self.assertIn("api-fn", out["message"])

    def test_ok_and_other_states_do_nothing(self):
        for state in ("OK", "INSUFFICIENT_DATA"):
            self.assertEqual(killswitch.handler(alarm_event(state), None)["action"], "ignored")
        self.assertIsNone(self.concurrency())

    def test_a_different_alarm_is_ignored(self):
        out = killswitch.handler(alarm_event("ALARM", name="something-else"), None)
        self.assertEqual(out["action"], "ignored")
        self.assertIsNone(self.concurrency())


if __name__ == "__main__":
    unittest.main(verbosity=2)
