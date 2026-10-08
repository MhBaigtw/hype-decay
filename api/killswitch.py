"""
Kill switch for the public API (Task 6 hardening).

Invoked DIRECTLY by the runaway-traffic CloudWatch alarm (an alarm Lambda
action, no SNS hop). On ALARM it sets the API Lambda's reserved concurrency to
0 -- API Gateway then answers every request with an error and nothing behind it
runs or bills -- and emails what it did, and how to undo it, through the
existing alerts topic. On any other state, or any other alarm, it does nothing.

Why concurrency 0 and not deleting or disabling the stage: it is one API call
that changes no infrastructure Terraform owns as a resource, it is instant, and
undoing it is one command:

    aws lambda delete-function-concurrency --function-name hype-decay-api --profile hype-decay-deploy

Restoring does not re-arm anything by itself: if the crawler is still there the
alarm fires again and the switch trips again, which is the point.
"""

import json
import os

import boto3

_clients = {}


def client(name):
    if name not in _clients:
        _clients[name] = boto3.client(name)
    return _clients[name]


def handler(event, context):
    alarm = (event.get("alarmData") or {})
    name = alarm.get("alarmName")
    state = (alarm.get("state") or {}).get("value")
    function = os.environ["API_FUNCTION"]

    if name != os.environ["ALARM_NAME"] or state != "ALARM":
        return {"action": "ignored", "alarm": name, "state": state}

    client("lambda").put_function_concurrency(FunctionName=function,
                                              ReservedConcurrentExecutions=0)
    restore = (f"aws lambda delete-function-concurrency --function-name {function} "
               f"--profile hype-decay-deploy")
    message = (
        f"The public API was SWITCHED OFF automatically.\n\n"
        f"Alarm {name} went to ALARM: {(alarm.get('state') or {}).get('reason', '')}\n\n"
        f"The API Lambda '{function}' now has reserved concurrency 0, so every API\n"
        f"request is refused and nothing behind it runs or bills. The page's\n"
        f"summary still loads from Netlify's cache; search and curves do not.\n\n"
        f"To switch it back on, once the traffic has stopped:\n\n    {restore}\n")
    client("sns").publish(TopicArn=os.environ["ALERT_TOPIC_ARN"],
                          Subject="hype-decay: public API switched off (runaway traffic)",
                          Message=message)
    print(json.dumps({"action": "disabled", "function": function, "alarm": name}))
    return {"action": "disabled", "function": function, "message": message}
