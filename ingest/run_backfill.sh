#!/bin/bash
# Runs the backfill detached from any login, then shuts the box down.
#
# Started on the instance as a transient systemd service, so it belongs to PID 1
# and not to an SSM command or anyone's session:
#
#   systemd-run --unit=hype-decay-backfill --collect \
#     /bin/bash /opt/hype-decay/ingest/run_backfill.sh [backfill.py args]
#
# Whatever the runner's exit code, the box shuts down when it ends, and it is set
# to TERMINATE on shutdown, so finishing early stops the bill early. The
# user_data timer stays armed as the backstop if this script itself hangs.
#
# The box's disk dies with it, so logs go to S3: days.jsonl and the main log
# every 10 minutes while it runs (so a timer kill still leaves a record), and
# everything, per-hour logs included, once at the end.
#
# Deliberately no `set -e`: nothing that fails here may stop the shutdown.

BUCKET="${HYPE_DECAY_BUCKET:-hype-decay-curated-820697996849}"
REGION=us-east-1
LOG=/var/log/hype-decay
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="s3://$BUCKET/logs/backfill/$RUN_ID"

mkdir -p "$LOG"
trap 'echo "shutting down $(date -u +%FT%TZ)" >> "$LOG/backfill.log"; shutdown -h now' EXIT

ship() {
    aws s3 cp "$LOG/backfill.log" "$DEST/backfill.log" --region $REGION --only-show-errors
    [ -f "$LOG/days.jsonl" ] && aws s3 cp "$LOG/days.jsonl" "$DEST/days.jsonl" \
        --region $REGION --only-show-errors
}

( while sleep 600; do ship; done ) &
shipper=$!

cd /opt/hype-decay/ingest
echo "run $RUN_ID, commit $(cat /opt/hype-decay/COMMIT), args: $*" > "$LOG/backfill.log"
python3.12 backfill.py --log-dir "$LOG" "$@" >> "$LOG/backfill.log" 2>&1
code=$?
echo "runner exit $code at $(date -u +%FT%TZ)" >> "$LOG/backfill.log"

kill $shipper 2>/dev/null
ship
[ -f "$LOG/summary.json" ] && aws s3 cp "$LOG/summary.json" "$DEST/summary.json" \
    --region $REGION --only-show-errors
tar -czf /var/tmp/backfill-logs.tgz -C "$LOG" . \
    && aws s3 cp /var/tmp/backfill-logs.tgz "$DEST/all-logs-exit$code.tgz" \
       --region $REGION --only-show-errors
# EXIT trap shuts the box down.
