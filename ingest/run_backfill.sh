#!/bin/bash
# Runs the backfill detached from any login, then shuts the box down.
#
# Started on the instance as a transient systemd service, so it belongs to PID 1
# and not to an SSM command or anyone's session:
#
#   systemd-run --unit=hype-decay-backfill --collect -p OOMScoreAdjust=-950 \
#     /bin/bash /opt/hype-decay/ingest/run_backfill.sh [backfill.py args]
#
# RESTARTS. A run has FINISHED when backfill.py reaches its end and writes
# summary.json -- whatever its exit code, since 1 there only means some days
# were left uncompacted, which a restart cannot fix. Anything else (killed,
# crashed, out of memory) is abnormal, and the runner is restarted, up to 3
# times. The manifest makes a restart lose and repeat nothing.
#
# STALL ALARM. hype-decay-backfill-stall fires if no day is compacted for 20
# minutes. Terraform creates it disarmed. This script arms it only after the
# first day has compacted and the alarm reads OK -- arming any earlier either
# sends a false stall (no data yet reads as breaching) or, with ok_actions, a
# false recovery on every launch. If it has not read OK within 10 minutes of the
# first compaction it is armed anyway: a run that stalls straight after its
# first day must still report. Disarmed again only after a clean finish, so it
# stays silent between runs but fires if the box dies mid-run -- the case run 1
# went unnoticed in.
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
ALARM=hype-decay-backfill-stall
MAX_RESTARTS=3
LOG="${HYPE_DECAY_LOG:-/var/log/hype-decay}"
HOME_DIR="${HYPE_DECAY_HOME:-/opt/hype-decay}"
RESTART_DELAY="${HYPE_DECAY_RESTART_DELAY:-30}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
DEST="s3://$BUCKET/logs/backfill/$RUN_ID"

mkdir -p "$LOG"
echo -950 > /proc/$$/oom_score_adj 2>/dev/null   # below the runner's -900
trap 'echo "shutting down $(date -u +%FT%TZ)" >> "$LOG/backfill.log"; shutdown -h now' EXIT

ship() {
    aws s3 cp "$LOG/backfill.log" "$DEST/backfill.log" --region $REGION --only-show-errors
    [ -f "$LOG/days.jsonl" ] && aws s3 cp "$LOG/days.jsonl" "$DEST/days.jsonl" \
        --region $REGION --only-show-errors
}

( while sleep 600; do ship; done ) &
shipper=$!

cd "$HOME_DIR/ingest"
echo "run $RUN_ID, commit $(cat "$HOME_DIR/COMMIT"), args: $*" > "$LOG/backfill.log"
arm_after_first_compaction() {
    until grep -q '"outcome": "compacted"' "$LOG/days.jsonl" 2>/dev/null; do sleep 15; done
    local waited=0
    until [ "$(aws cloudwatch describe-alarms --alarm-names "$ALARM" --region $REGION \
               --query 'MetricAlarms[0].StateValue' --output text 2>/dev/null)" = OK ] \
          || [ $waited -ge 600 ]; do
        sleep 15; waited=$((waited + 15))
    done
    aws cloudwatch enable-alarm-actions --alarm-names "$ALARM" --region $REGION \
        && echo "stall alarm armed $(date -u +%FT%TZ), after the first compacted day" \
                "(waited ${waited}s for OK)" >> "$LOG/backfill.log"
}
arm_after_first_compaction &
armer=$!

attempt=0
finished=no
while :; do
    attempt=$((attempt + 1))
    rm -f "$LOG/summary.json"
    echo "attempt $attempt starting $(date -u +%FT%TZ)" >> "$LOG/backfill.log"
    python3.12 backfill.py --log-dir "$LOG" "$@" >> "$LOG/backfill.log" 2>&1
    code=$?
    echo "attempt $attempt: runner exit $code at $(date -u +%FT%TZ)" >> "$LOG/backfill.log"
    if [ -f "$LOG/summary.json" ]; then
        finished=yes
        break
    fi
    if [ $attempt -gt $MAX_RESTARTS ]; then
        echo "abnormal exit after $MAX_RESTARTS restarts; giving up" >> "$LOG/backfill.log"
        break
    fi
    echo "abnormal exit (no summary.json); restarting in ${RESTART_DELAY}s" >> "$LOG/backfill.log"
    ship
    sleep "$RESTART_DELAY"
done

kill $armer 2>/dev/null   # never arm after the run has ended
if [ $finished = yes ]; then
    aws cloudwatch disable-alarm-actions --alarm-names "$ALARM" --region $REGION \
        && echo "clean finish: stall alarm disarmed" >> "$LOG/backfill.log"
else
    echo "NOT finished: stall alarm left armed so it reports this" >> "$LOG/backfill.log"
fi

kill $shipper 2>/dev/null
ship
[ -f "$LOG/summary.json" ] && aws s3 cp "$LOG/summary.json" "$DEST/summary.json" \
    --region $REGION --only-show-errors
tar -czf /var/tmp/backfill-logs.tgz -C "$LOG" . \
    && aws s3 cp /var/tmp/backfill-logs.tgz "$DEST/all-logs-exit$code.tgz" \
       --region $REGION --only-show-errors
# EXIT trap shuts the box down.
