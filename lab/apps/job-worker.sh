#!/usr/bin/env bash
# Scenario 07's worker: a consumer in group "job-workers" (topic "jobs") that needs 0.5 s per job.
# It has max.poll.interval.ms=10000: if it doesn't come back to poll() within 10 s, Kafka removes it from the group.
# Every finished job is logged to /tmp/s07-done.log as "time worker job-id"; Kafka's warnings go to /tmp/s07-NAME.err.
#
# Usage:  bash /apps/job-worker.sh NAME MAX_POLL_RECORDS
# Start:  nohup bash /apps/job-worker.sh w1 500 >/dev/null 2>&1 &
# Stop:   pkill -f job-worker.sh
N=${1:?usage: job-worker.sh NAME MAX_POLL_RECORDS} M=${2:?missing MAX_POLL_RECORDS}
trap 'trap - TERM INT; kill 0' TERM INT

kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic jobs --group job-workers --from-beginning \
  --consumer-property client.id="$N" \
  --consumer-property max.poll.interval.ms=10000 \
  --consumer-property max.poll.records="$M" \
  --consumer-property auto.commit.interval.ms=1000 2>> "/tmp/s07-$N.err" \
| while read -r job _; do
    sleep 0.5                                            # the "work" for one job
    echo "$(date +%T) $N $job" >> /tmp/s07-done.log
  done &
wait
