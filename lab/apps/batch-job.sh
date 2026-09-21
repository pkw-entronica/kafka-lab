#!/usr/bin/env bash
# Scenario 22's noisy neighbour: an export job that reads a whole topic over and over, as fast as the
# cluster lets it, under client.id=batch-job. It never commits offsets, so every pass starts from the
# oldest message again. Its speed is logged to /tmp/s22-batch.log.
#
# Usage:  bash /apps/batch-job.sh [TOPIC=batch-dump]
# Start:  nohup bash /apps/batch-job.sh >/dev/null 2>&1 &
# Stop:   pkill -f batch-job.sh
T=${1:-batch-dump}
trap 'trap - TERM INT; kill 0' TERM INT

CFG=/tmp/batch-job.properties
printf 'client.id=batch-job\nenable.auto.commit=false\nauto.offset.reset=earliest\n' > "$CFG"

while true; do
  kafka-consumer-perf-test.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" --messages 100000000 \
    --group batch-job --consumer.config "$CFG" --timeout 20000 >> /tmp/s22-batch.log 2>&1 &
  wait $!
done
