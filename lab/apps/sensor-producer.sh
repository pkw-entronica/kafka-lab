#!/usr/bin/env bash
# Scenario 08's sensors: RATE readings per second to topic "sensors", as JSON like {"sensor":"s3","temp":24}.
#
# Usage:  bash /apps/sensor-producer.sh [RATE=5]
# Start:  nohup bash /apps/sensor-producer.sh >/dev/null 2>&1 &
# Stop:   pkill -f sensor-producer.sh
R=${1:-5}
trap 'trap - TERM INT; kill 0' TERM INT

while true; do
  for _ in $(seq 1 "$R"); do printf '{"sensor":"s%d","temp":%d}\n' $((1 + RANDOM % 5)) $((18 + RANDOM % 13)); done
  sleep 1
done | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic sensors &
wait
