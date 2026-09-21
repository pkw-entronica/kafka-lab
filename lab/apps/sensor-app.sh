#!/usr/bin/env bash
# Scenario 08's application: reads topic "sensors" in group "sensor-app" and stores every temperature in
# /tmp/s08-readings.log. Like a pod with restartPolicy: Always, it starts again 5 s after every crash.
# What happens to the app (crashes, restarts, bad records) is logged to /tmp/s08-app.log.
#
#   (default)  release 1: a record without a numeric "temp" makes the app crash on the spot
#              (no clean shutdown, so the consumer doesn't commit its last position)
#   --dlq      release 2: such a record is copied to the dead letter topic "sensors-dlq", and the app carries on
#
# Usage:  bash /apps/sensor-app.sh [--dlq]
# Start:  nohup bash /apps/sensor-app.sh >/dev/null 2>&1 &
# Stop:   pkill -f sensor-app.sh
DLQ=false; [ "${1:-}" = --dlq ] && DLQ=true
trap 'trap - TERM INT; kill 0' TERM INT
re='"temp":(-?[0-9]+)[,}]'

run() {   # one life of the app: returns when it crashes
  kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic sensors --group sensor-app --from-beginning \
    --consumer-property client.id=sensor-app-1 --property print.offset=true 2>/dev/null \
  | while IFS=$'\t' read -r off val; do
      off=${off#Offset:}
      if [[ $val =~ $re ]]; then
        echo "$(date +%T) offset $off temp ${BASH_REMATCH[1]}" >> /tmp/s08-readings.log
      elif $DLQ; then
        echo "$val" | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic sensors-dlq 2>/dev/null
        echo "$(date +%T) bad record at offset $off sent to sensors-dlq: $val" >> /tmp/s08-app.log
      else
        echo "$(date +%T) CRASH: cannot parse the record at offset $off: $val" >> /tmp/s08-app.log
        pkill -9 -f client.id=sensor-app-1
        exit 1
      fi
    done
}

while true; do
  run & wait $!
  echo "$(date +%T) app restarting in 5 s" >> /tmp/s08-app.log
  sleep 5 & wait $!
done
