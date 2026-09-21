#!/usr/bin/env bash
# Scenario 03's application: 2 consumers (s03-a and s03-b) in group "accounts-app", ~20 ms of work per event.
# Every processed event is logged to /tmp/s03-processed.log as:  time  consumer  partition  account  seq=N
#
# Start:  nohup bash /apps/account-consumers.sh >/dev/null 2>&1 &
# Stop:   pkill -f account-consumers.sh
trap 'trap - TERM INT; kill 0' TERM INT

consume() {
  kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic accounts --group accounts-app --from-beginning \
    --consumer-property client.id="$1" \
    --consumer-property metadata.max.age.ms=5000 \
    --consumer-property auto.commit.interval.ms=1000 \
    --consumer-property max.poll.records=50 \
    --property print.key=true --property print.partition=true 2>/dev/null \
  | while IFS=$'\t' read -r p k v; do
      sleep 0.02
      echo "$(date +%T.%3N) $1 ${p#Partition:} $k $v" >> /tmp/s03-processed.log
    done
}
consume s03-a &
consume s03-b &
wait
