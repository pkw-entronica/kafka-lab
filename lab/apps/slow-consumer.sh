#!/usr/bin/env bash
# A consumer that does "work" for every record: it reads TOPIC in consumer group GROUP and spends
# DELAY seconds per record (0.01 = about 10 ms, so at most ~95 records/s).
#
# Usage:  bash /apps/slow-consumer.sh TOPIC GROUP NAME [DELAY]
# Start:  nohup bash /apps/slow-consumer.sh orders orders-group c1 0.01 >/dev/null 2>&1 &
# Stop:   pkill -f "slow-consumer.sh orders"
T=${1:?usage: slow-consumer.sh TOPIC GROUP NAME [DELAY]} G=${2:?missing GROUP} N=${3:?missing NAME} D=${4:-0.01}
trap 'trap - TERM INT; kill 0' TERM INT            # stopping this script also stops the consumer it started

kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" --group "$G" --from-beginning \
  --consumer-property client.id="$N" \
  --consumer-property auto.commit.interval.ms=1000 \
  --consumer-property max.poll.records=50 \
  --consumer-property metadata.max.age.ms=5000 \
  --consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor \
| while read -r _; do sleep "$D"; done &
wait
