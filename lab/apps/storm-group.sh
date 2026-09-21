#!/usr/bin/env bash
# Scenario 06's application: 4 consumers (storm-1 ... storm-4) in group "storm-group", reading topic "events",
# with session.timeout.ms=10000 and heartbeat.interval.ms=3000.
#
# Usage:  bash /apps/storm-group.sh [--chaos] [--static] [--cooperative] [--kip848]
#   --chaos        every 5-10 s one random consumer crashes (kill -9) and is restarted 1 s later
#   --static       static membership: each consumer keeps its identity (group.instance.id) across restarts
#   --cooperative  CooperativeStickyAssignor: a rebalance only moves the partitions that have to move
#   --kip848       the new consumer group protocol (group.protocol=consumer), in its own group "storm-group-848"
# Start:  nohup bash /apps/storm-group.sh >/dev/null 2>&1 &
# Stop:   pkill -f storm-group.sh
# Crashes are logged to /tmp/s06-crashes.log.
CHAOS=false; STATIC=false; COOP=false; KIP848=false
for a in "$@"; do case "$a" in
  --chaos) CHAOS=true ;; --static) STATIC=true ;; --cooperative) COOP=true ;; --kip848) KIP848=true ;;
  *) echo "unknown option $a"; exit 1 ;; esac; done
trap 'trap - TERM INT; kill 0' TERM INT

GROUP=storm-group; $KIP848 && GROUP=storm-group-848
declare -a pid
start() {   # start N : start consumer storm-N in the background
  local props=(--consumer-property client.id=storm-$1 --consumer-property auto.commit.interval.ms=1000)
  if $KIP848; then
    props+=(--consumer-property group.protocol=consumer)       # timeouts and assignment are decided by the broker
  else
    props+=(--consumer-property session.timeout.ms=10000 --consumer-property heartbeat.interval.ms=3000)
    $COOP && props+=(--consumer-property partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor)
  fi
  $STATIC && props+=(--consumer-property group.instance.id=storm-$1)
  kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic events --group "$GROUP" "${props[@]}" >/dev/null 2>&1 &
  pid[$1]=$!
}

for i in 1 2 3 4; do start "$i"; done
if ! $CHAOS; then wait; exit 0; fi

while true; do
  sleep $((5 + RANDOM % 6)) & wait $!
  i=$((1 + RANDOM % 4))
  echo "$(date +%T) storm-$i crashed (kill -9), restarting it" >> /tmp/s06-crashes.log
  kill -9 "${pid[$i]}" 2>/dev/null
  sleep 1 & wait $!
  start "$i"
done
