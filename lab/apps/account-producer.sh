#!/usr/bin/env bash
# Scenario 03's event source: every second, the next event (seq=1, 2, 3 ...) for each of acct-1 ... acct-20,
# sent to topic "accounts" with the account id as key. The last seq is kept in /tmp/s03-seq.
#
# Start:   nohup bash /apps/account-producer.sh >/dev/null 2>&1 &
# Pause:   touch /tmp/s03-paused        Resume: rm /tmp/s03-paused
# Stop:    pkill -f account-producer.sh
trap 'trap - TERM INT; kill 0' TERM INT

k=$(cat /tmp/s03-seq 2>/dev/null || echo 0)
while true; do
  if [ -f /tmp/s03-paused ]; then sleep 1; continue; fi
  k=$((k + 1)); echo "$k" > /tmp/s03-seq
  for i in $(seq 1 20); do echo "acct-$i:seq=$k"; done
  sleep 1
done | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic accounts \
         --property parse.key=true --property key.separator=: \
         --producer-property metadata.max.age.ms=1000 &
wait
