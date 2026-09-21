#!/usr/bin/env bash
# Sends one message per second to a topic and prints whether Kafka accepted it - the client's view of
# a broken cluster. The producer does not retry (retries=0), so you see the real error each second.
#
# Usage:  bash /apps/produce-check.sh TOPIC [SECONDS=10] [ACKS=all]     ACKS: all | 1 | 0
T=${1:?usage: produce-check.sh TOPIC [SECONDS] [ACKS]} N=${2:-10} A=${3:-all}
LABEL=$A; [ "$A" = all ] && A=-1

CFG=/tmp/produce-check.properties
# linger.ms=0 is not the default any more (Kafka 4.0 made it 5), and the producer refuses to start
# unless delivery.timeout.ms >= linger.ms + request.timeout.ms.
printf 'enable.idempotence=false\nretries=0\nlinger.ms=0\nmax.block.ms=5000\nrequest.timeout.ms=5000\ndelivery.timeout.ms=5000\n' > "$CFG"

kafka-verifiable-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" --max-messages "$N" \
  --throughput 1 --acks "$A" --producer.config "$CFG" 2>/tmp/produce-check.err \
| { ok=0; err=0
    while IFS= read -r line; do
      case "$line" in
        *producer_send_success*) ok=$((ok + 1)); printf '%s  ok\n' "$(date +%T)" ;;
        *producer_send_error*)   err=$((err + 1))
          e=$(printf '%s' "$line" | sed -n 's/.*"exception":"\([^"]*\)".*/\1/p')
          printf '%s  ERROR %s\n' "$(date +%T)" "${e##*.}" ;;
      esac
    done
    echo "---"
    if [ $((ok + err)) -eq 0 ]; then
      echo "!! the producer never started - it sent nothing, so this is not a Kafka verdict:"
      sed -n '1,3p' /tmp/produce-check.err
    fi
    echo "$ok accepted, $err rejected (acks=$LABEL)"; }
