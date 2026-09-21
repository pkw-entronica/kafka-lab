#!/usr/bin/env bash
# Sends numbered messages to a topic: PREFIX-000001, PREFIX-000002, ... (optionally followed by a payload,
# to make each message bigger). The next number is kept in /tmp/numbered-TOPIC-PREFIX.next.
#
# Usage:  bash /apps/numbered-producer.sh TOPIC PREFIX RATE [PAYLOAD_BYTES]           RATE per second, until stopped
#         bash /apps/numbered-producer.sh --burst COUNT TOPIC PREFIX [PAYLOAD_BYTES]  COUNT at once, then exit
# Start:  nohup bash /apps/numbered-producer.sh audit audit 5 >/dev/null 2>&1 &
# Stop:   pkill -f "numbered-producer.sh audit"
BURST=""; if [ "${1:-}" = --burst ]; then BURST=${2:?missing COUNT}; shift 2; fi
T=${1:?usage: numbered-producer.sh TOPIC PREFIX RATE [PAYLOAD_BYTES]} P=${2:?missing PREFIX}
if [ -n "$BURST" ]; then PAY=${3:-0}; else R=${3:?missing RATE}; PAY=${4:-0}; fi
NEXT=/tmp/numbered-$T-$P.next
PAD=""; [ "$PAY" -gt 0 ] && PAD=" $(head -c "$PAY" /dev/zero | tr '\0' x)"

emit() {   # emit N : the next N numbered lines
  local n; n=$(cat "$NEXT" 2>/dev/null || echo 1)
  echo $((n + $1)) > "$NEXT"
  for i in $(seq "$n" $((n + $1 - 1))); do printf '%s-%06d%s\n' "$P" "$i" "$PAD"; done
}

if [ -n "$BURST" ]; then
  emit "$BURST" | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$T"
  echo "sent $BURST messages to $T ($P-...)"
  exit 0
fi

trap 'trap - TERM INT; kill 0' TERM INT
while true; do emit "$R"; sleep 1; done | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" &
wait
