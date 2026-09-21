#!/usr/bin/env bash
# Measures, over a few seconds, how fast messages come IN to a topic and go OUT through a consumer group.
#   in  = growth of the topic's end offsets (kafka-get-offsets.sh)
#   out = growth of the group's committed offsets (kafka-consumer-groups.sh --describe)
#
# Usage:  bash /apps/in-out.sh TOPIC GROUP [SECONDS=10]
T=${1:?usage: in-out.sh TOPIC GROUP [SECONDS]} G=${2:?missing GROUP} S=${3:-10}

ends() { kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" 2>/dev/null | awk -F: '{ s += $3 } END { print s + 0 }'; }
group() {
  kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group "$G" 2>/dev/null \
  | awk -v g="$G" '$1 == g { if ($4 ~ /^[0-9]+$/) c += $4; if ($6 ~ /^[0-9]+$/) l += $6 } END { print c + 0, l + 0 }'
}

e1=$(ends); read -r c1 l1 <<<"$(group)"; t1=$(date +%s)
sleep "$S"
e2=$(ends); read -r c2 l2 <<<"$(group)"; d=$(( $(date +%s) - t1 ))
echo "in:  $(( (e2 - e1) / d )) msg/s   (new messages written to $T)"
echo "out: $(( (c2 - c1) / d )) msg/s   (messages processed by $G)"
echo "lag: $l1 -> $l2"
