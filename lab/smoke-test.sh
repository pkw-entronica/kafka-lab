#!/usr/bin/env bash
# Smoke test: create topic "smoke" (6 partitions, RF 3), produce 1000 keyed messages, consume them
# back in a group, verify count / uniqueness / zero lag, then delete the group and the topic.
# Idempotent: a leftover "smoke" topic or group from an earlier run is removed first.
. "$(dirname "$0")/lib.sh"

hdr "Smoke test: topic smoke, 6 partitions, RF 3, 1000 messages"
kx <<'EOF'
set -euo pipefail
B="$BOOTSTRAP"; T=smoke; G=smoke-group; N=1000

topic_exists() { kafka-topics.sh --bootstrap-server "$B" --list | grep -qx "$T"; }
delete_topic() {
  kafka-topics.sh --bootstrap-server "$B" --delete --topic "$T"
  for _ in $(seq 1 30); do topic_exists || return 0; sleep 1; done
  echo "!! topic $T still listed after 30s"; return 1
}

echo ">> cleanup from earlier runs (if any)"
kafka-consumer-groups.sh --bootstrap-server "$B" --delete --group "$G" >/dev/null 2>&1 || true
if topic_exists; then echo "   leftover topic $T found, deleting"; delete_topic; else echo "   nothing to clean"; fi

echo; echo ">> create topic $T"
kafka-topics.sh --bootstrap-server "$B" --create --topic "$T" --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server "$B" --describe --topic "$T"

echo; echo ">> produce $N keyed messages (key-i:msg-i, acks=all) so every partition/leader gets writes"
seq 1 "$N" | sed 's/.*/key-&:msg-&/' | kafka-console-producer.sh --bootstrap-server "$B" --topic "$T" \
  --property parse.key=true --property key.separator=: --producer-property acks=all

echo; echo ">> end offsets per partition (kafka-get-offsets.sh)"
kafka-get-offsets.sh --bootstrap-server "$B" --topic "$T" --time -1 | tee /tmp/smoke-offsets.txt
produced=$(awk -F: '{s+=$3} END{print s}' /tmp/smoke-offsets.txt)
echo "   total in log: $produced"

echo; echo ">> consume from the beginning in group $G"
kafka-console-consumer.sh --bootstrap-server "$B" --topic "$T" --group "$G" --from-beginning \
  --max-messages "$N" --timeout-ms 60000 --property print.key=true --property key.separator=: \
  > /tmp/smoke-consumed.txt 2>/tmp/smoke-consumer.err || true
grep -v '^Processed a total' /tmp/smoke-consumer.err | grep -v '^\s*$' || true
consumed=$(wc -l < /tmp/smoke-consumed.txt)
unique=$(sort -u /tmp/smoke-consumed.txt | wc -l)
mismatched=$(awk -F: '{k=$1; v=$2; sub(/^key-/,"",k); sub(/^msg-/,"",v); if (k!=v) n++} END{print n+0}' /tmp/smoke-consumed.txt)
missing=$(comm -23 <(seq 1 "$N" | sort) <(sed 's/^key-//; s/:.*//' /tmp/smoke-consumed.txt | sort) | wc -l)
echo "   consumed=$consumed unique=$unique missing=$missing key/value-mismatch=$mismatched"
head -3 /tmp/smoke-consumed.txt | sed 's/^/   sample: /'

echo; echo ">> committed offsets and lag for $G"
kafka-consumer-groups.sh --bootstrap-server "$B" --describe --group "$G" 2>&1 | grep -v '^\s*$'

echo
if [ "$produced" -eq "$N" ] && [ "$consumed" -eq "$N" ] && [ "$unique" -eq "$N" ] && [ "$missing" -eq 0 ] && [ "$mismatched" -eq 0 ]; then
  echo "RESULT: PASS - $N produced, $N consumed, no duplicates, nothing missing"
  rc=0
else
  echo "RESULT: FAIL - produced=$produced consumed=$consumed unique=$unique missing=$missing mismatched=$mismatched"
  rc=1
fi

echo; echo ">> cleanup: delete group $G and topic $T"
kafka-consumer-groups.sh --bootstrap-server "$B" --delete --group "$G"
delete_topic && echo "   topic $T deleted"
rm -f /tmp/smoke-*.txt /tmp/smoke-consumer.err
exit $rc
EOF
