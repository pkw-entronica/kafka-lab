#!/usr/bin/env bash
# One-shot, read-only health view of the lab:
# broker pods + nodes, broker disk usage, KRaft quorum, topics, under-replicated / at-min-ISR /
# unavailable partitions, and lag for every consumer group.
. "$(dirname "$0")/lib.sh"
T="${STATUS_TIMEOUT:-30}"   # seconds per Kafka CLI call, so a dead cluster can't hang the script

hdr "Broker pods (and the k8s node each runs on)"
k get pods -l "$BROKER_SELECTOR" -o wide
echo
k get pod "$CLIENT_POD" -o wide 2>/dev/null || bad "kafka-client pod not found"

hdr "Broker data disk usage (/bitnami/kafka, 1 GiB each)"
for pod in $(k get pods -l "$BROKER_SELECTOR" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.phase}{"\n"}{end}' \
             | awk '$2=="Running"{print $1}'); do
  printf '%-20s ' "$pod"
  k exec "$pod" -c kafka -- df -h /bitnami/kafka 2>/dev/null | awk 'NR==2{print "size="$2"  used="$3"  avail="$4"  use%="$5}' \
    || echo "(exec failed)"
done

{ echo "T=$T"; cat <<'EOF'
run() {   # run CMD... with a timeout; CMD may be a POD_LIB function
  local rc=0
  if declare -F "$1" >/dev/null; then timeout "$T" bash -c "B='$B'; $(declare -f "$1"); \"\$@\"" _ "$@" || rc=$?
  else timeout "$T" "$@" || rc=$?; fi
  [ $rc -eq 124 ] && echo "!! timed out after ${T}s (brokers unreachable?)"; return 0
}
section() { printf '\n==== %s ====\n' "$*"; }
count_or_ok() {   # print the lines, or 'none', plus a count
  local label="$1" out="$2"
  if [ -z "$out" ]; then echo "OK: no $label partitions"
  else echo "$out"; echo "!! $(printf '%s\n' "$out" | grep -c 'Partition:') $label partition(s)"; fi
}

section "KRaft controller quorum"
run kafka-metadata-quorum.sh --bootstrap-server "$BOOTSTRAP" describe --status 2>&1 | grep -vE '^\s*$'

section "Topics"
out="$(run kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list 2>&1)"
if [ -z "$out" ]; then echo "(no topics)"; else printf '%s\n' "$out"; fi

section "Partition leaders per broker (all topics)"
run leaders_by_broker | sed 's/^/   /'

section "Under-replicated partitions (ISR smaller than replica set)"
count_or_ok "under-replicated" "$(run kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --under-replicated-partitions 2>&1)"

section "At-min-ISR partitions (one more failure and acks=all writes stop)"
count_or_ok "at-min-ISR" "$(run kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --at-min-isr-partitions 2>&1)"

section "Unavailable / offline partitions (no leader)"
count_or_ok "unavailable" "$(run kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --unavailable-partitions 2>&1)"

section "Consumer group lag (all groups)"
out="$(run kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --all-groups 2>&1)"
if [ -z "$(printf '%s' "$out" | tr -d '[:space:]')" ]; then
  echo "(no consumer groups)"
else
  printf '%s\n' "$out"
  echo
  echo "TOTAL LAG per group:"
  printf '%s\n' "$out" | awk '$1!="GROUP" && NF>=6 && $6 ~ /^[0-9]+$/ {lag[$1]+=$6; n++}
                             END {for (g in lag) printf "  %-30s %d\n", g, lag[g]; if (!n) print "  (no committed offsets)"}'
fi
EOF
} | kx

hdr "Apps and Kafka tools running in kafka-client (started by scenarios)"
kx <<'EOF'
ps -eo etime,args | awk '
  /\/apps\// && !/awk/ { t = $1; sub(/.*\/apps\//, "/apps/"); printf "   %-10s app    %s\n", t, $0; n++; next }
  /ProducerPerformance|ConsoleConsumer|ConsoleProducer/ && !/awk/ {
    m = ""
    for (i = 2; i <= NF; i++) {
      if ($i ~ /(ProducerPerformance|ConsoleConsumer|ConsoleProducer)$/) { c = $i; sub(/.*\./, "", c); m = c }
      if ($i == "--topic" || $i == "--group") m = m " " $i " " $(i + 1)
      if ($i ~ /^client\.id=/) m = m " " $i }
    printf "   %-10s kafka  %s\n", $1, m; n++ }
  END { if (!n) print "   (nothing running)" }'
EOF
