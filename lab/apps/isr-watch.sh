#!/usr/bin/env bash
# Watches replication health every few seconds:
#   under-replicated = a replica is missing from the ISR (it is behind, or its broker is down)
#   offline          = the partition has no leader at all, so it can't be written or read
#
# Usage:  bash /apps/isr-watch.sh [TOPIC] [SAMPLES=6] [SECONDS=5]      (no TOPIC = all topics)
T=${1:-} N=${2:-6} S=${3:-5}
args=(); [ -n "$T" ] && args=(--topic "$T")

for i in $(seq 1 "$N"); do
  kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe "${args[@]}" 2>/dev/null | awk -v t="$(date +%T)" '
    { isp = 0; leader = ""; rep = ""; isr = ""
      for (i = 1; i <= NF; i++) {
        if ($i == "Partition:") isp = 1
        if ($i == "Leader:")    leader = $(i + 1)
        if ($i == "Replicas:")  rep = $(i + 1)
        if ($i == "Isr:")       isr = $(i + 1) }
      if (!isp) next
      total++
      if (leader == "none" || leader == "-1") { off++; next }
      if (split(isr, b, ",") < split(rep, a, ",")) urp++ }
    END { printf "%s  partitions %4d   under-replicated %4d   offline %3d\n", t, total + 0, urp + 0, off + 0 }'
  if [ "$i" -lt "$N" ]; then sleep "$S"; fi
done
