#!/usr/bin/env bash
# Counts partition leaders per broker, and partitions that are not led by their preferred leader
# (the preferred leader is the FIRST broker in the "Replicas:" list of kafka-topics.sh --describe).
#
# Usage:  bash /apps/leaders.sh [TOPIC]      (no topic = all topics)
args=(); [ -n "$1" ] && args=(--topic "$1")

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe "${args[@]}" 2>/dev/null | awk '
  { isp = 0
    for (i = 1; i <= NF; i++) { if ($i == "Partition:") isp = 1; if ($i == "Leader:") l = $(i + 1); if ($i == "Replicas:") r = $(i + 1) }
    if (!isp) next
    split(r, rr, ","); c[l]++; t++; if (l != rr[1]) np++ }
  END { for (b = 0; b < 3; b++) printf "broker %d leads %3d partitions\n", b, c[b] + 0
        printf "%d of %d partitions are NOT on their preferred leader\n", np + 0, t }'
