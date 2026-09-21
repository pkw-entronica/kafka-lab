#!/usr/bin/env bash
# How much of a topic each broker actually stores on its disk (from kafka-log-dirs.sh).
# Replicas of the same partition should be the same size; a broker that is behind (or was never
# in sync) shows a smaller number.
#
# Usage:  bash /apps/replica-sizes.sh TOPIC
T=${1:?usage: replica-sizes.sh TOPIC}

kafka-log-dirs.sh --bootstrap-server "$BOOTSTRAP" --describe --topic-list "$T" 2>/dev/null \
| grep '"brokers"' | sed 's/{"broker":/\n/g' | awk -v t="$T" '
    NR > 1 {
      b = $0; sub(/,.*/, "", b)                       # the broker id comes first in each chunk
      n = 0; s = 0; rest = $0
      while (match(rest, /"size":[0-9]+/)) {
        s += substr(rest, RSTART + 7, RLENGTH - 7); n++
        rest = substr(rest, RSTART + RLENGTH) }
      printf "broker %-3s %3d replicas of %-16s %9.2f MB\n", b, n, t, s / 1048576 }'
