#!/usr/bin/env bash
# Shows where NEW messages go: how many landed on each partition of a topic during a few seconds.
# (It diffs kafka-get-offsets.sh before and after.)
#
# Usage:  bash /apps/new-per-partition.sh TOPIC [SECONDS=10]
T=${1:?usage: new-per-partition.sh TOPIC [SECONDS]} S=${2:-10}

a=$(kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" 2>/dev/null | sort -t: -k2,2n)
sleep "$S"
b=$(kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" 2>/dev/null | sort -t: -k2,2n)

paste -d' ' <(echo "$a") <(echo "$b") | awk -F'[: ]' '
  { p[NR] = $2; v[NR] = $6 - $3; t += $6 - $3 }
  END { for (i = 1; i <= NR; i++) {
          n = (t > 0) ? int(40 * v[i] / t + 0.5) : 0; bar = ""; for (j = 0; j < n; j++) bar = bar "#"
          printf "partition %-3s %7d new  %5.1f%%  %s\n", p[i], v[i], (t > 0) ? 100 * v[i] / t : 0, bar } }'
