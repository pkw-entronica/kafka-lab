#!/usr/bin/env bash
# Turns the saved output of several kafka-producer-perf-test.sh runs into one comparison table.
#
# Usage:  bash /apps/perf-table.sh [FILES...]        (default: /tmp/s14-*.out)
files=("$@"); [ $# -eq 0 ] && files=(/tmp/s14-*.out)

printf '%-24s %10s %8s %8s %8s %8s\n' run msg/s MB/s 'avg ms' 'p95 ms' 'p99 ms'
for f in "${files[@]}"; do
  [ -f "$f" ] || continue
  n=$(basename "$f" .out)
  grep "records sent" "$f" | tail -1 | awk -v n="$n" '{
      for (i = 1; i <= NF; i++) {
        if ($(i + 1) == "records/sec")               rate = $i
        if ($i ~ /^\(/)                              mb  = substr($i, 2)
        if ($(i + 1) == "ms" && $(i + 2) == "avg")   avg = $i
        if ($(i + 2) ~ /^95th/)                      p95 = $i
        if ($(i + 2) ~ /^99th/)                      p99 = $i }
      printf "%-24s %10s %8s %8s %8s %8s\n", n, rate, mb, avg, p95, p99 }'
done
