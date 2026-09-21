#!/usr/bin/env bash
# Prints the total lag of a consumer group every few seconds, and whether it is growing or draining.
# (Total lag = the LAG column of kafka-consumer-groups.sh --describe, summed over all partitions.)
#
# Usage:  bash /apps/lag-watch.sh GROUP [SAMPLES=6] [SECONDS=10]
G=${1:?usage: lag-watch.sh GROUP [SAMPLES] [SECONDS]} N=${2:-6} S=${3:-10}

prev=""; tprev=""
for i in $(seq 1 "$N"); do
  lag=$(kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group "$G" 2>/dev/null \
        | awk -v g="$G" '$1 == g && $6 ~ /^[0-9]+$/ { s += $6 } END { print s + 0 }')
  now=$(date +%s); trend=""
  if [ -n "$prev" ] && [ "$now" -gt "$tprev" ]; then
    r=$(( (lag - prev) / (now - tprev) ))
    if [ "$r" -gt 0 ]; then trend="GROWING by $r msg/s"
    elif [ "$r" -lt 0 ]; then trend="draining by $(( -r )) msg/s"
    else trend="flat"; fi
  fi
  printf '%s  total lag %9d   %s\n' "$(date +%T)" "$lag" "$trend"
  prev=$lag; tprev=$now
  if [ "$i" -lt "$N" ]; then sleep "$S"; fi
done
