#!/usr/bin/env bash
# Shows the state of a consumer group every few seconds (Stable, PreparingRebalance, CompletingRebalance, ...)
# and how many members it has - taken from kafka-consumer-groups.sh --describe --state.
#
# Usage:  bash /apps/group-state.sh GROUP [SAMPLES=10] [SECONDS=2]
G=${1:?usage: group-state.sh GROUP [SAMPLES] [SECONDS]} N=${2:-10} S=${3:-2}

for i in $(seq 1 "$N"); do
  read -r state members <<<"$(kafka-consumer-groups.sh --bootstrap-server "$BOOTSTRAP" --describe --group "$G" --state 2>/dev/null \
                              | awk -v g="$G" '$1 == g { print $(NF - 1), $NF }')"
  printf '%s  state %-20s members %s\n' "$(date +%T)" "${state:-?}" "${members:-?}"
  if [ "$i" -lt "$N" ]; then sleep "$S"; fi
done
