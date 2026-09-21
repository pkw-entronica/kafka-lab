#!/usr/bin/env bash
# Scenario 03's order checker. Reads /tmp/s03-processed.log and counts events that were processed
# AFTER a newer event of the same account had already been processed.
#
# Usage:  bash /apps/order-check.sh            (the whole log)
#         bash /apps/order-check.sh 600        (only the last 600 events, i.e. ~30 s)
LOG=/tmp/s03-processed.log
[ -f "$LOG" ] || { echo "no $LOG yet - is account-consumers.sh running?"; exit 1; }

if [ -n "$1" ]; then tail -n "$1" "$LOG"; else cat "$LOG"; fi | awk '
  { k = $4; s = $5; sub("seq=", "", s); s += 0; n++
    if (!((k, $3) in seen)) { seen[k, $3] = 1; parts[k] = parts[k] " " $3 }
    if ((k in hi) && s < hi[k]) { bad[k]++; total++ }
    if (!(k in hi) || s > hi[k]) hi[k] = s }
  END {
    for (i = 1; i <= 20; i++) { k = "acct-" i
      if (k in bad) printf "  %-8s %4d events out of order   (read from partitions%s)\n", k, bad[k], parts[k] }
    printf "%d events checked, %d out of order\n", n, total + 0 }'
