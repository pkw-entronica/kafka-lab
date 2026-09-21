#!/usr/bin/env bash
# Scenario 09's application: reads topic "ledger" in group "ledger-app" and "applies" every entry by appending
# its id to /tmp/s09-applied.log (a few ms of work each). Offsets are committed automatically every
# COMMIT_INTERVAL_MS (enable.auto.commit=true).
#   --idempotent  remember which entries were applied (like a unique key in a database) and skip any entry
#                 seen before; skipped entries go to /tmp/s09-skipped.log
#
# Usage:  bash /apps/ledger-consumer.sh [COMMIT_INTERVAL_MS=5000] [--idempotent]
# Start:            nohup bash /apps/ledger-consumer.sh 5000 >/dev/null 2>&1 &
# Stop cleanly:     pkill -f "group ledger-app"      (the consumer commits, the app finishes what it has, exits)
# Crash (kill -9):  pkill -9 -f ledger-consumer.sh; pkill -9 -f "group ledger-app"
I=${1:-5000}; IDEMPOTENT=false; [ "${2:-}" = --idempotent ] && IDEMPOTENT=true

kafka-console-consumer.sh --bootstrap-server "$BOOTSTRAP" --topic ledger --group ledger-app --from-beginning \
  --consumer-property enable.auto.commit=true --consumer-property auto.commit.interval.ms="$I" \
  --consumer-property max.poll.records=10 2>/dev/null \
| if $IDEMPOTENT; then
    declare -A seen
    if [ -f /tmp/s09-applied.log ]; then while read -r e; do seen[$e]=1; done < /tmp/s09-applied.log; fi
    while read -r e _; do
      if [ -n "${seen[$e]:-}" ]; then echo "$e" >> /tmp/s09-skipped.log; continue; fi
      seen[$e]=1; echo "$e" >> /tmp/s09-applied.log; sleep 0.005
    done
  else
    while read -r e _; do echo "$e" >> /tmp/s09-applied.log; sleep 0.005; done
  fi
