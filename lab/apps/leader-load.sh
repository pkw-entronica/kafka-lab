#!/usr/bin/env bash
# Shows which broker does the write work for a topic: all writes to a partition go to its LEADER,
# so this adds up the new messages of every partition per leader broker during a few seconds.
#
# Usage:  bash /apps/leader-load.sh TOPIC [SECONDS=15]
T=${1:?usage: leader-load.sh TOPIC [SECONDS]} S=${2:-15}

kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$T" 2>/dev/null | awk '
  { isp = 0; for (i = 1; i <= NF; i++) { if ($i == "Partition:") { isp = 1; p = $(i + 1) } if ($i == "Leader:") l = $(i + 1) }
    if (isp) print p, l }' > /tmp/.leader-load-leaders
kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" 2>/dev/null > /tmp/.leader-load-1
sleep "$S"
kafka-get-offsets.sh --bootstrap-server "$BOOTSTRAP" --topic "$T" 2>/dev/null > /tmp/.leader-load-2

awk 'FILENAME == ARGV[1] { lead[$1] = $2; next }
     FILENAME == ARGV[2] { split($0, a, ":"); o[a[2]] = a[3]; next }
     { split($0, a, ":"); w[lead[a[2]]] += a[3] - o[a[2]]; t += a[3] - o[a[2]] }
     END { for (b = 0; b < 3; b++)
             printf "broker %d leads partitions that received %6d writes  (%5.1f%%)\n", b, w[b] + 0, (t > 0) ? 100 * w[b] / t : 0 }' \
    /tmp/.leader-load-leaders /tmp/.leader-load-1 /tmp/.leader-load-2
