#!/usr/bin/env bash
# Checks a file of numbered messages (PREFIX-000001, PREFIX-000002, ...) written by a consumer:
# how many were read, and which numbers are MISSING in between.
#
# Usage:  bash /apps/gap-check.sh FILE PREFIX
F=${1:?usage: gap-check.sh FILE PREFIX} P=${2:?missing PREFIX}
[ -f "$F" ] || { echo "no file $F yet"; exit 1; }

total=$(grep -c "^$P-[0-9]" "$F" || true)
grep -o "^$P-[0-9]*" "$F" | sed "s/^$P-//" | sort -n -u | awk -v p="$P" -v total="$total" '
  NR == 1 { min = $1 }
  NR > 1 && $1 > prev + 1 { gaps++; missing += $1 - prev - 1
    if (gaps <= 5) list = list sprintf("\n  missing %s-%06d ... %s-%06d  (%d messages)", p, prev + 1, p, $1 - 1, $1 - prev - 1) }
  { prev = $1; n++ }
  END { if (n == 0) { print "0 messages read"; exit }
        printf "%d messages read (%d different), from %s-%06d to %s-%06d\n", total, n, p, min, p, prev
        printf "%d missing in between%s\n", missing + 0, list }'
