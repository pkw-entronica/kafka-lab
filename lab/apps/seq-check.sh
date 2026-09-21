#!/usr/bin/env bash
# Checks a file of numbers (one per line, in the order a consumer read them) for the two things that
# break when a producer retries without idempotence: the same number twice, and a number that arrives
# after a bigger one.
#
# Usage:  bash /apps/seq-check.sh FILE
F=${1:?usage: seq-check.sh FILE}
[ -f "$F" ] || { echo "no file $F"; exit 1; }

awk '/^[0-9]+$/ {
      n++
      if (n > 1 && $1 < prev) { ooo++; if (ooo <= 3) ex = ex sprintf("\n  %s came after %s", $1, prev) }
      if (seen[$1]++) { dup++; if (dup <= 3) dx = dx sprintf("\n  %s appears again", $1) }
      prev = $1 }
    END { printf "%d records read, %d different\n", n + 0, n - dup
          printf "%d duplicates%s\n", dup + 0, dx
          printf "%d out of order%s\n", ooo + 0, ex }' "$F"
