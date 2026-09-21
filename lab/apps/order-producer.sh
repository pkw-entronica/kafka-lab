#!/usr/bin/env bash
# Scenario 01's shop: sends ~RATE orders per second to topic "orders", key = customer id.
#   normal  orders from many different customers
#   whale   ~90% of orders come from one customer: customer-BIG
#   salted  the whale's orders use customer-BIG-0 ... customer-BIG-9
#
# Usage:  bash /apps/order-producer.sh [normal|whale|salted] [RATE]
# Start:  nohup bash /apps/order-producer.sh normal >/dev/null 2>&1 &
# Stop:   pkill -f order-producer.sh
M=${1:-normal} R=${2:-180}
trap 'trap - TERM INT; kill 0' TERM INT

while true; do
  awk -v m="$M" -v n="$R" 'BEGIN { srand(); pad = sprintf("%200s", ""); gsub(/ /, "x", pad)
    for (i = 0; i < n; i++) {
      if (m == "normal" || rand() < 0.1) k = sprintf("customer-%05d", int(rand() * 100000))
      else if (m == "whale") k = "customer-BIG"
      else k = sprintf("customer-BIG-%d", int(rand() * 10))
      printf "%s:order amount=%d items=%s\n", k, int(rand() * 1000), pad } }'
  sleep 1
done | kafka-console-producer.sh --bootstrap-server "$BOOTSTRAP" --topic orders \
         --property parse.key=true --property key.separator=: &
wait
