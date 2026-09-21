#!/usr/bin/env bash
# Restart the Kafka brokers the safe way: one pod at a time, and only while the cluster is healthy.
#   wsl -d Ubuntu -- bash lab/safe-rolling-restart.sh          restart all three brokers
#   wsl -d Ubuntu -- bash lab/safe-rolling-restart.sh 1 2      restart only these brokers
# Before each pod it checks, and after each pod it waits for:
#   - all three brokers Ready
#   - 0 under-replicated partitions (every replica back in its ISR)
# It ends with a preferred leader election, so leadership is spread evenly again (scenario 04).
. "$(dirname "$0")/lib.sh"

ids=("$@"); [ $# -eq 0 ] && ids=(0 1 2)
"$KUBECTL" version --request-timeout=5s >/dev/null 2>&1 || die "cluster unreachable - is Docker Desktop running?"

hdr "Safe rolling restart of $BROKER_STS (brokers: ${ids[*]})"
step "checking that the cluster is healthy before touching anything"
wait_healthy 600 || die "the cluster is not healthy yet - fix that first, a restart now could take a partition offline"

for i in "${ids[@]}"; do
  pod="$BROKER_STS-$i"
  hdr "broker $i ($pod)"
  step "deleting the pod"
  k delete pod "$pod" --wait=false >/dev/null
  step "waiting for the StatefulSet to recreate it"
  for _ in $(seq 1 60); do k get "pod/$pod" >/dev/null 2>&1 && break; sleep 2; done
  step "waiting for $pod to be Ready"
  k wait --for=condition=Ready "pod/$pod" --timeout=600s >/dev/null || die "$pod did not come back"
  step "waiting for replication to catch up"
  wait_healthy 600 || die "replication did not catch up after restarting $pod - stopping here"
  ok "broker $i is back and the cluster is healthy again"
done

hdr "Preferred leader election"
kx 'kafka-leader-election.sh --bootstrap-server "$B" --election-type PREFERRED --all-topic-partitions >/dev/null 2>&1 || true'
ok "rolling restart finished: every broker restarted, never more than one at a time"
