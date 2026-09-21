#!/usr/bin/env bash
# Reset scenarios in one command - use it when you're done, or when a scenario got into a mess.
#   bash cleanup.sh 03        reset scenario 03 (01, 1 or 03-partition-increase-ordering all work)
#   bash cleanup.sh 01 03     reset several
#   bash cleanup.sh           reset every scenario
# For each scenario it stops its apps and Kafka tools in kafka-client, deletes its consumer groups and
# topics (and waits until their data has left the broker disks), removes its files in /tmp, and undoes
# what it changed on the cluster: throttles, client quotas, a scaled-down StatefulSet, a NetworkPolicy,
# and any Helm values a scenario overrode (04, 20, 29, 30).
ROOT="$(cd "$(dirname "$0")" && pwd)"
. "$ROOT/lab/lib.sh"

case "${1:-}" in -h|--help) sed -n '2,9s/^# \{0,1\}//p' "$0"; exit 0 ;; esac
kctl version --request-timeout=5s >/dev/null 2>&1 || die "cluster unreachable - is Docker Desktop running?"

# What each scenario creates:  processes to stop | consumer groups | topics
declare -A PROCS CGROUPS TOPICS
PROCS[01]="order-producer.sh|slow-consumer.sh orders|--topic orders";   CGROUPS[01]="orders-group";   TOPICS[01]="orders"
PROCS[02]="slow-consumer.sh payments|--topic payments";                 CGROUPS[02]="payments-group"; TOPICS[02]="payments"
PROCS[03]="account-producer.sh|account-consumers.sh|--topic accounts";  CGROUPS[03]="accounts-app";   TOPICS[03]="accounts"
PROCS[04]="--topic inventory";                                          CGROUPS[04]="";               TOPICS[04]="inventory"
PROCS[05]="slow-consumer.sh clicks|--topic clicks";                     CGROUPS[05]="clicks-group";   TOPICS[05]="clicks"
PROCS[06]="storm-group.sh|--topic events";                              CGROUPS[06]="storm-group storm-group-848"; TOPICS[06]="events"
PROCS[07]="job-worker.sh|numbered-producer.sh jobs|--topic jobs";      CGROUPS[07]="job-workers";    TOPICS[07]="jobs"
PROCS[08]="sensor-app.sh|sensor-producer.sh|--topic sensors";           CGROUPS[08]="sensor-app";     TOPICS[08]="sensors sensors-dlq"
PROCS[09]="ledger-consumer.sh|--topic ledger";                          CGROUPS[09]="ledger-app";     TOPICS[09]="ledger"
PROCS[10]="numbered-producer.sh audit|--topic audit";                   CGROUPS[10]="audit-report audit-new"; TOPICS[10]="audit"
PROCS[11]="produce-check.sh|--topic critical";                          CGROUPS[11]="";               TOPICS[11]="critical"
PROCS[12]="--topic docs";                                               CGROUPS[12]="";               TOPICS[12]="docs"
PROCS[13]="--topic firehose";                                           CGROUPS[13]="";               TOPICS[13]="firehose"
PROCS[14]="--topic bench";                                              CGROUPS[14]="";               TOPICS[14]="bench"
PROCS[15]="--topic replicated";                                         CGROUPS[15]="";               TOPICS[15]="replicated"
PROCS[16]="produce-check.sh|--topic orders-rf3|--topic cache-rf1";      CGROUPS[16]="";               TOPICS[16]="orders-rf3 cache-rf1"
PROCS[17]="produce-check.sh|--topic settlements";                       CGROUPS[17]="";               TOPICS[17]="settlements"
PROCS[18]="--topic filler";                                             CGROUPS[18]="";               TOPICS[18]="filler"
PROCS[19]="produce-check.sh|--topic netpart";                           CGROUPS[19]="";               TOPICS[19]="netpart"
PROCS[20]="--topic manyparts|--topic health-check";                     CGROUPS[20]="manyparts-app";  TOPICS[20]="manyparts health-check"
PROCS[21]="produce-check.sh|--topic rolling";                           CGROUPS[21]="";               TOPICS[21]="rolling"
PROCS[22]="batch-job.sh|--topic api-events|--topic batch-dump";         CGROUPS[22]="batch-job";      TOPICS[22]="api-events batch-dump"
PROCS[23]="--topic parts-";                                             CGROUPS[23]="";               TOPICS[23]="parts-1 parts-2 parts-3"
PROCS[24]="numbered-producer.sh short-lived|--topic short-lived";       CGROUPS[24]="nightly-report"; TOPICS[24]="short-lived"
PROCS[25]="--topic user-profile";                                       CGROUPS[25]="profile-cache";  TOPICS[25]="user-profile"
PROCS[26]="--topic corrupt-me";                                         CGROUPS[26]="";               TOPICS[26]="corrupt-me"
PROCS[27]="transactional-id|--topic tx-topic";                          CGROUPS[27]="";               TOPICS[27]="tx-topic"
PROCS[28]="--topic ordering";                                           CGROUPS[28]="";               TOPICS[28]="ordering ordering2 ordering3 ordering-idem"
PROCS[29]="produce-check.sh|--topic orders-app|--topic ordres";         CGROUPS[29]="";               TOPICS[29]="orders-app ordres odrers ordrs"
PROCS[30]="--topic external-test";                                      CGROUPS[30]="";               TOPICS[30]="external-test"
PROCS[31]="--topic secure-orders";                                      CGROUPS[31]="app-group";      TOPICS[31]="secure-orders"
PROCS[32]="produce-check.sh|--topic quorum-";                           CGROUPS[32]="";               TOPICS[32]="quorum-test quorum-ok-1 quorum-ok-2 quorum-lost"

nums=()
if [ $# -eq 0 ]; then nums=(01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32)
else
  for a in "$@"; do
    n="$(printf '%s' "$a" | grep -oE '^[0-9]+' || true)"
    [ -n "$n" ] && n="$(printf '%02d' "$((10#$n))")"
    [ -n "$n" ] && [ -n "${TOPICS[$n]:-}" ] || die "unknown scenario '$a' (use 01 .. 32)"
    nums+=("$n")
  done
fi

for n in "${nums[@]}"; do
  hdr "Cleanup: scenario $n"
  step "stopping its apps and Kafka tools in kafka-client"
  IFS='|' read -r -a pats <<<"${PROCS[$n]}"
  for p in "${pats[@]}"; do kx "kill_strays '$p'"; done
  # Undo the cluster-level changes first, so the cluster can delete the topics afterwards.
  case "$n" in
    15|17)
      step "removing replication throttles from the brokers"
      kx 'for b in 0 1 2; do kafka-configs.sh --bootstrap-server "$B" --entity-type brokers --entity-name $b \
            --alter --delete-config leader.replication.throttled.rate,follower.replication.throttled.rate >/dev/null 2>&1 || true
          done; echo "   throttles removed (if they were set)"' ;;
    13|22)
      step "removing the client quotas of batch-job"
      kx 'for c in producer_byte_rate consumer_byte_rate request_percentage; do
            kafka-configs.sh --bootstrap-server "$B" --alter --entity-type clients --entity-name batch-job \
              --delete-config $c >/dev/null 2>&1 || true
          done; echo "   quotas removed (if they were set)"' ;;
    11|16|26|32)
      reps="$(k get "statefulset/$BROKER_STS" -o jsonpath='{.spec.replicas}' 2>/dev/null || echo 3)"
      if [ "$reps" != "3" ]; then
        step "scaling the brokers back to 3"
        k scale "statefulset/$BROKER_STS" --replicas=3 >/dev/null
        wait_healthy 600 || true
      fi ;;
    19)
      if k get networkpolicy isolate-broker-2 >/dev/null 2>&1; then
        step "removing the NetworkPolicy isolate-broker-2"
        k delete networkpolicy isolate-broker-2 >/dev/null
        wait_healthy 600 || true
      fi ;;
    20)
      heap="$(k get "statefulset/$BROKER_STS" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_HEAP_OPTS")].value}' 2>/dev/null || true)"
      case "$heap" in *128m*)
        step "restoring lab/values.yaml: broker heap back to 512m (rolling restart, ~3 min)"
        helm_lab_upgrade
        wait_healthy 900 || true ;;
      esac ;;
    29)
      if [ "$(broker_config auto.create.topics.enable)" != "true" ]; then
        step "restoring lab/values.yaml: auto.create.topics.enable back to true (rolling restart, ~3 min)"
        helm_lab_upgrade
        wait_healthy 900 || true
      fi ;;
    30)
      if k get "svc/$RELEASE-controller-0-external" >/dev/null 2>&1; then
        step "removing the external NodePort listener (rolling restart, ~3 min)"
        helm_lab_upgrade
        wait_healthy 900 || true
      fi ;;
    31)
      # Nothing can talk to Kafka while the client listener requires SASL, so undo that first.
      lmap="$(k get "statefulset/$BROKER_STS" -o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP")].value}' 2>/dev/null || true)"
      case "$lmap" in *SASL*)
        step "restoring lab/values.yaml: PLAINTEXT client listener, no authorizer (rolling restart, ~3 min)"
        bad "any ACLs left behind stay in the metadata (inert without an authorizer) - remove them before reverting next time"
        helm_lab_upgrade
        wait_healthy 900 || true ;;
      esac ;;
  esac

  step "deleting its consumer groups and topics"
  # static members (scenario 06) don't leave the group when they stop: wait for their session timeout (10 s)
  [ "$n" = 06 ] && { echo "   waiting 12 s for static members to time out"; sleep 12; }
  for g in ${CGROUPS[$n]}; do kx "delete_group $g" || bad "group $g not deleted - run cleanup.sh $n again in a few seconds"; done
  for t in ${TOPICS[$n]}; do kx "delete_topic $t"; done
  files="/tmp/s$n-*"; for t in ${TOPICS[$n]}; do files="$files /tmp/numbered-$t-*"; done
  kx "rm -f $files"; echo "   removed its files in kafka-client: $files"
  if [ "$n" = 04 ]; then
    if [ "$(broker_config auto.leader.rebalance.enable)" != "true" ]; then
      step "restoring lab/values.yaml: auto.leader.rebalance.enable back to true (rolling restart, ~2 min)"
      helm_lab_upgrade
      wait_healthy 600
    fi
    step "preferred leader election, so leadership is balanced again"
    kx 'kafka-leader-election.sh --bootstrap-server "$B" --election-type PREFERRED --all-topic-partitions >/dev/null 2>&1 || true'
  fi
  if [ "$n" = 18 ]; then
    step "checking that every broker is healthy again (its disk was filled on purpose)"
    wait_healthy 300 || bad "a broker is still down - see scenarios/18-disk-full/README.md steps 13-14"
  fi
  for t in ${TOPICS[$n]}; do wait_topic_files_gone "$t" 180; done
  ok "scenario $n is reset"
done

hdr "Removing throwaway console-consumer-* groups (from commands run without --group)"
kx 'n=0; for g in $(kafka-consumer-groups.sh --bootstrap-server "$B" --list 2>/dev/null | grep "^console-consumer-"); do
      delete_group "$g"; n=$((n + 1)); done; [ $n -gt 0 ] || echo "   none"'

bash "$ROOT/lab/lab-status.sh" || true
