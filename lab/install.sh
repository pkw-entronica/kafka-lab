#!/usr/bin/env bash
# Build (or re-apply) the Kafka failure lab. Idempotent: safe to re-run at any time.
. "$(dirname "$0")/lib.sh"
cd "$LAB_DIR"                 # relative paths also work for Windows helm.exe/kubectl.exe

hdr "1. Namespace $NS"
kctl create namespace "$NS" --dry-run=client -o yaml | kctl apply -f -

hdr "2a. Storage: real 1 GiB volumes (loop-mounted ext4) + static PVs"
bash ./node-disks.sh
kctl apply -f storage.yaml

hdr "2b. Helm release '$RELEASE' ($CHART $CHART_VERSION, values: lab/values.yaml)"
helm_lab_upgrade
# Chart quirk: on a fresh install the pod annotation checksum/secret is computed from a *different*
# random KRaft cluster-id than the one stored in the Secret; every later upgrade uses lookup() and is
# stable. So the first upgrade after install rolls all brokers once. Do that settling upgrade now,
# so re-running this script later (mid-scenario) never restarts Kafka unexpectedly.
if [ "$("$(helm_bin)" "${HCTX[@]}" -n "$NS" history "$RELEASE" -o json | grep -o '"revision":' | wc -l)" -eq 1 ]; then
  step "fresh install: one settling upgrade (expect a single rolling restart now)"
  helm_lab_upgrade
fi
wait_brokers_ready 5m
k get pods -l "$BROKER_SELECTOR" -o wide
k get pvc

hdr "4. kafka-client pod (the lab shell) with the helper apps from lab/apps mounted at /apps"
kctl -n "$NS" create configmap lab-apps --from-file=apps/ --dry-run=client -o yaml | kctl apply -f -
if ! k apply -f kafka-client.yaml 2>/tmp/kafka-client-apply.err; then
  if grep -q "Forbidden: pod updates may not change" /tmp/kafka-client-apply.err; then
    step "kafka-client spec changed in an immutable field: recreating the pod (it holds no state)"
    k delete pod "$CLIENT_POD" --wait=true
    k apply -f kafka-client.yaml
  else
    cat /tmp/kafka-client-apply.err >&2; exit 1
  fi
fi
k wait --for=condition=Ready pod/"$CLIENT_POD" --timeout=3m

hdr "3. Verify broker defaults (effective config of every broker)"
kx '
for id in 0 1 2; do
  echo "--- broker $id"
  kafka-configs.sh --bootstrap-server "$B" --entity-type brokers --entity-name "$id" --describe --all \
    | grep -E "^ *(default\.replication\.factor|min\.insync\.replicas|auto\.create\.topics\.enable|unclean\.leader\.election\.enable|auto\.leader\.rebalance\.enable)=" \
    | sed -E "s/ sensitive=.*//"
done'

hdr "5. kafka-ui (kafbat)"
k apply -f kafka-ui.yaml
k rollout status deploy/kafka-ui --timeout=5m

hdr "Done"
ok "lab is up. Next: bash lab/lab-status.sh   and   bash lab/smoke-test.sh"
echo "kafka-ui:  kubectl -n $NS port-forward svc/kafka-ui 8080:8080   ->  http://localhost:8080"
