#!/usr/bin/env bash
# Shared helpers for the lab's own scripts (install.sh, lab-status.sh, smoke-test.sh, node-disks.sh, cleanup.sh).
# The scenarios don't use this file: they are plain commands in scenarios/NN-name/README.md.
set -euo pipefail
# Git Bash (MSYS) rewrites bare /paths in arguments into Windows paths, which breaks commands meant
# for the containers (e.g. df -h /bitnami/kafka). Harmless on WSL and Linux.
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$LAB_DIR")"
NS="${NS:-kafka-lab}"
RELEASE="${RELEASE:-kafka}"
CHART="oci://registry-1.docker.io/bitnamicharts/kafka"
CHART_VERSION="32.4.3"
BOOTSTRAP="${BOOTSTRAP:-kafka.kafka-lab.svc.cluster.local:9092}"
CLIENT_POD="${CLIENT_POD:-kafka-client}"
BROKER_STS="${BROKER_STS:-kafka-controller}"
BROKER_SELECTOR="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=controller-eligible"
KIND_NODE="${KIND_NODE:-kind-control-plane}"

# ---- output helpers (colour only when writing to a terminal) ----
if [ -t 1 ]; then C_H=$'\e[1;36m'; C_S=$'\e[1;33m'; C_OK=$'\e[1;32m'; C_BAD=$'\e[1;31m'; C_0=$'\e[0m'
else C_H=''; C_S=''; C_OK=''; C_BAD=''; C_0=''; fi
hdr()  { printf '\n%s==== %s ====%s\n' "$C_H" "$*" "$C_0"; }
step() { printf '%s>> %s%s\n' "$C_S" "$*" "$C_0"; }
ok()   { printf '%sOK: %s%s\n' "$C_OK" "$*" "$C_0"; }
bad()  { printf '%s!! %s%s\n' "$C_BAD" "$*" "$C_0"; }
die()  { bad "$*"; exit 1; }

# ---- tool discovery: native Linux/macOS binaries, or Windows .exe through WSL interop ----
pick() {
  local c
  for c in "$@"; do command -v "$c" >/dev/null 2>&1 && { echo "$c"; return 0; }; done
  echo "ERROR: none of [$*] found on PATH" >&2; return 1
}
KUBECTL="${KUBECTL:-$(pick kubectl kubectl.exe)}"
# The lab always talks to its own cluster, whatever kubectl's current context is - you may well have
# other kind clusters. Override with KUBE_CONTEXT=... if you renamed it.
KUBE_CONTEXT="${KUBE_CONTEXT:-kind-kind}"
KCTX=(--context "$KUBE_CONTEXT"); HCTX=(--kube-context "$KUBE_CONTEXT")   # for kubectl / for helm
if ! "$KUBECTL" config get-contexts -o name 2>/dev/null | grep -qx -- "$KUBE_CONTEXT"; then
  KCTX=(); HCTX=()
  echo "!! kube context '$KUBE_CONTEXT' not found - falling back to the current one ($("$KUBECTL" config current-context 2>/dev/null))" >&2
fi
kctl() { "$KUBECTL" "${KCTX[@]}" "$@"; }
helm_bin()   { pick helm helm.exe; }
# Docker Desktop puts a 'docker' stub into WSL distros without integration; only use one that works.
docker_bin() {
  local c
  for c in docker docker.exe; do
    command -v "$c" >/dev/null 2>&1 && "$c" info >/dev/null 2>&1 && { echo "$c"; return 0; }
  done
  echo "ERROR: no working docker CLI found" >&2; return 1
}
k() { kctl -n "$NS" "$@"; }

# ---------------------------------------------------------------------------------------------
# POD_LIB: bash functions available inside every kx snippet (they run in kafka-client).
# ---------------------------------------------------------------------------------------------
read -r -d '' POD_LIB <<'POD_LIB_EOF' || true
B="$BOOTSTRAP"
topic_exists() { kafka-topics.sh --bootstrap-server "$B" --list 2>/dev/null | grep -qx -- "$1"; }
group_exists() { kafka-consumer-groups.sh --bootstrap-server "$B" --list 2>/dev/null | grep -qx -- "$1"; }
delete_topic() {   # delete_topic T : delete and wait until it is gone (no-op if absent)
  topic_exists "$1" || { echo "   topic $1: not present"; return 0; }
  kafka-topics.sh --bootstrap-server "$B" --delete --topic "$1"
  for _ in $(seq 1 60); do topic_exists "$1" || { echo "   topic $1 deleted"; return 0; }; sleep 1; done
  echo "!! topic $1 still listed after 60s"; return 1
}
delete_group() {   # delete_group G : kafka-consumer-groups --delete exits 0 even when it fails, so read its output
  kafka-consumer-groups.sh --bootstrap-server "$B" --delete --group "$1" >/tmp/.dg 2>&1 || true
  if grep -q "does not exist" /tmp/.dg; then echo "   group $1: not present"
  elif grep -q "was successful" /tmp/.dg; then echo "   group $1 deleted"
  else grep -vE '^\s*$|^\s+at ' /tmp/.dg; return 1; fi
}
kill_strays() {    # kill_strays PATTERN : stop processes in kafka-client whose command line matches
  local n; n=$(pgrep -f -- "$1" | wc -l)
  [ "$n" -gt 0 ] || return 0
  pkill -TERM -f -- "$1"; sleep 3; pkill -KILL -f -- "$1" 2>/dev/null
  echo "   stopped $n process(es) matching: $1"
}
leaders_by_broker() { # leaders_by_broker [TOPIC] : leader count per broker + partitions not on their preferred leader
  local args=(); [ -n "${1:-}" ] && args=(--topic "$1")
  kafka-topics.sh --bootstrap-server "$B" --describe "${args[@]}" 2>/dev/null | awk '
    { isp=0; for (i=1;i<=NF;i++) { if ($i=="Partition:") isp=1; if ($i=="Leader:") l=$(i+1); if ($i=="Replicas:") r=$(i+1) }
      if (!isp) next; split(r, rr, ","); cnt[l]++; tot++; if (l!=rr[1]) np++ }
    END { for (b=0; b<3; b++) printf "broker-%d %d\n", b, cnt[b]+0
          printf "#total=%d not-on-preferred-leader=%d\n", tot, np+0 }'
}
POD_LIB_EOF

_b64() { base64 | tr -d '\n'; }

# POD_LIB is installed once into kafka-client as a file named after its checksum; every snippet sources it.
POD_LIB_FILE="/tmp/lab-pod-lib-$(printf '%s' "$POD_LIB" | cksum | cut -d' ' -f1).sh"
_pod_lib_install() {
  local b64; b64="$(printf '%s\n' "$POD_LIB" | _b64)"
  kctl -n "$NS" exec "$CLIENT_POD" -- bash -c \
    "echo $b64 | base64 -d > $POD_LIB_FILE.tmp && mv $POD_LIB_FILE.tmp $POD_LIB_FILE"
}

# kx: run a bash snippet (args or stdin) inside kafka-client, with POD_LIB loaded and $BOOTSTRAP/$B set.
# The snippet is base64-encoded so quotes/pipes/$ survive any host shell, including WSL -> kubectl.exe.
kx() {
  local script b64 rc=0
  if [ $# -gt 0 ]; then script="$*"; else script="$(cat)"; fi
  b64="$(printf "export BOOTSTRAP='%s'\n[ -f %s ] || exit 97\n. %s\n%s" \
         "$BOOTSTRAP" "$POD_LIB_FILE" "$POD_LIB_FILE" "$script" | _b64)"
  [ "${#b64}" -lt 30000 ] || die "kx snippet too large (${#b64} bytes base64); split it up"
  kctl -n "$NS" exec "$CLIENT_POD" -- bash -c "echo $b64 | base64 -d | bash" || rc=$?
  if [ "$rc" -eq 97 ]; then          # library not in the pod yet: install it and run again
    _pod_lib_install || return 1
    rc=0; kctl -n "$NS" exec "$CLIENT_POD" -- bash -c "echo $b64 | base64 -d | bash" || rc=$?
  fi
  return "$rc"
}

# kxb: run a snippet in a broker pod (container 'kafka'), e.g. df.
kxb() {
  local pod="$1"; shift
  local script b64
  if [ $# -gt 0 ]; then script="$*"; else script="$(cat)"; fi
  b64="$(printf '%s' "$script" | _b64)"
  kctl -n "$NS" exec "$pod" -c kafka -- bash -c "echo $b64 | base64 -d | bash"
}

# ---- cluster-level helpers ----
wait_brokers_ready() {
  step "waiting for statefulset/$BROKER_STS to be fully ready"
  k rollout status "statefulset/$BROKER_STS" --timeout="${1:-5m}"
}
urp_count() { kx 'timeout 30 kafka-topics.sh --bootstrap-server "$B" --describe --under-replicated-partitions 2>/dev/null | grep -c "Partition:" || true'; }
wait_healthy() {   # wait_healthy [SECONDS] : 3/3 brokers Ready and 0 under-replicated partitions
  local timeout="${1:-300}" t0=$SECONDS urp
  wait_brokers_ready "${timeout}s"
  while :; do
    urp="$(urp_count 2>/dev/null || echo '?')"
    if [ "$urp" = "0" ]; then ok "cluster healthy: 3/3 brokers ready, 0 under-replicated partitions"; return 0; fi
    if [ $((SECONDS - t0)) -ge "$timeout" ]; then bad "still $urp under-replicated partitions after ${timeout}s"; return 1; fi
    echo "   under-replicated partitions: $urp (waiting for ISR to catch up)"; sleep 3
  done
}
# wait_topic_files_gone TOPIC [SECONDS] : topic deletion is asynchronous - brokers rename the log dirs
# to *-delete and remove them after log.segment.delete.delay.ms (60s; a broker restart re-queues it).
wait_topic_files_gone() {
  local t="$1" timeout="${2:-180}" t0=$SECONDS left i n
  while :; do
    left=0
    for i in 0 1 2; do
      n="$(kxb "$BROKER_STS-$i" "ls /bitnami/kafka/data | grep -c '^$t-' || true" 2>/dev/null || echo 0)"
      left=$((left + ${n:-0}))
    done
    if [ "$left" -eq 0 ]; then ok "no log dirs of topic $t left on any broker disk"; return 0; fi
    if [ $((SECONDS - t0)) -ge "$timeout" ]; then bad "$left log dirs of $t still on disk after ${timeout}s"; return 1; fi
    echo "   $left log dirs of deleted topic $t still on disk (async delete, ~60s) - waiting"
    sleep 10
  done
}
# helm_lab_upgrade [extra values files relative to repo root] : lab/values.yaml (+ overrides)
helm_lab_upgrade() {
  local helm; helm="$(helm_bin)"
  local args=(-f lab/values.yaml) f
  for f in "$@"; do args+=(-f "$f"); done
  ( cd "$ROOT_DIR" && "$helm" upgrade --install "$RELEASE" "$CHART" --version "$CHART_VERSION" \
      "${HCTX[@]}" -n "$NS" "${args[@]}" --wait --timeout 10m ) | { grep -E '^(Release|STATUS:|REVISION:)' || true; }
}
broker_config() { # broker_config KEY [BROKER_ID] : effective value of a broker config
  kx "kafka-configs.sh --bootstrap-server \"\$B\" --entity-type brokers --entity-name ${2:-0} --describe --all 2>/dev/null \
      | awk -F'[= ]+' '\$2==\"$1\" {print \$3; exit}'"
}
