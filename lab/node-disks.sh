#!/usr/bin/env bash
# Give each broker a REAL 1 GiB filesystem: a loop-mounted ext4 image inside the kind node.
#
# Why: kind's default StorageClass "standard" (rancher local-path) is just a directory on the node's
# ~1 TB disk. It ignores the PVC size, so a "disk full" scenario would fill the Docker Desktop VM
# instead of a 1Gi volume. Here each PV (lab/storage.yaml) points at /mnt/kafka-disks/disk-N/data,
# which only exists INSIDE the mounted 1 GiB image. If the image is not mounted, the path is missing
# and kubelet refuses to start the broker (instead of silently writing to the big node disk).
#
# Idempotent. Re-run after Docker Desktop / kind restarts: loop mounts do not survive a restart of
# the node container (the images and their data do).
. "$(dirname "$0")/lib.sh"
DOCKER="$(docker_bin)"
SIZE="${DISK_SIZE:-1G}"

hdr "Loop-backed ${SIZE} disks for the 3 brokers (node container: $KIND_NODE)"
"$DOCKER" exec -i "$KIND_NODE" bash -s -- "$SIZE" <<'EOF'
set -euo pipefail
SIZE="$1"
mkdir -p /var/kafka-disks /mnt/kafka-disks
for i in 0 1 2; do
  img=/var/kafka-disks/disk-$i.img
  mnt=/mnt/kafka-disks/disk-$i
  mkdir -p "$mnt"
  if [ ! -f "$img" ]; then
    echo "creating $img ($SIZE, ext4, 0% root reserve)"
    fallocate -l "$SIZE" "$img"
    mkfs.ext4 -q -m 0 -L "kafka-disk-$i" "$img"
  fi
  if mountpoint -q "$mnt"; then
    echo "disk-$i: already mounted on $mnt"
  else
    echo "disk-$i: mounting $img on $mnt"
    mount -o loop "$img" "$mnt"
  fi
  mkdir -p "$mnt/data"                 # the PV path lives inside the loop filesystem
  chown 1001:1001 "$mnt/data"          # Bitnami Kafka runs as uid/gid 1001
  chmod 0775 "$mnt/data"
done
echo
df -h /mnt/kafka-disks/disk-0 /mnt/kafka-disks/disk-1 /mnt/kafka-disks/disk-2
EOF
ok "disks ready"
