# 26 · A corrupted log segment

**What you'll learn:** what a broker does at startup when its own files are damaged — index rebuild, log
recovery, truncation — and how replication repairs the missing data from the leader, as long as the
other replicas are healthy.

**Time:** about 25 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; the exact log
> messages will differ.

> ⚠️ This scenario deliberately damages files on **broker 2's** disk, from the kind node. Brokers 0 and
> 1 keep the data safe. No PVC is deleted, and the recovery path is part of the guide.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 26`.

---

## Part 1 · Normal: one partition, three identical copies

### Step 1 · Create the topic and fill it
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic corrupt-me --partitions 1 --replication-factor 3
bash /apps/numbered-producer.sh --burst 20000 corrupt-me rec 200
```
✅ **Expected:** `Created topic corrupt-me.` and `sent 20000 messages to corrupt-me (rec-...)` — about
4 MB.

### Step 2 · All three replicas hold the same data
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic corrupt-me --time -1
bash /apps/replica-sizes.sh corrupt-me
```
✅ **Expected:** `corrupt-me:0:20000`, and three brokers with the same size (~4 MB each).

---

## Part 2 · Break: bad blocks on broker 2

A disk with a failing sector, a host that lost power mid-write, a bug in a storage driver: the result is
a log segment whose tail is garbage and indexes that no longer match.

### Step 3 · PowerShell: stop broker 2
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
kubectl -n kafka-lab wait --for=delete pod/kafka-controller-2 --timeout=300s
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled` and then `pod/kafka-controller-2 condition
met` (or a message that it's already gone).

### Step 4 · PowerShell: look at the partition's files on its disk
```powershell
docker exec kind-control-plane sh -c 'd=/mnt/kafka-disks/disk-2/data/data/corrupt-me-0; [ -d "$d" ] || d=/mnt/kafka-disks/disk-2/data/corrupt-me-0; echo "log dir: $d"; ls -l "$d"'
```
✅ **Expected:** the segment files: `00000000000000000000.log`, `.index`, `.timeindex`, plus a
`leader-epoch-checkpoint` and maybe a `.snapshot`. The `.log` is the data; the rest can be rebuilt from it.

### Step 5 · PowerShell: damage the tail of the log and remove the indexes
```powershell
docker exec kind-control-plane sh -c 'd=/mnt/kafka-disks/disk-2/data/data/corrupt-me-0; [ -d "$d" ] || d=/mnt/kafka-disks/disk-2/data/corrupt-me-0; f=$(ls "$d"/*.log | tail -1); sz=$(stat -c %s "$f"); echo "overwriting the last 2048 bytes of $f ($sz bytes)"; dd if=/dev/urandom of="$f" bs=1 seek=$((sz-2048)) count=2048 conv=notrunc 2>/dev/null; rm -f "$d"/*.index "$d"/*.timeindex; rm -f "$(dirname "$d")"/.kafka_cleanshutdown; ls -l "$d"'
```
✅ **Expected:** the "overwriting …" line, then a listing without `.index` / `.timeindex`. Deleting the
clean-shutdown marker makes Kafka treat the next start as a crash, so it verifies its logs instead of
trusting them.

### Step 6 · PowerShell: start broker 2 again
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=3
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`. **Wait ~60 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · PowerShell: what the broker says about its own files
```powershell
kubectl -n kafka-lab logs kafka-controller-2 | Select-String -Pattern "corrupt|Recovering|recovery|Rebuilding|truncat|invalid" | Select-Object -First 20
```
✅ **Expected:** lines about rebuilding the missing index files, recovering the unflushed segment,
finding invalid or corrupt messages, and **truncating** the log to the last valid offset — for example
`Found invalid messages`, `Rebuilding index for`, `Truncating to offset …`. Kafka repairs what it can
and throws away what it can't verify.

### Step 8 · PowerShell: is it running?
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** `kafka-controller-2` back to `1/1 Running`. If it is in `CrashLoopBackOff`, jump to the
troubleshooting note at the end of Part 4.

### Step 9 · The replica is behind, and Kafka knows
```bash
bash /apps/isr-watch.sh corrupt-me 6 5
bash /apps/replica-sizes.sh corrupt-me
```
✅ **Expected:** `under-replicated 1` for a moment, then `0` again. The sizes may differ for a few
seconds and then match: broker 2 truncated its damaged tail and re-fetched those records from the leader.

### Step 10 · No data was lost
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic corrupt-me --time -1
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic corrupt-me --from-beginning --timeout-ms 30000 2>/dev/null > /tmp/s26-read.log
bash /apps/gap-check.sh /tmp/s26-read.log rec
```
✅ **Expected:** still `corrupt-me:0:20000`, and `20000 messages read (20000 different) … 0 missing`.
The leader had every record, so the corruption cost nothing but a resync.

---

## Part 4 · Fix: nothing to fix — that's the point

Replication already repaired it. The only thing to verify is that the cluster is fully healthy again.

### Step 11 · Confirm all three replicas are in sync
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic corrupt-me
```
✅ **Expected:** `Replicas: 0,1,2` and `Isr: 0,1,2`.

> **If broker 2 keeps crashing** (rare, when the damage hits the metadata log or a file Kafka can't
> rebuild), delete that partition's directory on the node and let replication recreate it:
> ```powershell
> kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
> docker exec kind-control-plane sh -c 'rm -rf /mnt/kafka-disks/disk-2/data/data/corrupt-me-0 /mnt/kafka-disks/disk-2/data/corrupt-me-0'
> kubectl -n kafka-lab scale statefulset kafka-controller --replicas=3
> ```
> A broker that starts without a partition it should hold simply copies it again from the leader.

---

## Part 5 · Back to normal

### Step 12 · Writes and reads work
```bash
bash /apps/produce-check.sh corrupt-me 5 all
bash /apps/isr-watch.sh "" 3 5
```
✅ **Expected:** `5 accepted, 0 rejected (acks=-1)` and `under-replicated 0   offline 0` for the whole
cluster.

---

## Part 6 · Clean up

### Step 13 · Delete the topic and the files
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic corrupt-me
rm -f /tmp/s26-* /tmp/numbered-corrupt-me-*
```
✅ **Expected:** no output.

### Step 14 · PowerShell: all three brokers running
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** three pods `1/1 Running`.

---

## Why it happened, and how to prevent it

- **Kafka checks its own data.** Every record batch carries a CRC. On an unclean start the broker
  recovers each log: it rebuilds missing `.index` / `.timeindex` files from the `.log`, validates the
  records, and truncates at the first batch that doesn't check out. That's why the indexes are
  disposable — only the `.log` files are precious.
- **Replication is the actual repair mechanism.** A follower that truncates simply fetches the missing
  records from the leader. With RF 3 and a healthy ISR, one broker's bad disk is an incident, not a
  data loss.
- **When it does cost data:** if the corrupted replica is the **leader** and the others are out of sync
  (scenario 17), truncation removes records that only existed there. Same story as unclean leader
  election — the defence is RF 3, `min.insync.replicas=2` and `acks=all`.
- **What to watch:**
  - broker restarts that take unusually long (log recovery is per segment — scenario 23 makes it worse);
  - `CorruptRecordException` in broker or consumer logs, and offline log directories;
  - kernel messages about I/O errors, and the disk's SMART data — Kafka is often the first process to
    notice a failing disk.
- **Useful tools:** `kafka-dump-log.sh --files <segment>.log --deep-iteration` verifies a segment's
  records offline (see scenario 25), and `kafka-log-dirs.sh` shows what each broker really holds
  (`/apps/replica-sizes.sh` reads it).
