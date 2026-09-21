# 18 · A broker disk fills up

**What you'll learn:** what Kafka does when a log directory runs out of space, why the damage spreads
beyond the topic that caused it, and how to get the broker back — without touching a PVC.

**Time:** about 30 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> messages will differ.

> ⚠️ **This scenario really fills a disk.** It only targets **broker 2** (its own 1 GiB loop-mounted
> disk); brokers 0 and 1 keep the cluster alive. Before you start, make sure no other scenario is
> running: `wsl -d Ubuntu -- bash cleanup.sh` resets everything. Never delete a PVC to fix this — the
> steps below don't need it.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 18`.

---

## Part 1 · Normal: three brokers with 1 GiB each

### Step 1 · PowerShell: how much space is free?
```powershell
docker exec kind-control-plane df -h /mnt/kafka-disks/disk-0 /mnt/kafka-disks/disk-1 /mnt/kafka-disks/disk-2
```
✅ **Expected:** three lines of about `1.0G` each, mostly free (`Use%` well below 50%). These are real
filesystems: each broker's `/bitnami/kafka` lives on one of them.

### Step 2 · Create a topic that never deletes anything, on broker 2 only
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic filler --replica-assignment 2 --config retention.ms=-1 --config retention.bytes=-1
```
✅ **Expected:** `Created topic filler.` One partition, one replica, on broker 2, and Kafka will keep
every message forever — the combination behind most "disk full" incidents.

### Step 3 · Write 100 MB of archive data
```bash
kafka-producer-perf-test.sh --topic filler --num-records 1000 --record-size 100000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 | tail -1
```
✅ **Expected:** a summary line, `1000 records sent, … (about 95 MB)`.

### Step 4 · Kafka's own view of the disk usage
```bash
bash /apps/replica-sizes.sh filler
```
✅ **Expected:** `broker 2    1 replicas of filler        ~95.00 MB`. No other broker stores this topic.

### Step 5 · PowerShell: the disk is filling
```powershell
docker exec kind-control-plane df -h /mnt/kafka-disks/disk-2
```
✅ **Expected:** `Use%` roughly 10–15 points higher than in step 1.

---

## Part 2 · Break: the archive job runs all night

### Step 6 · Write until the disk gives up (this takes a few minutes)
```bash
kafka-producer-perf-test.sh --topic filler --num-records 9000 --record-size 100000 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 2>&1 | tail -20
```
✅ **Expected:** progress lines, then errors and a stop, with exceptions such as
`org.apache.kafka.common.errors.KafkaStorageException` (the broker can't write to its log dir),
`NotLeaderOrFollowerException` or `TimeoutException`. The tool may not finish its 9,000 records at all.

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · PowerShell: the disk
```powershell
docker exec kind-control-plane df -h /mnt/kafka-disks/disk-2
```
✅ **Expected:** `Use% 100%` and almost no space available.

### Step 8 · PowerShell: the broker
```powershell
kubectl -n kafka-lab get pods
kubectl -n kafka-lab logs kafka-controller-2 --tail=40 | Select-String -Pattern "No space left|offline|Shutdown|ERROR"
```
✅ **Expected:** `kafka-controller-2` with `RESTARTS` above 0 — possibly `CrashLoopBackOff` — and log
lines about `No space left on device`, the log directory being taken **offline**, and the broker
shutting itself down. A Kafka broker that can't write to its log directory stops; it does not keep
going with half a disk.

### Step 9 · The partition that caused it
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions
```
✅ **Expected:** `filler` with `Leader: none` (once broker 2 is down). It has only one replica, so
there's nothing to fail over to.

### Step 10 · The damage is not limited to that topic
```bash
bash /apps/isr-watch.sh "" 3 5
```
✅ **Expected:** `under-replicated` well above 0 across the cluster: **every** topic with a replica on
broker 2 is now missing a copy, including Kafka's internal `__consumer_offsets`. One runaway topic took
a third of the cluster's redundancy with it.

### Step 11 · The rest of the cluster still serves clients
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | head -5
bash /apps/produce-check.sh filler 3 1
```
✅ **Expected:** the topic list works (brokers 0 and 1 are fine and still have a controller majority),
while writes to `filler` fail with `ERROR TimeoutException` / `LeaderNotAvailableException`.

---

## Part 4 · Fix: delete the data that filled the disk

### Step 12 · Delete the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic filler
```
✅ **Expected:** no output. The deletion is recorded by the controller; broker 2 removes the files as
soon as it is running again. **Wait ~60 seconds** and check `kubectl -n kafka-lab get pods` in
PowerShell: if broker 2 is `1/1 Running`, jump to step 15.

### Step 13 · PowerShell: if broker 2 can't start, look at its disk
```powershell
docker exec kind-control-plane sh -c "ls /mnt/kafka-disks/disk-2/data; du -sh /mnt/kafka-disks/disk-2/data/* | sort -h | tail -5"
```
✅ **Expected:** either the topic directories (`filler-0`, `__cluster_metadata-0`, …) directly, or a
`data/` directory that holds them, with `filler-0` as by far the biggest.

### Step 14 · PowerShell: remove the topic's files and restart the broker
```powershell
docker exec kind-control-plane sh -c "rm -rf /mnt/kafka-disks/disk-2/data/filler-0 /mnt/kafka-disks/disk-2/data/data/filler-0"
kubectl -n kafka-lab delete pod kafka-controller-2
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=300s
```
✅ **Expected:** the `rm` prints nothing (one of the two paths didn't exist — that's fine), then
`pod "kafka-controller-2" deleted` and after ~60 s `pod/kafka-controller-2 condition met`. This is the
lab's version of "log in to the box and delete the segments so the broker can start".

---

## Part 5 · Back to normal

### Step 15 · PowerShell: free space and a running broker
```powershell
docker exec kind-control-plane df -h /mnt/kafka-disks/disk-2
kubectl -n kafka-lab get pods
```
✅ **Expected:** `Use%` back near where it was in step 1, and all three pods `1/1 Running`.

### Step 16 · Replication catches up
```bash
bash /apps/isr-watch.sh "" 8 10
```
✅ **Expected:** `under-replicated` falls back to `0` as broker 2 copies what it missed (it can take a
minute or two).

### Step 17 · Full health check
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions; echo "(nothing above = healthy)"
```
✅ **Expected:** only the `(nothing above = healthy)` line.

---

## Part 6 · Clean up

### Step 18 · Make sure the topic is gone
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep filler; echo "(no filler above = clean)"
```
✅ **Expected:** only the `(no filler above = clean)` line.

---

## Why it happened, and how to prevent it

- **Why:** `retention.ms=-1` with `retention.bytes=-1` means "keep everything forever". Kafka happily
  filled the disk, and when a write failed with `No space left on device` the broker took that log
  directory **offline**. In KRaft the metadata log lives on the same disk here, so the broker shut
  itself down instead of running blind.
- **One topic, cluster-wide damage:** the disk is shared by every partition on that broker. When it
  filled, every replica on broker 2 stopped — including `__consumer_offsets`, which is why consumer
  groups elsewhere can start failing too.
- **Prevent it:**
  - Give every topic a retention limit, and prefer a **size** limit as the safety net:
    `retention.bytes` per partition × partitions per broker must fit the disk with room to spare.
  - Alert early on disk usage (70%) and on `OfflineLogDirectoryCount` / `LogDirectoryOffline`, not when
    it's already 100%.
  - Watch what `kafka-log-dirs.sh` reports per broker (that's what `/apps/replica-sizes.sh` reads), so
    you can see which topic is growing.
  - Keep headroom for compaction, segment rolling and replication catch-up — a full disk can't even
    delete data on some filesystems.
- **Recovering for real:**
  1. Stop the producer that is filling the disk.
  2. Delete the topic, or lower `retention.ms` / `retention.bytes` and wait for the cleaner (it runs
     every `log.retention.check.interval.ms`, 5 minutes by default).
  3. If the broker won't start, remove the biggest partition directories from that log dir by hand and
     restart, as in step 14.
  4. Expanding the volume is the clean answer when the data is legitimate: the PVC needs a
     StorageClass with `allowVolumeExpansion: true` (this lab's `kafka-lab-1g` has it off on purpose).
     **Deleting a PVC deletes the broker's data — ask before anyone does that.**
