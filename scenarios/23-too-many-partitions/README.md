# 23 · Too many partitions

**What you'll learn:** what a partition costs when it's idle — file handles, memory, metadata, and above
all **time**: how much longer a broker takes to restart and to catch up when it holds thousands of them.

**Time:** about 30 minutes (mostly waiting for broker restarts).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; the actual
> numbers depend on your PC.

> ⚠️ This scenario creates 1,500 partitions (4,500 replicas) on a 3-broker lab cluster with 512 MB of
> heap each. Don't run other scenarios at the same time, and don't raise the numbers unless you want to
> find the breaking point — deleting them again takes a few minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 23`.

---

## Part 1 · Normal: measure the cluster as it is now

### Step 1 · How many partitions does the cluster hold?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe | grep -c "Partition:"
```
✅ **Expected:** a small number — the internal topics plus anything a previous scenario left behind
(run `wsl -d Ubuntu -- bash cleanup.sh` first if it's more than ~60).

### Step 2 · PowerShell: what a broker uses today
```powershell
kubectl -n kafka-lab exec kafka-controller-0 -- sh -c 'p=$(pgrep -f kafka.Kafka | head -1); echo "open files: $(ls /proc/$p/fd | wc -l)"; echo "memory: $(( $(cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes) / 1048576 )) MB"'
```
✅ **Expected:** something like `open files: 150` and `memory: 700 MB`. Every replica keeps its segment,
index and time-index files open, so the file count follows the partition count.

### Step 3 · PowerShell: how long does a broker take to restart?
```powershell
Measure-Command { kubectl -n kafka-lab delete pod kafka-controller-2; kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=600s } | Select-Object TotalSeconds
```
✅ **Expected:** roughly `40`–`70` seconds. **Write it down.** Then, in the lab shell, wait until
`bash /apps/isr-watch.sh "" 4 5` reports `under-replicated 0` before you continue.

---

## Part 2 · Break: a team creates "a topic per customer"

### Step 4 · Create 1,500 partitions (3 topics × 500, replication factor 3)
```bash
time for t in 1 2 3; do kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic parts-$t --partitions 500 --replication-factor 3 --config retention.bytes=1048576; done
```
✅ **Expected:** three `Created topic parts-N.` lines and a `real` time of anywhere from 10 s to a
minute. That's 4,500 replicas, 1,500 per broker — and not a single message yet.

---

## Part 3 · Observe: what does the problem look like?

### Step 5 · The new size of the cluster
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe | grep -c "Partition:"
```
✅ **Expected:** about `1500` more than in step 1.

### Step 6 · PowerShell: what a broker uses now
```powershell
kubectl -n kafka-lab exec kafka-controller-0 -- sh -c 'p=$(pgrep -f kafka.Kafka | head -1); echo "open files: $(ls /proc/$p/fd | wc -l)"; echo "memory: $(( $(cat /sys/fs/cgroup/memory.current 2>/dev/null || cat /sys/fs/cgroup/memory/memory.usage_in_bytes) / 1048576 )) MB"'
```
✅ **Expected:** thousands of open files (roughly 3 per replica: `.log`, `.index`, `.timeindex`) and
noticeably more memory than in step 2 — for partitions that hold no data at all.

### Step 7 · PowerShell: restart a broker again, and time it
```powershell
Measure-Command { kubectl -n kafka-lab delete pod kafka-controller-2; kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=600s } | Select-Object TotalSeconds
```
✅ **Expected:** clearly longer than step 3 — the broker has to open and verify every log on startup.

### Step 8 · How long until it is safe to touch the next broker?
```bash
s=$(date +%s); while [ "$(kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions | grep -c 'Partition:')" != "0" ]; do sleep 5; done; echo "replication caught up after $(( $(date +%s) - s )) s"
```
✅ **Expected:** tens of seconds to minutes. In a rolling restart (scenario 21) you pay this **per
broker**, so the maintenance window grows with the partition count, not with the amount of data.

### Step 9 · The metadata grew too
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status | grep -E "HighWatermark|LeaderId"
```
✅ **Expected:** a `HighWatermark` far higher than before: every partition, every leader change and
every ISR update is a record in the KRaft metadata log that all controllers replicate and replay.

### Step 10 · And it costs the clients too
```bash
kafka-producer-perf-test.sh --topic parts-1 --num-records 50000 --record-size 200 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 | tail -1
```
✅ **Expected:** a much lower records/s than a 3- or 6-partition topic would give (compare with
scenario 14). With 500 partitions and no keys, each batch carries a handful of records, so the
producer pays the per-request cost over and over.

---

## Part 4 · Fix: delete what nobody needed

### Step 11 · Delete the three topics
```bash
time for t in 1 2 3; do kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic parts-$t; done
```
✅ **Expected:** no output per topic, and a `real` time of some seconds. The brokers delete the files
in the background (~60 s later), so the disk usage falls a little after that.

### Step 12 · Check the count is back down
```bash
sleep 30; kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe | grep -c "Partition:"
```
✅ **Expected:** back to roughly the number from step 1.

---

## Part 5 · Back to normal

### Step 13 · PowerShell: restart time is back to normal
```powershell
Measure-Command { kubectl -n kafka-lab delete pod kafka-controller-2; kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=600s } | Select-Object TotalSeconds
```
✅ **Expected:** close to step 3 again.

### Step 14 · PowerShell: and so are the file handles
```powershell
kubectl -n kafka-lab exec kafka-controller-0 -- sh -c 'p=$(pgrep -f kafka.Kafka | head -1); echo "open files: $(ls /proc/$p/fd | wc -l)"'
```
✅ **Expected:** back near the number from step 2.

### Step 15 · The cluster is healthy
```bash
bash /apps/isr-watch.sh "" 4 5
```
✅ **Expected:** `under-replicated 0   offline 0`.

---

## Part 6 · Clean up

### Step 16 · Make sure the topics are gone
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep parts- ; echo "(no parts- above = clean)"
```
✅ **Expected:** only the `(no parts- above = clean)` line.

---

## Why it happened, and how to prevent it

- **What one partition costs, even empty:**
  - 3 open files per replica, plus memory for its index buffers and in-memory state;
  - a record in the KRaft metadata log, replayed by every controller and broker at startup;
  - a slot in every fetch request between brokers (replication) and in every client metadata response;
  - time on startup: the broker opens and validates every log before it reports Ready.
- **The numbers that matter are per broker, not per topic.** A rough guide for KRaft clusters: a few
  thousand partitions per broker is comfortable, tens of thousands is where startup time, failover time
  and memory start to hurt. In this lab, 1,500 per broker on a 512 MB heap is already visible.
- **Choose a partition count from throughput and consumers, not from "just in case":**
  - target throughput ÷ what one partition can handle (often 10–50 MB/s), and
  - at most one consumer per partition (scenario 02), so it caps consumer parallelism, and
  - remember you can add partitions later — but that breaks key ordering (scenario 03).
- **Patterns that create partition explosions:** a topic per customer/tenant/device, "let's use 100
  partitions everywhere by default", and many small topics for low-volume events. Put the tenant in the
  **key** instead, and share one topic.
- **What to watch:** total partition count per broker, broker startup time, controller failover time,
  and open file descriptors against the process limit (`ulimit -n`) — a broker that runs out of file
  handles fails in ways that look like disk corruption.
