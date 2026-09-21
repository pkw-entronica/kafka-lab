# 11 · acks=1 and the illusion of safety

**What you'll learn:** what `acks` really promises, why `min.insync.replicas` does **nothing** for
`acks=1`, and how to see that "accepted" messages exist on a single disk.

**Time:** about 15 minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open (it survives the broker going away):
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window.
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 11`.

---

## Part 1 · Normal: two copies, both in sync

The topic `critical` carries payment instructions. The team chose **two** copies and
`min.insync.replicas=2` — "two disks is enough, and Kafka will tell us if it ever isn't".

The **ISR** (in-sync replicas) is the set of replicas that have caught up with the leader. Kafka tracks
it per partition, and it's the heart of this scenario. The replicas are pinned to brokers 0 and 2 so
that the rest of the guide can name them.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic critical --replica-assignment 0:2 --config min.insync.replicas=2
```
✅ **Expected:** `Created topic critical.`

### Step 2 · Look at the replicas
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** `ReplicationFactor: 2`, `min.insync.replicas=2`, and one partition line with
`Leader: 0`, `Replicas: 0,2` and `Isr: 0,2`. Both replicas are in sync.

### Step 3 · Write 20 MB with acks=all
```bash
kafka-producer-perf-test.sh --topic critical --num-records 20000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=-1
```
✅ **Expected:** a summary line like `20000 records sent, 5870.3 records/sec (5.73 MB/sec)`. With
`acks=all` (`-1`), the leader answers only after every in-sync replica has the message.

### Step 4 · Where does the data live?
```bash
bash /apps/replica-sizes.sh critical
```
✅ **Expected:** brokers 0 and 2 hold about **19.8 MB** each — the same data on two different disks.
Broker 1 shows `0 replicas … 0.00 MB`, because this topic was never placed there.

---

## Part 2 · Break: one of the two copies goes away

A node has to be taken out for maintenance. Nobody checks which topics only had two copies to begin
with. In the lab, scaling the StatefulSet down does the same thing: broker 2 stops, its disk stays.

### Step 5 · PowerShell: stop broker 2
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`. Within ~30 s,
`kubectl -n kafka-lab get pods` shows only `kafka-controller-0` and `kafka-controller-1`.
**Wait ~30 seconds** before continuing.

---

## Part 3 · Observe: what does the problem look like?

### Step 6 · Who is still in sync?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** `Replicas: 0,2` but `Isr: 0` — a **single** replica, the leader. Broker 2 is gone, so
the controller removed it from the ISR. The partition still has a leader, so it keeps serving.

### Step 7 · Does Kafka consider this a problem?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-min-isr-partitions
```
✅ **Expected:** the `critical` partition is listed: its ISR (1) is below `min.insync.replicas` (2).
This is the metric to alert on (`UnderMinIsrPartitionCount`).

### Step 8 · But acks=1 keeps accepting writes
```bash
bash /apps/produce-check.sh critical 5 1
```
✅ **Expected:** 5 × `ok`, then `5 accepted, 0 rejected (acks=1)`. `min.insync.replicas` is **not**
checked for `acks=1`: the leader alone decides.

### Step 9 · 10 MB more with acks=1 — where do they live?
```bash
kafka-producer-perf-test.sh --topic critical --num-records 10000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$BOOTSTRAP acks=1; bash /apps/replica-sizes.sh critical
```
✅ **Expected:** every message accepted, and broker 0 now holds about **29.7 MB** — while broker 2
isn't listed at all. Roughly 10 MB of acknowledged payments exist on **one disk only**. If that disk
dies now, they're gone — see scenario 17 for exactly how that looks.

### Step 10 · What acks=all says about the same cluster
```bash
bash /apps/produce-check.sh critical 5 all
```
✅ **Expected:** 5 × `ERROR NotEnoughReplicasException`, then `0 accepted, 5 rejected (acks=all)`.
With `acks=all`, Kafka refuses the write because fewer than `min.insync.replicas` replicas are in sync.
The producer is **told**, instead of silently taking the risk.

---

## Part 4 · Fix: bring the second copy back

### Step 11 · PowerShell: start broker 2 again
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=3
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`, and the pod is `Running` again after
~60–90 s.

---

## Part 5 · Back to normal

### Step 12 · The ISR fills up again
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** `Isr: 0,2` again, usually within ~30 s of the pod being ready. Run it again if the
second replica is still missing — it has ~10 MB to copy.

### Step 13 · acks=all works again
```bash
bash /apps/produce-check.sh critical 5 all
```
✅ **Expected:** 5 × `ok`, then `5 accepted, 0 rejected (acks=all)`, as in step 3.

### Step 14 · Both disks have the data
```bash
bash /apps/replica-sizes.sh critical
```
✅ **Expected:** brokers 0 and 2 match again (about **29.7 MB** each). The copy caught up on its own.

---

## Part 6 · Clean up

### Step 15 · Delete the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic critical; rm -f /tmp/s11-*
```
✅ **Expected:** no output. Check with `kubectl -n kafka-lab get pods` that all three brokers are
`Running` — if you stopped after Part 2, scale the StatefulSet back to 3 first.

---

## Why it happened, and how to prevent it

- **What `acks` means for the producer:**
  | `acks` | The leader answers "ok" when … | Risk |
  |---|---|---|
  | `0` | the message left the client | anything can be lost, even with a healthy cluster |
  | `1` | the **leader** wrote it to its log | lost if that broker dies before the followers copy it |
  | `all` (`-1`) | every **in-sync replica** wrote it | safe, as long as the ISR is big enough |
- **`min.insync.replicas` is only checked for `acks=all`.** It's the other half of the promise: with
  `acks=all` **and** `min.insync.replicas=2`, at least two brokers have every acknowledged message, and
  Kafka rejects writes (`NotEnoughReplicas`) when that's no longer true. `acks=1` ignores it completely.
- **RF 2 leaves no room.** With two copies and `min.insync.replicas=2`, a single broker restart already
  stops `acks=all` writes — the config is either unsafe (`acks=1`) or unavailable (`acks=all`), with
  nothing in between. RF **3** with `min.insync.replicas=2` is the combination that survives one broker
  going away and still guarantees two copies of everything. Scenario 16 shows the mirror-image mistake:
  RF 3 with `min.insync.replicas=3`, which is just as brittle.
- **The ISR is what makes `acks=all` meaningful.** A replica leaves the ISR when its broker is gone, or
  when it hasn't caught up for `replica.lag.time.max.ms` (30 s). Common causes: a broker that is away
  (this scenario, and scenario 15), a slow or overloaded broker, a network problem (scenario 19).
- **A replication throttle is *not* a way to shrink the ISR.** It is tempting to think
  `follower.replication.throttled.rate=1` would starve the followers out of the ISR. It does not: Kafka
  deliberately skips the throttle for any replica that is currently **in sync** — the check is
  `!isReplicaInSync && isThrottled && isQuotaExceeded`, on both the leader and the follower side —
  precisely so that a throttled partition reassignment cannot cause ISR churn. Measured on this lab:
  20 MB pushed through the topic at 11 MB/s with the rate set to 1 byte/s, and the ISR never moved.
- **Prevent it:**
  - `acks=all` + `min.insync.replicas=2` + RF 3 for anything you can't lose, and handle
    `NotEnoughReplicasException` in the app (retry, or buffer and alert).
  - Alert on `UnderMinIsrPartitionCount` > 0 and `UnderReplicatedPartitions` > 0.
  - Before draining a node, check which topics have RF < 3 — they are the ones that will notice.
- **The other half of this story:** losing an under-replicated partition's leader is scenario 17
  (unclean leader election), where those "accepted" messages actually disappear.
