# 11 · acks=1 and the illusion of safety

**What you'll learn:** what `acks` really promises, why `min.insync.replicas` does **nothing** for
`acks=1`, and how to see that "accepted" messages exist on a single disk.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 11`.

---

## Part 1 · Normal: three replicas, all in sync

The topic `critical` carries payment instructions: 1 partition, 3 replicas, `min.insync.replicas=2`.
The **ISR** (in-sync replicas) is the set of replicas that have caught up with the leader. Kafka tracks
it per partition, and it's the heart of this scenario.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic critical --partitions 1 --replication-factor 3 --config min.insync.replicas=2
```
✅ **Expected:** `Created topic critical.`

### Step 2 · Look at the replicas
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** one partition line with `Replicas: 0,1,2` and `Isr: 0,1,2` (in some order). All three
replicas are in sync.

### Step 3 · Send 200 messages with acks=all
```bash
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic critical --max-messages 200 --throughput 100 --acks -1 > /tmp/s11-normal.json
grep -c producer_send_success /tmp/s11-normal.json
```
✅ **Expected:** `200`. With `acks=all` (`-1`), the leader answers only after every in-sync replica has
the message.

### Step 4 · Where does the data live?
```bash
bash /apps/replica-sizes.sh critical
```
✅ **Expected:** three lines, one per broker, with roughly the same size (a few KB each). The same data
is on three different disks.

---

## Part 2 · Break: replication stalls

A colleague moved partitions around last week and left a **replication throttle** behind: replication is
limited to 1 byte per second. Nobody noticed, because the topic kept working.

### Step 5 · Mark the topic's replicas as throttled
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name critical --alter --add-config 'leader.replication.throttled.replicas=*,follower.replication.throttled.replicas=*'
```
✅ **Expected:** `Completed updating config for topic critical.`

### Step 6 · Set the throttle to 1 byte/s on every broker
```bash
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --add-config 'leader.replication.throttled.rate=1,follower.replication.throttled.rate=1'; done
```
✅ **Expected:** three times `Completed updating config for broker $b.`

### Step 7 · Keep writing (acks=1, like many apps do)
```bash
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic critical --max-messages 500 --throughput 50 --acks 1 > /tmp/s11-acks1.json
grep -c producer_send_success /tmp/s11-acks1.json
```
✅ **Expected:** `500` — every message accepted, no warning of any kind. **Now wait ~60 seconds** (a
replica leaves the ISR after `replica.lag.time.max.ms`, 30 s by default).

---

## Part 3 · Observe: what does the problem look like?

### Step 8 · Who is still in sync?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** `Replicas: 0,1,2` but `Isr:` with a **single** broker, the leader. The two followers
fell behind and were dropped from the ISR.

### Step 9 · Does Kafka consider this a problem?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-min-isr-partitions
```
✅ **Expected:** the `critical` partition is listed: its ISR (1) is below `min.insync.replicas` (2).
This is the metric to alert on (`UnderMinIsrPartitionCount`).

### Step 10 · But acks=1 keeps accepting writes
```bash
bash /apps/produce-check.sh critical 10 1
```
✅ **Expected:** 10 × `ok`, then `10 accepted, 0 rejected (acks=1)`. `min.insync.replicas` is **not**
checked for `acks=1`: the leader alone decides.

### Step 11 · Where do those messages live now?
```bash
bash /apps/replica-sizes.sh critical
```
✅ **Expected:** the leader's replica is clearly bigger than the other two (roughly 500 messages more).
Every message accepted since step 7 exists on **one disk only**. If that broker's disk dies now, they're
gone — see scenario 17 for exactly how that looks.

### Step 12 · What acks=all says about the same cluster
```bash
bash /apps/produce-check.sh critical 10 all
```
✅ **Expected:** 10 × `ERROR NotEnoughReplicasException`, then `0 accepted, 10 rejected (acks=all)`.
With `acks=all`, Kafka refuses the write because fewer than `min.insync.replicas` replicas are in sync.
The producer is **told**, instead of silently taking the risk.

---

## Part 4 · Fix: let replication catch up again

### Step 13 · Remove the throttle from the brokers
```bash
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate'; done
```
✅ **Expected:** three times `Completed updating config for broker $b.`

### Step 14 · Remove it from the topic
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name critical --alter --delete-config 'leader.replication.throttled.replicas,follower.replication.throttled.replicas'
```
✅ **Expected:** `Completed updating config for topic critical.`

---

## Part 5 · Back to normal

### Step 15 · The ISR fills up again
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic critical
```
✅ **Expected:** within ~30 s, `Isr: 0,1,2` again. Run it again if a replica is still missing.

### Step 16 · acks=all works again
```bash
bash /apps/produce-check.sh critical 10 all
```
✅ **Expected:** 10 × `ok`, then `10 accepted, 0 rejected (acks=all)`, as in step 3.

### Step 17 · All three disks have the data
```bash
bash /apps/replica-sizes.sh critical
```
✅ **Expected:** the three sizes match again, as in step 4.

---

## Part 6 · Clean up

### Step 18 · Delete the topic and any leftover throttles
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic critical
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate' 2>/dev/null; done
rm -f /tmp/s11-*
```
✅ **Expected:** nothing from the delete, and `Completed updating config for broker …` (or an error that
the config isn't set, which is fine — it means step 13 already removed it).

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
- **The ISR is what makes `acks=all` meaningful.** A replica leaves the ISR when it hasn't caught up for
  `replica.lag.time.max.ms` (30 s). Common causes: a slow or overloaded broker, a leftover replication
  throttle (this scenario), a network problem (scenario 19), a restart (scenario 15).
- **Prevent it:**
  - `acks=all` + `min.insync.replicas=2` + RF 3 for anything you can't lose, and handle
    `NotEnoughReplicasException` in the app (retry, or buffer and alert).
  - Alert on `UnderMinIsrPartitionCount` > 0 and `UnderReplicatedPartitions` > 0.
  - Check for leftover `*.replication.throttled.*` configs after every partition reassignment.
- **The other half of this story:** losing an under-replicated partition's leader is scenario 17
  (unclean leader election), where those "accepted" messages actually disappear.
