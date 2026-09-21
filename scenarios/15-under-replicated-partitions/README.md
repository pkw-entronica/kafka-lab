# 15 · Under-replicated partitions

**What you'll learn:** what "under-replicated" means, the two everyday causes (a broker that is *slow*
and a broker that is *gone*), how long Kafka waits before it drops a replica from the ISR, and how to
watch the recovery.

**Time:** about 20 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open (it survives the broker restart):
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- After each command, compare what you see with **✅ Expected**.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 15`.

---

## Part 1 · Normal: 6 partitions, 3 replicas each, all in sync

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic replicated --partitions 6 --replication-factor 3 --config retention.bytes=33554432
```
✅ **Expected:** `Created topic replicated.`

### Step 2 · Start continuous traffic (500 per second)
```bash
nohup kafka-producer-perf-test.sh --topic replicated --num-records 1000000000 --record-size 200 --throughput 500 --producer-props bootstrap.servers=$BOOTSTRAP acks=1 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Check replication health
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic replicated --under-replicated-partitions; echo "(nothing above = healthy)"
```
✅ **Expected:** only the `(nothing above = healthy)` line. Every partition has all 3 replicas in its ISR.

### Step 4 · Watch it for a few samples
```bash
bash /apps/isr-watch.sh replicated 3 5
```
✅ **Expected:** three lines with `partitions 6   under-replicated 0   offline 0`.

---

## Part 2 · Break (a): one broker can't keep up

Last week someone moved partitions around and left a **replication throttle** behind: 1 byte per second.

### Step 5 · Throttle this topic's replication
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name replicated --alter --add-config 'leader.replication.throttled.replicas=*,follower.replication.throttled.replicas=*'
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --add-config 'leader.replication.throttled.rate=1,follower.replication.throttled.rate=1'; done
```
✅ **Expected:** `Completed updating config for topic replicated.` and three times
`Completed updating config for broker $b.` **Wait ~60 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 6 · How many partitions are under-replicated?
```bash
bash /apps/isr-watch.sh replicated 4 5
```
✅ **Expected:** `under-replicated 6` on every line, `offline 0`. The data is still being written and
read — only the copies are missing.

### Step 7 · Which replicas are missing?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic replicated --under-replicated-partitions
```
✅ **Expected:** 6 lines, each with `Replicas: 0,1,2` but an `Isr:` that holds only the leader.

### Step 8 · Is this dangerous?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-min-isr-partitions
```
✅ **Expected:** the same 6 partitions: their ISR (1) is below `min.insync.replicas` (2), so `acks=all`
writes would now be rejected (scenario 11), and losing this one broker would lose data (scenario 17).

### Step 9 · Why did the followers fall out?
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type topics --entity-name replicated
kafka-configs.sh --bootstrap-server $BOOTSTRAP --describe --entity-type brokers --entity-name 0
```
✅ **Expected:** the topic lists `leader.replication.throttled.replicas=*` and
`follower.replication.throttled.replicas=*`; the broker lists
`leader.replication.throttled.rate=1` and `follower.replication.throttled.rate=1`.

A follower is removed from the ISR when it hasn't caught up with the leader for
`replica.lag.time.max.ms` (**30 s** by default). At 1 byte/s it never will.

---

## Part 4 · Fix (a): remove the throttle

### Step 10 · Remove it from the brokers and the topic
```bash
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate'; done
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name replicated --alter --delete-config 'leader.replication.throttled.replicas,follower.replication.throttled.replicas'
```
✅ **Expected:** `Completed updating config for broker $b.` three times, then
`Completed updating config for topic replicated.`

### Step 11 · Watch the ISR fill up again
```bash
bash /apps/isr-watch.sh replicated 6 5
```
✅ **Expected:** `under-replicated` falls back to `0` within ~10–30 s: the followers catch up and the
leader adds them back to the ISR.

---

## Part 5 · Break (b) and back to normal: a broker restarts

The second everyday cause is simply a broker being away — a restart, a node drain, a crash.

### Step 12 · PowerShell: restart broker 1
```powershell
kubectl -n kafka-lab delete pod kafka-controller-1
```
✅ **Expected:** `pod "kafka-controller-1" deleted`.

### Step 13 · Watch what happens (start this immediately)
```bash
bash /apps/isr-watch.sh replicated 12 10
```
✅ **Expected:** over ~2 minutes:
- `under-replicated` jumps to the partitions that have a replica on broker 1 (usually all 6);
- `offline` stays `0` — the other replicas took over as leaders, so clients keep working;
- when the pod is back and has caught up, `under-replicated` returns to `0`.

### Step 14 · Check the leaders
```bash
bash /apps/leaders.sh replicated
```
✅ **Expected:** broker 1 leads fewer partitions than before (leadership moved away while it was down).
`auto.leader.rebalance.enable=true` moves it back within ~5 minutes; scenario 04 is all about this.

### Step 15 · Confirm the cluster is healthy
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --under-replicated-partitions; echo "(nothing above = healthy)"
```
✅ **Expected:** only the `(nothing above = healthy)` line, as in step 3 — for **all** topics, not just
this one.

---

## Part 6 · Clean up

### Step 16 · Stop the traffic and delete the topic
```bash
pkill -f "topic replicated"; sleep 3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic replicated
```
✅ **Expected:** `Terminated`, then nothing from the delete.

### Step 17 · Make sure no throttle is left anywhere
```bash
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate' 2>/dev/null; done; echo done
```
✅ **Expected:** `done` (errors about configs that aren't set are fine).

---

## Why it happened, and how to prevent it

- **Under-replicated** means: the partition has fewer replicas in its ISR than its replication factor.
  Nothing is lost yet, but the safety margin is. `UnderReplicatedPartitions` should be **0** at all
  times; anything else is either a broker that is away or a broker that is too slow.
- **How Kafka decides:** the leader drops a follower from the ISR when the follower hasn't fetched up
  to the leader's end offset within `replica.lag.time.max.ms` (30 s). It adds it back as soon as the
  follower catches up — that's why the numbers in step 13 move on their own.
- **Common causes:**
  - a restart, a node drain, an `OOMKilled` broker (scenario 20);
  - leftover replication throttles after a reassignment (this scenario) — check with
    `kafka-configs.sh --describe --entity-type brokers`;
  - a slow or full disk (scenario 18), a network problem (scenario 19);
  - too much load on one broker, often from leader imbalance (scenario 04).
- **Prevent and detect:**
  - Alert on `UnderReplicatedPartitions` > 0 for more than a couple of minutes, and on
    `UnderMinIsrPartitionCount` > 0 immediately.
  - Restart brokers **one at a time**, and wait for under-replicated to reach 0 in between — that's
    exactly what scenario 21 turns into a script.
  - Use a throttle when you move partitions, but always remove it afterwards (`--verify` on
    `kafka-reassign-partitions.sh` does that for you).
