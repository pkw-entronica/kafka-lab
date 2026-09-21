# 16 · Losing a broker: offline partitions and NotEnoughReplicas

**What you'll learn:** what happens to an RF 1 topic when its broker goes away, why
`min.insync.replicas=3` on an RF 3 topic means you can't lose a single broker, and why the KRaft
controller quorum needs a majority.

**Time:** about 25 minutes (it stops and starts a broker).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- After each command, compare what you see with **✅ Expected**.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 16`.

---

## Part 1 · Normal: two topics, two different bets

- `orders-rf3`: 3 partitions, **RF 3**, and the team set `min.insync.replicas=3` for "extra safety".
- `cache-rf1`: 3 partitions, **RF 1** — "it's only a cache, we can rebuild it".

### Step 1 · Create both topics
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic orders-rf3 --partitions 3 --replication-factor 3 --config min.insync.replicas=3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic cache-rf1 --partitions 3 --replication-factor 1
```
✅ **Expected:** `Created topic orders-rf3.` and `Created topic cache-rf1.`

### Step 2 · See how the replicas are spread
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic cache-rf1
```
✅ **Expected:** three partition lines, each with a **single** replica: `Replicas: 0`, `Replicas: 1`,
`Replicas: 2` (in some order). Each partition lives on exactly one broker.

### Step 3 · Both topics accept writes
```bash
bash /apps/produce-check.sh orders-rf3 5 all
bash /apps/produce-check.sh cache-rf1 5 all
```
✅ **Expected:** `5 accepted, 0 rejected (acks=all)` for both.

### Step 4 · Look at the controller quorum
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
```
✅ **Expected:** a `LeaderId`, a `HighWatermark`, and **3 current voters**. In KRaft, these three nodes
also form the *controller quorum* that stores all cluster metadata. Decisions need a **majority**: 2 of 3.

---

## Part 2 · Break: one broker goes away

A node has to be taken out for maintenance. In this lab, scaling the StatefulSet down does the same
thing: broker 2 stops, its disk stays.

### Step 5 · PowerShell: stop broker 2
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`, and `kubectl -n kafka-lab get pods` soon
shows only `kafka-controller-0` and `kafka-controller-1`. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 6 · Partitions with no leader at all
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions
```
✅ **Expected:** the `cache-rf1` partition that lived on broker 2, with `Leader: none`. Its only replica
is gone, so the partition can't be written or read — and nothing can bring it back except broker 2.

### Step 7 · What clients see for that topic
```bash
bash /apps/produce-check.sh cache-rf1 6 all
```
✅ **Expected:** a mix: some `ok` (the partitions on brokers 0 and 1) and some
`ERROR TimeoutException` or `ERROR LeaderNotAvailableException` (the offline partition). A third of the
cache traffic is failing.

### Step 8 · And the RF 3 topic?
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic orders-rf3
```
✅ **Expected:** every partition still has a leader, but `Isr:` now lists only 2 brokers. Nothing is
offline: the two remaining replicas of each partition are enough to serve reads and writes.

### Step 9 · …but its writes are rejected
```bash
bash /apps/produce-check.sh orders-rf3 5 all
```
✅ **Expected:** 5 × `ERROR NotEnoughReplicasException`, then `0 accepted, 5 rejected`. The ISR is 2 and
`min.insync.replicas=3`, so Kafka refuses every `acks=all` write. The topic is fully replicated by any
sane standard, and still it's down — because the setting left no room for a single failure.

### Step 10 · Check the quorum again
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```
✅ **Expected:** a row per voter: nodes 0 and 1 are caught up, node 2 has an old `LastFetchTimestamp`
and a growing lag. Two of three voters are alive, which **is** a majority, so the cluster can still
elect leaders, create topics and change configs.

> With **two** brokers down there is no majority. The controller can't make any decision: no leader
> elections, no topic changes, no ISR updates. The surviving broker serves what it already leads until
> someone brings a second node back. That is why a 3-node KRaft cluster tolerates exactly one node loss.

---

## Part 4 · Fix: bring the broker back, then fix both mistakes

### Step 11 · PowerShell: start broker 2 again
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=3
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=300s
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`, then after ~60 s
`pod/kafka-controller-2 condition met`.

### Step 12 · Everything is reachable again
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions; echo "(nothing above = healthy)"
bash /apps/isr-watch.sh orders-rf3 4 5
```
✅ **Expected:** only the `(nothing above = healthy)` line, and `under-replicated` back to `0` within a
few samples.

### Step 13 · Give the RF 3 topic room to lose a broker
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name orders-rf3 --alter --add-config min.insync.replicas=2
```
✅ **Expected:** `Completed updating config for topic orders-rf3.` With RF 3 and `min.insync.replicas=2`
you can lose one broker and keep writing — and still have two copies of every acknowledged message.

### Step 14 · Give the cache real replicas
```bash
echo '{"version":1,"partitions":[{"topic":"cache-rf1","partition":0,"replicas":[0,1,2]},{"topic":"cache-rf1","partition":1,"replicas":[1,2,0]},{"topic":"cache-rf1","partition":2,"replicas":[2,0,1]}]}' > /tmp/s16-rf3.json
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file /tmp/s16-rf3.json --execute
```
✅ **Expected:** `Successfully started partition reassignments for cache-rf1-0,cache-rf1-1,cache-rf1-2.`
This is how you raise the replication factor of an existing topic.

### Step 15 · Wait for the copies to be made
```bash
kafka-reassign-partitions.sh --bootstrap-server $BOOTSTRAP --reassignment-json-file /tmp/s16-rf3.json --verify
```
✅ **Expected:** `Reassignment of partition cache-rf1-N is completed.` for all three (run it again if it
still says "in progress"), and a line saying the throttle was removed.

---

## Part 5 · Back to normal

### Step 16 · Both topics are now RF 3
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic cache-rf1
```
✅ **Expected:** each partition has `Replicas: 0,1,2` and `Isr: 0,1,2`.

### Step 17 · Both topics accept writes again
```bash
bash /apps/produce-check.sh orders-rf3 5 all
bash /apps/produce-check.sh cache-rf1 5 all
```
✅ **Expected:** `5 accepted, 0 rejected` for both, as in step 3.

---

## Part 6 · Clean up

### Step 18 · Delete the topics
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic orders-rf3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic cache-rf1
rm -f /tmp/s16-*
```
✅ **Expected:** no output.

### Step 19 · PowerShell: make sure all three brokers are running
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** `kafka-controller-0`, `-1` and `-2`, all `1/1 Running`.

---

## Why it happened, and how to prevent it

- **RF 1 means "this data is on one machine".** Any restart — maintenance, an upgrade, a crash — takes
  that partition offline, and a disk failure loses it for good. RF 1 is only for data you can rebuild
  from somewhere else *and* whose downtime nobody minds. In the lab it's also what makes
  `--unavailable-partitions` show something.
- **`min.insync.replicas` is about the margin, not about safety alone.** With RF 3:
  - `min.insync.replicas=2`: survives one broker loss, still keeps 2 copies of every ack'd write. This
    is the standard choice.
  - `min.insync.replicas=3`: no margin at all — a single restart stops writes (step 9).
  - `min.insync.replicas=1`: no real guarantee; the same risk as `acks=1` (scenario 11).
- **KRaft quorum:** the controllers replicate the metadata log and need a **majority** to make any
  decision. 3 controllers tolerate 1 loss, 5 tolerate 2. Losing the majority doesn't erase data, but
  the cluster freezes: no leader elections, no new topics, no config changes.
- **Prevent it:**
  - RF 3 and `min.insync.replicas=2` as the default for every real topic; check new topics against it.
  - Alert on `OfflinePartitionsCount` > 0 (something is unreadable **now**) and on
    `UnderMinIsrPartitionCount` > 0 (writes are being rejected).
  - Spread brokers over failure domains (nodes, racks, zones) so one failure takes exactly one replica.
  - Restart one broker at a time and wait for the ISR to recover in between (scenarios 15 and 21).
