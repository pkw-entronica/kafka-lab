# 32 · Losing the KRaft controller quorum

**What you'll learn:** what the controller quorum is, why 3 nodes tolerate exactly one failure, and what
a cluster without a majority actually does — metadata operations fail, while partitions that already
have leaders can keep serving for a while.

**Time:** about 25 minutes.

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> messages will differ.

> ⚠️ This scenario deliberately leaves the cluster without a controller majority. Nothing is deleted and
> every disk stays intact — scaling back to 3 brings it all back. Don't run other scenarios at the same
> time.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 32`.

---

## Part 1 · Normal: three voters, one leader

In KRaft there is no ZooKeeper: the cluster metadata (topics, partitions, ISRs, ACLs, SCRAM users) lives
in its own replicated log, `__cluster_metadata`. The nodes that replicate it are the **voters**. In this
lab all three brokers are also controllers.

### Step 1 · Who is in the quorum?
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
```
✅ **Expected:** a `LeaderId`, a `LeaderEpoch`, a `HighWatermark`, and `CurrentVoters` listing **3**
nodes. The leader is the **active controller**; the others replicate from it.

### Step 2 · How far behind is each voter?
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```
✅ **Expected:** one row per voter with `LogEndOffset`, `Lag`, `LastFetchTimestamp` and a `Status` of
`Leader` or `Follower`. All lags should be `0` — this is the health check for the metadata layer.

### Step 3 · A working cluster
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic quorum-test --partitions 3 --replication-factor 3
bash /apps/produce-check.sh quorum-test 5 all
```
✅ **Expected:** `Created topic quorum-test.` and `5 accepted, 0 rejected (acks=all)`.

---

## Part 2 · Break: lose one controller, then a second

### Step 4 · PowerShell: stop broker 2
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`. **Wait ~30 seconds.**

### Step 5 · The quorum still has a majority
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```
✅ **Expected:** still three rows, but node 2's `LastFetchTimestamp` stops moving and its `Lag` grows.
Two of three voters are alive — that **is** a majority, so the metadata log can still commit records.

### Step 6 · Everything still works
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic quorum-ok-1 --partitions 1 --replication-factor 2
bash /apps/produce-check.sh quorum-test 5 all
```
✅ **Expected:** `Created topic quorum-ok-1.` and `5 accepted, 0 rejected (acks=all)`. A 3-node quorum is
designed to survive exactly this.

### Step 7 · PowerShell: now stop broker 1 as well
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=1
```
✅ **Expected:** `statefulset.apps/kafka-controller scaled`, leaving only `kafka-controller-0`.
**Wait ~30 seconds.**

---

## Part 3 · Observe: what does a cluster without a majority do?

### Step 8 · Ask the quorum about itself
```bash
timeout 45 kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status; echo "exit: $?"
```
✅ **Expected:** either a timeout/error, or a status with **no leader** (`LeaderId: -1`) and a
`HighWatermark` that no longer moves. One voter out of three cannot elect anything: it keeps voting for
itself and never gets a second vote.

### Step 9 · Metadata changes fail
```bash
timeout 60 kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic quorum-lost --partitions 1 --replication-factor 1; echo "exit: $?"
```
✅ **Expected:** it hangs and then fails — usually
`org.apache.kafka.common.errors.TimeoutException` — with a non-zero exit code. Creating or deleting
topics, changing configs, electing leaders, expanding an ISR: all of it needs a committed metadata
record, and nothing can be committed.

### Step 10 · …but the data path isn't dead yet
```bash
bash /apps/produce-check.sh quorum-test 8 1
```
✅ **Expected:** a mix. Writes to the partitions that the surviving broker already **leads** may still
be accepted (`ok`) for a while, because it serves them from the metadata it already had. Writes to
partitions whose leaders are gone fail — there is no controller to elect new ones. With `acks=all`
they'd fail as well, since the missing replicas can't be dropped from the ISR without the controller.

### Step 11 · What Kafka itself reports
```bash
timeout 45 kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic quorum-test; echo "exit: $?"
```
✅ **Expected:** either stale information (leaders on brokers that are stopped), or a timeout. Nothing
here is corrupted — the metadata log is complete on all three disks. It is simply **frozen**: no
decision can be made until a second node comes back.

---

## Part 4 · Fix: restore the majority

### Step 12 · PowerShell: start one controller back up
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=2
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-1 --timeout=300s
```
✅ **Expected:** `pod/kafka-controller-1 condition met` after ~60 s. Two of three voters is a majority
again, so an election can happen immediately.

### Step 13 · The quorum elects a leader again
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --status
```
✅ **Expected:** a real `LeaderId` and a `HighWatermark` that moves again when you run it twice.

### Step 14 · PowerShell: bring the third one back too
```powershell
kubectl -n kafka-lab scale statefulset kafka-controller --replicas=3
kubectl -n kafka-lab wait --for=condition=Ready pod/kafka-controller-2 --timeout=300s
```
✅ **Expected:** `pod/kafka-controller-2 condition met`.

---

## Part 5 · Back to normal

### Step 15 · Every voter caught up
```bash
kafka-metadata-quorum.sh --bootstrap-server $BOOTSTRAP describe --replication
```
✅ **Expected:** three rows with `Lag 0` and fresh `LastFetchTimestamp` values, as in step 2.

### Step 16 · Metadata operations work again
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic quorum-ok-2 --partitions 1 --replication-factor 3
kafka-topics.sh --bootstrap-server $BOOTSTRAP --list | grep quorum
```
✅ **Expected:** `Created topic quorum-ok-2.` and a list containing `quorum-test`, `quorum-ok-1` and
`quorum-ok-2` — but **not** `quorum-lost`: that creation never committed.

### Step 17 · And so does the data path
```bash
bash /apps/isr-watch.sh "" 4 5
bash /apps/produce-check.sh quorum-test 5 all
```
✅ **Expected:** `under-replicated 0   offline 0` after the replicas catch up, and
`5 accepted, 0 rejected (acks=all)`.

---

## Part 6 · Clean up

### Step 18 · Delete the topics
```bash
for t in quorum-test quorum-ok-1 quorum-ok-2; do kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic $t; done
```
✅ **Expected:** no output.

### Step 19 · PowerShell: all three brokers running
```powershell
kubectl -n kafka-lab get pods
```
✅ **Expected:** three pods `1/1 Running`.

---

## Why it happened, and how to prevent it

- **Raft needs a majority.** Every metadata record has to be written by more than half of the voters
  before it counts. With 3 voters you need 2, so you survive **one** failure; with 5 you need 3 and
  survive two. An even number buys you nothing: 4 voters also tolerate only one failure, so quorums are
  always odd.
- **Losing the quorum is not losing data.** The metadata log is intact on every node; the cluster just
  can't agree on anything new. Partitions that already have leaders can keep serving reads and writes
  for a while — which makes the outage look partial and confusing, and is exactly why you should alert
  on the controller layer separately from the broker layer.
- **What to monitor:**
  - `kafka-metadata-quorum.sh describe --replication` — every voter's `Lag` should be 0 and its
    `LastFetchTimestamp` recent;
  - `describe --status` — there should always be a `LeaderId`, and the `HighWatermark` should advance;
  - the metadata-layer metrics on the controllers (`ActiveControllerCount` = 1 across the cluster,
    metadata load/apply times).
- **In production:** use **dedicated controllers** (3 or 5 small nodes, `process.roles=controller`)
  instead of combined nodes like this lab, spread them across failure domains, and never take more than
  one down at a time — the same rule as scenario 21, for the same reason.
- **If a majority is lost permanently** (disks gone, not just pods stopped), recovery means rebuilding
  the quorum from a surviving node: Kafka 4.0 supports dynamic quorums (KIP-853), so
  `kafka-metadata-quorum.sh add-controller` / `remove-controller` can replace a dead voter while the
  cluster runs — as long as a majority still exists. Keeping a healthy odd-sized quorum is far cheaper
  than any of the alternatives.
