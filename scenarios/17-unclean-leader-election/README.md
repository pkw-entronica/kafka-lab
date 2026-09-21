# 17 · Unclean leader election: offline, or data gone

**What you'll learn:** what happens when the only in-sync replica dies — the partition stays offline
(availability lost) with `unclean.leader.election.enable=false`, or comes back **with messages missing**
when it's `true`. You'll prove the loss by watching the end offset go backwards.

**Time:** about 30 minutes (it stops and starts a broker twice).

> **Not yet verified on the lab.** The expected results describe what Kafka should do; exact numbers and
> timings will differ.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- Run the **PowerShell** commands in a second window, in the project folder.
- Do scenario 11 first if you can: it explains the ISR and `acks`, which this scenario builds on.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 17`.

---

## Part 1 · Normal: one partition, three copies

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic settlements --partitions 1 --replication-factor 3 --config min.insync.replicas=2
```
✅ **Expected:** `Created topic settlements.`

### Step 2 · Write 1,000 settlements with acks=all
```bash
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic settlements --max-messages 1000 --throughput 500 --acks -1 > /tmp/s17-safe.json
grep -c producer_send_success /tmp/s17-safe.json
```
✅ **Expected:** `1000`.

### Step 3 · Note where the log ends, and who is in sync
```bash
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
```
✅ **Expected:** `settlements:0:1000`, and a partition line with `Isr: 0,1,2`. All three brokers hold
the same 1,000 messages.

---

## Part 2 · Break: the ISR shrinks to one, and that broker dies

### Step 4 · Stall replication (the throttle from scenario 11)
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name settlements --alter --add-config 'leader.replication.throttled.replicas=*,follower.replication.throttled.replicas=*'
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --add-config 'leader.replication.throttled.rate=1,follower.replication.throttled.rate=1'; done
```
✅ **Expected:** `Completed updating config for topic settlements.` and three broker lines.
**Wait ~60 seconds.**

### Step 5 · Find the last replica standing
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
```
✅ **Expected:** `Isr:` with a **single** broker, which is also the `Leader:`. **Write that number
down** — this guide calls it **L**, and its pod is `kafka-controller-L`.

### Step 6 · Keep writing with acks=1 (500 messages that only one broker has)
```bash
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic settlements --max-messages 500 --throughput 250 --acks 1 > /tmp/s17-risky.json
grep -c producer_send_success /tmp/s17-risky.json
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** `500` accepted, and `settlements:0:1500`. The application believes 1,500 settlements
are stored.

### Step 7 · PowerShell: that broker dies (replace L with your number)
```powershell
kubectl -n kafka-lab delete pod kafka-controller-L
```
✅ **Expected:** `pod "kafka-controller-L" deleted`.

---

## Part 3 · Observe: two different disasters

### With `unclean.leader.election.enable=false` (the lab default)

### Step 8 · The partition has no leader
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --unavailable-partitions
```
✅ **Expected:** `Leader: none`, `Isr:` still listing only broker L, and the partition shown as
unavailable. The other two replicas are **alive but out of sync**, and Kafka refuses to promote them.

### Step 9 · What clients see
```bash
bash /apps/produce-check.sh settlements 5 all
```
✅ **Expected:** 5 × `ERROR` (`TimeoutException` or `LeaderNotAvailableException`). Reads fail the same
way. This is **availability lost — and nothing lost**: every acknowledged message is still on broker L's
disk, waiting for it to come back.

### Step 10 · It comes back by itself
The StatefulSet restarts the pod. After ~60 s:
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** `Leader: L` again and `settlements:0:1500` — the outage ended with all 1,500 messages
intact. That is what `unclean.leader.election.enable=false` buys you.

### Now the tempting switch: `unclean.leader.election.enable=true`

### Step 11 · Turn it on for this topic
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name settlements --alter --add-config unclean.leader.election.enable=true
```
✅ **Expected:** `Completed updating config for topic settlements.` "Now the partition will never be
offline again" — which is true, at a price.

### Step 12 · Same situation: ISR of one, 500 more messages with acks=1
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
kafka-verifiable-producer.sh --bootstrap-server $BOOTSTRAP --topic settlements --max-messages 500 --throughput 250 --acks 1 > /tmp/s17-risky2.json
grep -c producer_send_success /tmp/s17-risky2.json; kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** the ISR is still just broker L (the throttle is still on), `500` accepted, and
`settlements:0:2000`.

### Step 13 · PowerShell: kill broker L again
```powershell
kubectl -n kafka-lab delete pod kafka-controller-L
```
✅ **Expected:** `pod "kafka-controller-L" deleted`. **Wait ~20 seconds.**

### Step 14 · The partition is online again — look at the end offset
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** a **different** broker is now `Leader:`, and the end offset is back around
`settlements:0:1000` — it went **backwards** by ~1,000. Kafka promoted a replica that never received
those messages.

### Step 15 · Count what's really there
```bash
kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic settlements --from-beginning --timeout-ms 20000 | wc -l
```
✅ **Expected:** about `1000` lines, after a `TimeoutException` line that just means "no more messages".
The producer got an "ok" for 2,000 settlements; roughly 1,000 of them no longer exist.

### Step 16 · And the old leader can't bring them back
Wait ~60 s for the pod to restart, then:
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** broker L is back in the ISR, and the end offset is still ~1,000. On rejoining, a
replica **truncates** its log to match the new leader. The extra messages were deleted, permanently.

---

## Part 4 · Fix: put the guardrails back

### Step 17 · Turn unclean election off again
```bash
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name settlements --alter --delete-config unclean.leader.election.enable
```
✅ **Expected:** `Completed updating config for topic settlements.`

### Step 18 · Remove the throttle so replication can catch up
```bash
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate'; done
kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type topics --entity-name settlements --alter --delete-config 'leader.replication.throttled.replicas,follower.replication.throttled.replicas'
```
✅ **Expected:** three broker lines, then `Completed updating config for topic settlements.`

---

## Part 5 · Back to normal

### Step 19 · All three replicas in sync again
```bash
bash /apps/isr-watch.sh settlements 4 5
kafka-topics.sh --bootstrap-server $BOOTSTRAP --describe --topic settlements
```
✅ **Expected:** `under-replicated 0` within a few samples, and `Isr: 0,1,2`.

### Step 20 · Write the safe way, and watch the offset move forwards only
```bash
bash /apps/produce-check.sh settlements 5 all
kafka-get-offsets.sh --bootstrap-server $BOOTSTRAP --topic settlements --time -1
```
✅ **Expected:** `5 accepted, 0 rejected (acks=-1)` and an end offset 5 higher than in step 16. With
`acks=all`, `min.insync.replicas=2` and unclean election off, an acknowledged message is on at least two
disks — and can't be thrown away by an election.

---

## Part 6 · Clean up

### Step 21 · Delete the topic and any leftovers
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic settlements
for b in 0 1 2; do kafka-configs.sh --bootstrap-server $BOOTSTRAP --entity-type brokers --entity-name $b --alter --delete-config 'leader.replication.throttled.rate,follower.replication.throttled.rate' 2>/dev/null; done
rm -f /tmp/s17-*; echo done
```
✅ **Expected:** `done` (errors about configs that aren't set are fine).

---

## Why it happened, and how to prevent it

- **The choice Kafka makes for you:** when a partition loses every in-sync replica, there are only two
  options.
  | `unclean.leader.election.enable` | What happens | You lose |
  |---|---|---|
  | `false` (default) | the partition waits for an in-sync replica to return | availability |
  | `true` | an out-of-sync replica becomes leader | acknowledged data, silently |
- **"Offsets went backwards" is the fingerprint.** After an unclean election the new leader's end
  offset is lower than what producers had been told, and the old leader **truncates** its log when it
  rejoins. Consumers that were ahead get `OffsetOutOfRange` and reset (scenario 10), and offsets that
  were already used get handed out again to new messages — two different consumers can read completely
  different data at the same offset.
- **What actually prevents the loss** is never getting to an ISR of one:
  - RF 3 with `min.insync.replicas=2` **and** `acks=all`: writes stop before the margin is gone
    (scenario 11), so there's nothing to lose.
  - Alert on `UnderMinIsrPartitionCount` and `OfflinePartitionsCount`, and fix under-replication fast
    (scenario 15).
  - Keep `unclean.leader.election.enable=false` (the Kafka default since 3.0). Turn it on per topic,
    consciously, only where stale-but-available beats correct-but-down (metrics, caches).
  - If you ever need it in an emergency, `kafka-leader-election.sh --election-type UNCLEAN` does it
    once, for named partitions, instead of leaving it on forever.
- **Kafka 4.0 note:** `--describe` now prints `Elr:` and `LastKnownElr:` columns. Eligible Leader
  Replicas remember which replicas were in the ISR when a partition went offline, so a future release
  can pick the least-bad replica instead of any replica. It's a preview feature and off by default.
