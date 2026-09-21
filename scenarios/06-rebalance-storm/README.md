# 06 · Rebalance storm

**What you'll learn:** why consumers that keep crashing and restarting stop the *whole* group again and
again, and how static membership and cooperative rebalancing make a group shrug off restarts.

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
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 06`.

---

## Part 1 · Normal: 4 consumers sharing 6 partitions

An app receives ~500 events/s on the topic `events` (6 partitions). It runs as 4 instances in the group
`storm-group`. Each instance uses `session.timeout.ms=10000`: if Kafka hears nothing from an instance for
10 s, it declares it dead.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic events --partitions 6 --replication-factor 3 --config retention.bytes=67108864
```
✅ **Expected:** `Created topic events.`

### Step 2 · Start the traffic (500 per second)
```bash
nohup kafka-producer-perf-test.sh --topic events --num-records 1000000000 --record-size 100 --throughput 500 --producer-props bootstrap.servers=$BOOTSTRAP >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start the app (4 consumers)
```bash
nohup bash /apps/storm-group.sh >/dev/null 2>&1 &
```
✅ **Expected:** a job line. **Wait ~20 seconds** while the 4 consumers join the group.

### Step 4 · Check the group's state
```bash
bash /apps/group-state.sh storm-group 5
```
✅ **Expected:** 5 lines, 2 s apart, all with `state Stable` and `members 4`.

### Step 5 · Check that the lag stays small
```bash
bash /apps/lag-watch.sh storm-group 3 5
```
✅ **Expected:** a small total lag (a few hundred at most) on every line.

---

## Part 2 · Break: the instances keep crashing

A memory leak in the new release makes Kubernetes kill an instance every 5–10 seconds (`OOMKilled`).
Each killed instance is restarted a second later, like a pod.

### Step 6 · Deploy the leaky release
```bash
pkill -f storm-group.sh; sleep 3; nohup bash /apps/storm-group.sh --chaos >/dev/null 2>&1 &
```
✅ **Expected:** `Terminated` for the old app, then a new job line. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · Are instances crashing?
```bash
tail -4 /tmp/s06-crashes.log
```
✅ **Expected:** lines like `10:15:02 storm-3 crashed (kill -9), restarting it`, 5–10 s apart.

### Step 8 · What is the group doing?
```bash
bash /apps/group-state.sh storm-group 10
```
✅ **Expected:** the state keeps changing. Many lines show `PreparingRebalance` or `CompletingRebalance`
instead of `Stable`, and `members` jumps between 3, 4 and 5.

### Step 9 · What happens to the lag?
```bash
bash /apps/lag-watch.sh storm-group 6 5
```
✅ **Expected:** the lag jumps around: thousands while the group rebalances, lower when it's `Stable`
for a moment. In step 5 it stayed at a few hundred.

### Step 10 · Who is in the group?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group storm-group --members
```
✅ **Expected:** either `Warning: Consumer group 'storm-group' is rebalancing.`, or up to 4 members
with `CONSUMER-ID`s like `storm-2-1a2b…`. Run it again a few seconds later: the IDs change, and
`GROUP-INSTANCE-ID` is `-` for every member.

Why one crash hurts the whole group:
- A restarted instance has **no memory** of who it was. It joins as a brand-new member, which starts a
  rebalance.
- The dead instance still counts as a member until its session times out (10 s), so the rebalance waits
  for it.
- With the default (eager) rebalancing, **every** member gives up **all** its partitions during a
  rebalance. So each crash pauses all 4 instances for several seconds, and the next crash comes before
  the group has caught up.

---

## Part 4 · Fix: static membership and cooperative rebalancing

The memory leak will take a while to fix. Meanwhile, two consumer settings let the group ride out the
crashes:
- `group.instance.id=storm-N` (**static membership**): a restarted instance comes back *as the same
  member* and gets its old partitions back, with no rebalance, as long as it returns within the session
  timeout.
- `partition.assignment.strategy=CooperativeStickyAssignor` (**cooperative rebalancing**): when a
  rebalance does happen, only the partitions that must move are paused; the others keep flowing.

### Step 11 · Deploy the same app with both settings
```bash
pkill -f storm-group.sh; sleep 12; nohup bash /apps/storm-group.sh --chaos --static --cooperative >/dev/null 2>&1 &
```
✅ **Expected:** `Terminated`, then a job line 12 s later. The pause lets the old members time out.
**Wait ~30 seconds.**

---

## Part 5 · Back to normal

The crashes continue (the leak is not fixed yet), but the group should barely notice.

### Step 12 · The crashes go on
```bash
tail -3 /tmp/s06-crashes.log
```
✅ **Expected:** new crash lines with the current time, 5–10 s apart, as in step 7.

### Step 13 · Check the group's state
```bash
bash /apps/group-state.sh storm-group 10
```
✅ **Expected:** `state Stable` and `members 4` on (almost) every line, as in step 4.

### Step 14 · Check the lag
```bash
bash /apps/lag-watch.sh storm-group 3 5
```
✅ **Expected:** small again, as in step 5. It may blip for a moment while a crashed instance restarts
(its partitions wait for it), but it never climbs into the thousands.

### Step 15 · Who is in the group now?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group storm-group --members
```
✅ **Expected:** 4 members with `GROUP-INSTANCE-ID` `storm-1` … `storm-4`, each with 1 or 2
partitions. Run it again after a crash: the same instance IDs are still there.

---

## Part 6 · Clean up

### Step 16 · Stop the app and the traffic
```bash
pkill -f "storm-group.sh|topic events"; sleep 15
```
✅ **Expected:** several `Terminated` lines. The 15 s wait matters: static members don't leave the
group when they stop. Kafka removes them when their session times out (10 s).

### Step 17 · Delete the consumer group and the topic
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group storm-group
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic events
rm -f /tmp/s06-*
```
✅ **Expected:** `Deletion of requested consumer groups ('storm-group') was successful.` If it says the
group is not empty, wait 10 s and run the first line again.

---

## Why it happened, and how to prevent it

- **Why:** each crash-and-restart caused **two** membership changes: a new member joined, and the dead
  one timed out. With eager rebalancing, every change pauses **all** consumers. Frequent restarts keep
  the group rebalancing instead of working.
- **Things that cause rebalance storms in real life:**
  - pods crash-looping or getting `OOMKilled`;
  - rolling deployments, since every restarted pod is a new member;
  - slow processing that exceeds `max.poll.interval.ms` (scenario 07);
  - a session timeout shorter than GC pauses or network hiccups.
- **Fix, most impactful first:**
  - Fix the crash itself. The settings below only soften the damage.
  - **Static membership** (`group.instance.id`, unique and stable per instance, e.g. the StatefulSet
    pod name). Set `session.timeout.ms` a bit longer than a normal restart takes.
  - **Cooperative rebalancing** (`CooperativeStickyAssignor`, or Kafka Streams' default), so a
    rebalance only pauses the partitions that move.
  - **The new consumer protocol** (Kafka 4.0, KIP-848: `group.protocol=consumer`). The broker computes
    assignments and changes them one member at a time, without stopping the whole group.
- **Spot it early:** watch the group state (`--describe --state`) and the rebalance rate in consumer
  metrics. Alert on a group that is often not `Stable`.
- **Try it yourself (optional):** after Part 5, run the storm with the new protocol in its own group.
  `bash /apps/storm-group.sh --chaos --kip848` uses the group `storm-group-848`. Compare
  `bash /apps/group-state.sh storm-group-848 10` with step 8. Its states are named differently
  (`Stable`, `Reconciling`, `Assigning`). Stop it with `pkill -f storm-group.sh`. `cleanup.sh 06`
  removes that group too.
