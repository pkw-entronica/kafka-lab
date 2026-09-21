# 05 · Consumer lag after a slow release

**What you'll learn:** how to tell whether lag comes from *too much input* or *too slow output*, and why
fixing it can need both faster processing **and** more consumers.

**Time:** about 15 minutes.

## How to follow this guide

- Every command runs in the **lab shell** unless it says **PowerShell**. Open the lab shell once from
  PowerShell and keep it open:
  ```powershell
  kubectl -n kafka-lab exec -it kafka-client -- bash
  ```
- After each command, compare what you see with **✅ Expected**. Numbers vary a little from run to run.
- Stuck, or want to start over? In PowerShell, run `wsl -d Ubuntu -- bash cleanup.sh 05`.

---

## Part 1 · Normal: a click pipeline that keeps up

A website sends ~2,000 click events/s to the topic `clicks`, which has 6 partitions. Two consumers
(release 1.0) process them with no noticeable delay per event.

### Step 1 · Create the topic
```bash
kafka-topics.sh --bootstrap-server $BOOTSTRAP --create --topic clicks --partitions 6 --replication-factor 3 --config retention.bytes=67108864
```
✅ **Expected:** `Created topic clicks.` The size limit keeps the 1 GiB lab disks from filling up.

### Step 2 · Start the click traffic (2,000 per second)
```bash
nohup kafka-producer-perf-test.sh --topic clicks --num-records 1000000000 --record-size 100 --throughput 2000 --producer-props bootstrap.servers=$BOOTSTRAP acks=all linger.ms=20 >/dev/null 2>&1 &
```
✅ **Expected:** a job line like `[1] 2345`.

### Step 3 · Start release 1.0 of the consumer (2 instances)
```bash
for i in 1 2; do nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic clicks --group clicks-group --consumer-property client.id=v1-$i >/dev/null 2>&1 & done
```
✅ **Expected:** two job lines.

### Step 4 · Check that the lag stays small
```bash
bash /apps/lag-watch.sh clicks-group 3
```
✅ **Expected:** 3 lines about 10 s apart. The total lag is small (a few thousand at most, about one
second of traffic) and `flat` or changing little.

### Step 5 · Check in vs out
```bash
bash /apps/in-out.sh clicks clicks-group
```
✅ **Expected:** `in: ~2000 msg/s` and `out: ~2000 msg/s`.

---

## Part 2 · Break: release 2.0 is slower

Release 2.0 adds ~10 ms of work per event, for example a new database call.

### Step 6 · Deploy release 2.0
```bash
pkill -f "client.id=v1"; for i in 1 2; do nohup bash /apps/slow-consumer.sh clicks clicks-group v2-$i 0.01 >/dev/null 2>&1 & done
```
✅ **Expected:** `Terminated` for the two old consumers and two new job lines. **Wait ~30 seconds.**

---

## Part 3 · Observe: what does the problem look like?

### Step 7 · Is the lag growing?
```bash
bash /apps/lag-watch.sh clicks-group 6
```
✅ **Expected:** `GROWING by ~1800 msg/s` on every line.

### Step 8 · Too much in, or too little out?
```bash
bash /apps/in-out.sh clicks clicks-group
```
✅ **Expected:** `in: ~2000 msg/s` (the same as before) but `out: ~190 msg/s`. The input didn't change;
the consumers got slower.

### Step 9 · How many consumers, and on how many partitions?
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group clicks-group --members
```
✅ **Expected:** 2 members, each with `#PARTITIONS 3`. The topic has 6 partitions, so up to 6 consumers could work.

### Step 10 · What does release 2.0 do per event?
```bash
grep -n "sleep" /apps/slow-consumer.sh
```
✅ **Expected:** `| while read -r _; do sleep "$D"; done`, and it was started with `0.01`, so 10 ms per
event. That's at most ~100 events/s per consumer:
- 2 consumers do ~200/s;
- even 6 would do only ~600/s, far below 2,000.

The fix needs **faster processing and more consumers**.

---

## Part 4 · Fix: release 2.1, scaled to 6 consumers

Release 2.1 removes the slow per-event work (for example by batching the database writes). It runs as
6 instances, one per partition.

### Step 11 · Deploy release 2.1
```bash
pkill -f "slow-consumer.sh clicks"; for i in 1 2 3 4 5 6; do nohup kafka-console-consumer.sh --bootstrap-server $BOOTSTRAP --topic clicks --group clicks-group --consumer-property client.id=v3-$i >/dev/null 2>&1 & done
```
✅ **Expected:** `Terminated` for the slow consumers and six new job lines.

---

## Part 5 · Back to normal

### Step 12 · Watch the backlog disappear
```bash
bash /apps/lag-watch.sh clicks-group 4
```
✅ **Expected:** within seconds the total lag drops to a few thousand and stays there, as in step 4.

### Step 13 · Check in vs out, and who works
```bash
bash /apps/in-out.sh clicks clicks-group
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --describe --group clicks-group --members
```
✅ **Expected:** out ≈ in (~2,000 msg/s), and 6 members with `#PARTITIONS 1` each.

---

## Part 6 · Clean up

### Step 14 · Stop the traffic and the consumers
```bash
pkill -f "topic clicks"; sleep 5
```
✅ **Expected:** several `Terminated` lines.

### Step 15 · Delete the consumer group and the topic
```bash
kafka-consumer-groups.sh --bootstrap-server $BOOTSTRAP --delete --group clicks-group
kafka-topics.sh --bootstrap-server $BOOTSTRAP --delete --topic clicks
```
✅ **Expected:** `Deletion of requested consumer groups ('clicks-group') was successful.`

---

## Why it happened, and how to prevent it

- **Why:** lag grows whenever *in > out*. Out is limited by two things:
  - processing time per event (10 ms means ~100/s per consumer);
  - the number of consumers, which is at most the number of partitions.
- **Fix, most impactful first:**
  - Roll back or speed up the slow release: batch work, use async I/O, avoid per-event remote calls.
  - Scale consumers up to the partition count.
  - Add partitions (mind scenario 03).
- **Spot it early:** alert on **lag growth** and **lag in time**, not just on the lag number. Compare
  processing time per event before and after each release.
- **Lab note:** the original plan used 5,000 msg/s. That works too (`--throughput 5000`) if Windows has
  plenty of free RAM.
